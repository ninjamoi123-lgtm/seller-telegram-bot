import os
import io
import asyncio
import pandas as pd

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import CommandStart

from openai import OpenAI


# =====================
# ENV
# =====================
BOT_TOKEN = os.environ["BOT_TOKEN"]
OPENROUTER_API_KEY = os.environ["OPENROUTER_API_KEY"]
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openai/gpt-4o-mini")

# OpenRouter via OpenAI-compatible SDK
client = OpenAI(
    api_key=OPENROUTER_API_KEY,
    base_url="https://openrouter.ai/api/v1",
    default_headers={
        # Можно оставить заглушки. Если есть сайт/страница — поставь.
        "HTTP-Referer": "https://example.com",
        "X-Title": "Ozon Profit Bot",
    },
)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# =====================
# UI: one button
# =====================
menu_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📊 Посчитать чистую прибыль")]],
    resize_keyboard=True
)

# Простое состояние: ждём файл только после нажатия кнопки
WAITING_FOR_FILE = set()


# =====================
# PROMPT (строго по твоим требованиям)
# =====================
SYSTEM_PROMPT = """Ты — финансовый аналитик маркетплейса OZON.

Тебе передан Excel-отчет «Начисления» из OZON Seller.
Структура может быть любой: служебные строки, разные заголовки, разное расположение колонок.

ТВОЯ ЗАДАЧА:
1) Самостоятельно определить:
   - где находится строка заголовков
   - колонку SKU (артикул/offer_id/код товара)
   - колонку «Сумма итого» (деньги продавцу)
   - колонку типа операции (продажа / возврат / прочее), если она есть

2) Считать строго по методике из видео:
   - Выручка = сумма всех значений «Сумма итого» (это деньги, которые поступят на расчетный счет)
   - УСН 6% считается СО ВСЕЙ ВЫРУЧКИ
   - Чистая прибыль = Выручка − УСН 6%
   - Себестоимость НЕ учитывать

3) По операциям (если нужно для определения продаж/возвратов):
   - продажа товара = sale
   - возврат/отмена = return
   - логистика/комиссия/услуги/штрафы/корректировки = other

4) ВЫВЕСТИ СТРОГО в формате (без воды, без пояснений, без лишнего текста):

1) Общая выручка (на расчетный счет): ХХХ ₽
2) Общая чистая прибыль (после УСН 6%): ХХХ ₽
3) По каждому SKU:
- SKU XXXXX: выручка ХХХ ₽, чистая прибыль ХХХ ₽
"""


def excel_to_compact_text(df: pd.DataFrame) -> str:
    """
    Превращаем весь Excel в компактный текст, чтобы ИИ сам нашёл заголовки.
    Важно: мы не предполагаем, где заголовки — отправляем всё.
    """
    # Всё в строки
    df = df.fillna("")
    # Сохраняем как TSV без индексов/заголовков, чтобы не ломать структуру
    tsv = df.astype(str).to_csv(sep="\t", index=False, header=False)
    return tsv


async def ask_ai_calculate(tsv_text: str) -> str:
    user_prompt = f"""Вот содержимое Excel (TSV, строки как в файле, включая служебные строки):
{tsv_text}

Сделай расчет и выведи ответ строго в требуемом формате.
"""

    # Используем chat.completions — максимально совместимо с OpenRouter
    resp = client.chat.completions.create(
        model=OPENROUTER_MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        # чтобы ответ был коротким
        max_tokens=800,
    )
    return resp.choices[0].message.content.strip()


# =====================
# Handlers
# =====================
@dp.message(CommandStart())
async def start(msg: Message):
    WAITING_FOR_FILE.discard(msg.from_user.id)
    await msg.answer(
        "Нажми кнопку «📊 Посчитать чистую прибыль».",
        reply_markup=menu_kb
    )


@dp.message(F.text == "📊 Посчитать чистую прибыль")
async def ask_file(msg: Message):
    WAITING_FOR_FILE.add(msg.from_user.id)
    await msg.answer("Пришли XLSX-файл отчета OZON «Начисления».")


@dp.message(F.document)
async def handle_file(msg: Message):
    # Принимаем файл только если пользователь нажал кнопку
    if msg.from_user.id not in WAITING_FOR_FILE:
        await msg.answer("Сначала нажми «📊 Посчитать чистую прибыль», потом пришли файл.")
        return

    if not msg.document.file_name.lower().endswith(".xlsx"):
        await msg.answer("Нужен файл в формате .xlsx")
        return

    WAITING_FOR_FILE.discard(msg.from_user.id)
    await msg.answer("Считаю…")

    try:
        tg_file = await bot.get_file(msg.document.file_id)
        buf = await bot.download_file(tg_file.file_path)
        file_bytes = buf.read()

        # Читаем как есть, без попыток угадать заголовки
        df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl", header=None)

        tsv_text = excel_to_compact_text(df)

        # Защита от сверх-огромных файлов (иначе любой LLM упадёт по лимиту)
        # Если нужно — скажи, сделаю авто-нарезку и многошаговый расчет.
        if len(tsv_text) > 180_000:
            await msg.answer("Файл слишком большой для обработки ИИ за один запрос. Уменьши период отчета и попробуй снова.")
            return

        result = await ask_ai_calculate(tsv_text)
        await msg.answer(result)

    except Exception as e:
        s = str(e)

        # Частые ошибки OpenRouter/OpenAI совместимые
        if "401" in s or "invalid_api_key" in s or "Incorrect API key" in s:
            await msg.answer("❌ Неверный OPENROUTER_API_KEY. Проверь ключ и Redeploy.")
        elif "429" in s or "insufficient_quota" in s or "rate limit" in s.lower():
            await msg.answer("❌ Лимит/баланс OpenRouter закончился или сработал rate limit. Пополни баланс и попробуй снова.")
        else:
            await msg.answer("❌ Ошибка при обработке файла. Проверь, что это XLSX «Начисления», и попробуй снова.")

        print("ERROR:", e)


# =====================
# Run
# =====================
async def main():
    # Важно: только один экземпляр бота должен работать (иначе Conflict)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
