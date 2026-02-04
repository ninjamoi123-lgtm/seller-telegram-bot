import os
import io
import asyncio
import pandas as pd

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.filters import CommandStart

from openai import OpenAI

BOT_TOKEN = os.environ["BOT_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")

client = OpenAI(api_key=OPENAI_API_KEY)

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

# ---------- КНОПКИ ----------
menu_kb = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="📊 Посчитать чистую прибыль")]],
    resize_keyboard=True
)

# ---------- START ----------
@dp.message(CommandStart())
async def start(msg: Message):
    await msg.answer(
        "Нажми кнопку ниже, чтобы посчитать чистую прибыль по отчету OZON «Начисления».",
        reply_markup=menu_kb
    )

# ---------- КНОПКА ----------
@dp.message(F.text == "📊 Посчитать чистую прибыль")
async def ask_file(msg: Message):
    await msg.answer("Пришли Excel-файл отчета «Начисления» (.xlsx)")

# ---------- ФАЙЛ ----------
@dp.message(F.document)
async def handle_file(msg: Message):
    if not msg.document.file_name.lower().endswith(".xlsx"):
        await msg.answer("Нужен файл в формате XLSX.")
        return

    file = await bot.get_file(msg.document.file_id)
    file_bytes = (await bot.download_file(file.file_path)).read()

    # читаем excel как таблицу
    df = pd.read_excel(io.BytesIO(file_bytes), header=None)
    table_text = df.astype(str).values.tolist()

    PROMPT = f"""
Ты — финансовый аналитик маркетплейса OZON.

Вот данные Excel-отчета «Начисления» (в виде таблицы):
{table_text}

Выполни расчет строго по инструкции:

1. Найди строку заголовков.
2. Определи колонку SKU и «Сумма итого».
3. Посчитай:
- Выручку = сумма «Сумма итого»
- УСН 6% со всей выручки
- Чистую прибыль = выручка − УСН

4. Выведи результат строго в формате:

1) Общая выручка (на расчетный счет): ХХХ ₽
2) Общая чистая прибыль (после УСН 6%): ХХХ ₽
3) По каждому SKU:
- SKU XXXXX: выручка ХХХ ₽, чистая прибыль ХХХ ₽

НИКАКИХ пояснений.
"""

    response = client.responses.create(
        model=OPENAI_MODEL,
        input=PROMPT
    )

    text = response.output_text
    await msg.answer(text)

# ---------- RUN ----------
async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
