import os
import base64
from dataclasses import dataclass
from typing import Optional
import io
import json
import traceback

from PIL import Image
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from openai import OpenAI

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not BOT_TOKEN:
    raise RuntimeError("Нет BOT_TOKEN в переменных окружения (Variables) или .env")
if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY в переменных окружения (Variables) или .env")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
client = OpenAI(api_key=OPENAI_API_KEY)


# ---------------------------
# FSM states
# ---------------------------
class ProfitFlow(StatesGroup):
    cogs = State()
    price = State()
    commission = State()
    logistics = State()
    tax = State()


class ImageFlow(StatesGroup):
    photo = State()
    style = State()
    text = State()


class DescriptionFlow(StatesGroup):
    info = State()        # пользователь описывает товар
    style = State()       # стиль текста (коротко/подробно/премиум)
    extra = State()       # доп. требования (опционально)


# ---------------------------
# Business logic (math)
# ---------------------------
@dataclass
class ProfitInput:
    cogs: float
    price: float
    commission_pct: float
    logistics: float
    tax_pct: float


def parse_float_ru(text: str) -> float:
    t = text.strip().replace(" ", "").replace(",", ".")
    return float(t)


def calc_profit(x: ProfitInput) -> dict:
    commission = x.price * (x.commission_pct / 100.0)
    tax = x.price * (x.tax_pct / 100.0)
    profit = x.price - commission - x.logistics - tax - x.cogs
    margin_pct = (profit / x.price * 100.0) if x.price > 0 else 0.0
    return {"commission": commission, "tax": tax, "profit": profit, "margin_pct": margin_pct}


# ---------------------------
# Reply (bottom) menu keyboard — как на 3 скрине
# ---------------------------
def bottom_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏠 Главное меню")],
            [KeyboardButton(text="📊 Посчитать чистую прибыль"), KeyboardButton(text="🖼 Фото для карточки OZON")],
            [KeyboardButton(text="📝 Создать описание"), KeyboardButton(text="💼 Мои товары")],
            [KeyboardButton(text="💰 Тарифы")],
        ],
        resize_keyboard=True
    )


async def show_main_menu(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Главное меню 👇", reply_markup=bottom_menu_kb())


# ---------------------------
# OpenAI error logging helpers
# ---------------------------
def log_openai_error(context: dict, e: Exception):
    """
    Печатает в Railway Logs максимально понятную диагностику.
    """
    payload = {
        "tag": "OPENAI_ERROR",
        "context": context,
        "error_type": type(e).__name__,
        "error_str": str(e),
        "traceback": traceback.format_exc(limit=6),
    }
    print("OPENAI ERROR CONTEXT:", json.dumps(payload, ensure_ascii=False))


def classify_openai_error_message(err: str) -> str:
    """
    Человеческая классификация — что сказать пользователю.
    """
    low = err.lower()

    if "billing_hard_limit" in low or "hard limit" in low:
        return "billing_limit"
    if "incorrect api key" in low or "invalid_api_key" in low or "api key" in low and "invalid" in low:
        return "invalid_key"
    if "rate limit" in low or "rate_limit" in low:
        return "rate_limit"
    if "quota" in low and "exceeded" in low:
        return "quota"
    if "bad request" in low or "error code: 400" in low:
        return "bad_request"
    if "error code: 401" in low:
        return "unauthorized"
    if "error code: 403" in low:
        return "forbidden"
    if "error code: 413" in low or "too large" in low:
        return "too_large"
    if "timeout" in low or "timed out" in low:
        return "timeout"
    return "unknown"


def user_friendly_error_text(kind: str) -> str:
    if kind == "billing_limit":
        return (
            "⚠️ Генерация временно недоступна.\n"
            "Причина: достигнут лимит оплаты OpenAI (Billing hard limit).\n\n"
            "Решение: пополнить баланс/подключить оплату в OpenAI Billing."
        )
    if kind == "invalid_key":
        return (
            "⚠️ Ошибка ключа OpenAI.\n"
            "Проверь переменную OPENAI_API_KEY в Railway → Variables.\n"
            "После изменения сделай Redeploy."
        )
    if kind in ("rate_limit", "timeout"):
        return (
            "⚠️ Временная перегрузка/лимит запросов.\n"
            "Попробуй ещё раз через минуту."
        )
    if kind == "too_large":
        return (
            "⚠️ Фото слишком тяжёлое.\n"
            "Попробуй другое фото или сделай его меньше/проще."
        )
    # универсально
    return (
        "⚠️ Не получилось выполнить запрос к OpenAI.\n"
        "Я записал причину в логи Railway (Logs)."
    )


# ---------------------------
# /start & main menu
# ---------------------------
@dp.message(Command("start"))
async def start(message: Message, state: FSMContext):
    await show_main_menu(message, state)


@dp.message(F.text == "🏠 Главное меню")
async def go_main_menu(message: Message, state: FSMContext):
    await show_main_menu(message, state)


@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Главное меню 👇", reply_markup=bottom_menu_kb())
    await cb.answer()


# ---------------------------
# PROFIT flow
# ---------------------------
@dp.message(F.text == "📊 Посчитать чистую прибыль")
async def profit_from_bottom_menu(message: Message, state: FSMContext):
    await state.set_state(ProfitFlow.cogs)
    await message.answer("Введи себестоимость товара (₽). Например: 1500")


@dp.message(ProfitFlow.cogs)
async def profit_cogs(message: Message, state: FSMContext):
    try:
        cogs = parse_float_ru(message.text)
        await state.update_data(cogs=cogs)
        await state.set_state(ProfitFlow.price)
        await message.answer("Теперь введи цену продажи на OZON (₽). Например: 3990")
    except Exception:
        await message.answer("Не понял число. Введи, например: 1500")


@dp.message(ProfitFlow.price)
async def profit_price(message: Message, state: FSMContext):
    try:
        price = parse_float_ru(message.text)
        await state.update_data(price=price)
        await state.set_state(ProfitFlow.commission)
        await message.answer("Комиссия OZON (%)? Например: 12")
    except Exception:
        await message.answer("Не понял число. Введи, например: 3990")


@dp.message(ProfitFlow.commission)
async def profit_commission(message: Message, state: FSMContext):
    try:
        commission = parse_float_ru(message.text)
        await state.update_data(commission=commission)
        await state.set_state(ProfitFlow.logistics)
        await message.answer("Логистика/фулфилмент (₽)? Например: 350")
    except Exception:
        await message.answer("Не понял число. Введи, например: 12")


@dp.message(ProfitFlow.logistics)
async def profit_logistics(message: Message, state: FSMContext):
    try:
        logistics = parse_float_ru(message.text)
        await state.update_data(logistics=logistics)
        await state.set_state(ProfitFlow.tax)
        await message.answer("Налог (%)? Например: 6")
    except Exception:
        await message.answer("Не понял число. Введи, например: 350")


@dp.message(ProfitFlow.tax)
async def profit_tax(message: Message, state: FSMContext):
    try:
        tax = parse_float_ru(message.text)
        data = await state.get_data()

        x = ProfitInput(
            cogs=float(data["cogs"]),
            price=float(data["price"]),
            commission_pct=float(data["commission"]),
            logistics=float(data["logistics"]),
            tax_pct=float(tax),
        )
        r = calc_profit(x)

        text = (
            "📊 Расчёт:\n\n"
            f"Цена продажи: {x.price:,.0f} ₽\n"
            f"Комиссия: -{r['commission']:,.0f} ₽\n"
            f"Логистика: -{x.logistics:,.0f} ₽\n"
            f"Налог: -{r['tax']:,.0f} ₽\n"
            f"Себестоимость: -{x.cogs:,.0f} ₽\n\n"
            f"✅ Чистая прибыль: {r['profit']:,.0f} ₽\n"
            f"📈 Маржа: {r['margin_pct']:.1f}%\n"
        )

        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ В главное меню", callback_data="back_to_menu")
        kb.adjust(1)

        await state.clear()
        await message.answer(text, reply_markup=kb.as_markup())

    except Exception:
        await message.answer("Не понял число. Введи, например: 6")


# ---------------------------
# IMAGE flow
# ---------------------------
def image_style_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⚪️ Белый фон (OZON)", callback_data="style_white")
    kb.button(text="✨ Премиум студия", callback_data="style_premium")
    kb.button(text="🌆 Lifestyle", callback_data="style_lifestyle")
    kb.button(text="⬅️ В главное меню", callback_data="back_to_menu")
    kb.adjust(1)
    return kb.as_markup()


@dp.message(F.text == "🖼 Фото для карточки OZON")
async def image_from_bottom_menu(message: Message, state: FSMContext):
    await state.set_state(ImageFlow.photo)
    await message.answer("Загрузи фото товара (чёткое, товар полностью в кадре).")


@dp.message(ImageFlow.photo, F.photo)
async def image_got_photo(message: Message, state: FSMContext):
    file_id = message.photo[-1].file_id
    await state.update_data(file_id=file_id)
    await state.set_state(ImageFlow.style)
    await message.answer("Выбери стиль:", reply_markup=image_style_kb())


@dp.message(ImageFlow.photo)
async def image_need_photo(message: Message):
    await message.answer("Пожалуйста, отправь именно фото товара (как фотографию в Telegram).")


@dp.callback_query(ImageFlow.style, F.data.startswith("style_"))
async def image_choose_style(cb: CallbackQuery, state: FSMContext):
    await state.update_data(style=cb.data)
    await state.set_state(ImageFlow.text)
    await cb.message.answer(
        "Хочешь добавить текст на картинку? Если не надо — отправь: -\n"
        "Пример: 'Хлопок 100%' или 'Гарантия 12 мес'\n\n"
        "⚠️ Не пиши то, чего нет у товара."
    )
    await cb.answer()


def build_image_prompt(style_code: str, user_text: Optional[str]) -> str:
    base = (
        "Professional e-commerce product photo. "
        "Keep the product realistic and consistent with the original. "
        "No extra objects."
    )

    if style_code == "style_white":
        style = "Clean white background, soft studio lighting, realistic shadow, centered composition, marketplace style."
    elif style_code == "style_premium":
        style = "Premium studio look, subtle gradient background, high-end commercial lighting, ultra realistic, sharp details."
    else:
        style = "Lifestyle scene that fits the product category, realistic environment, commercial photography, natural light, not artificial."

    text_part = ""
    if user_text and user_text.strip() and user_text.strip() != "-":
        text_part = f" Add minimal clean infographic text overlay: '{user_text.strip()}'."

    return f"{base} {style}{text_part}"


@dp.message(ImageFlow.text)
async def image_make(message: Message, state: FSMContext):
    data = await state.get_data()
    file_id = data["file_id"]
    style_code = data["style"]
    user_text = message.text.strip() if message.text else "-"
    prompt = build_image_prompt(style_code, user_text)

    await message.answer("⏳ Делаю изображение...")

    tg_file = await bot.get_file(file_id)
    file_bytes = await bot.download_file(tg_file.file_path)
    raw_bytes = file_bytes.read()

    # Нормализация: Telegram -> PNG + ресайз
    image = Image.open(io.BytesIO(raw_bytes)).convert("RGBA")

    max_side = 1024
    w, h = image.size
    scale = min(max_side / w, max_side / h, 1.0)
    if scale < 1.0:
        image = image.resize((int(w * scale), int(h * scale)))

    png_buffer = io.BytesIO()
    image.save(png_buffer, format="PNG", optimize=True)
    input_image_bytes = png_buffer.getvalue()

    try:
        result = client.images.edit(
            model="gpt-image-1",
            image=[("image.png", input_image_bytes, "image/png")],
            prompt=prompt,
            size="1024x1024",
        )
        b64 = result.data[0].b64_json
        out = base64.b64decode(b64)

        await message.answer_photo(out, caption="✅ Готово. Нажми «🏠 Главное меню» или /start.")
    except Exception as e:
        err = str(e)
        kind = classify_openai_error_message(err)
        log_openai_error(
            context={"feature": "image_edit", "style": style_code, "user_text": user_text[:80]},
            e=e,
        )
        await message.answer(user_friendly_error_text(kind))
    finally:
        await state.clear()


# ---------------------------
# DESCRIPTION (text) generation
# ---------------------------
def description_style_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⚡ Коротко", callback_data="desc_short")
    kb.button(text="🧾 Подробно", callback_data="desc_detailed")
    kb.button(text="✨ Премиум", callback_data="desc_premium")
    kb.button(text="⬅️ В главное меню", callback_data="back_to_menu")
    kb.adjust(1)
    return kb.as_markup()


@dp.message(F.text == "📝 Создать описание")
async def description_start(message: Message, state: FSMContext):
    await state.set_state(DescriptionFlow.info)
    await message.answer(
        "Опиши товар одним сообщением.\n"
        "Например:\n"
        "«Мужская худи оверсайз, 100% хлопок, чёрная, размеры S-XL, без принта»\n\n"
        "Можно добавить: бренд, материал, размеры, особенности."
    )


@dp.message(DescriptionFlow.info)
async def description_got_info(message: Message, state: FSMContext):
    await state.update_data(info=message.text.strip())
    await state.set_state(DescriptionFlow.style)
    await message.answer("Выбери стиль текста:", reply_markup=description_style_kb())


@dp.callback_query(DescriptionFlow.style, F.data.startswith("desc_"))
async def description_choose_style(cb: CallbackQuery, state: FSMContext):
    await state.update_data(desc_style=cb.data)
    await state.set_state(DescriptionFlow.extra)
    await cb.message.answer(
        "Хочешь дополнительные требования? (например: «без эмодзи», «до 500 символов», «с ключевыми словами»)\n"
        "Если не надо — отправь: -"
    )
    await cb.answer()


@dp.message(DescriptionFlow.extra)
async def description_make(message: Message, state: FSMContext):
    data = await state.get_data()
    info = data.get("info", "")
    style = data.get("desc_style", "desc_detailed")
    extra = message.text.strip() if message.text else "-"

    if style == "desc_short":
        style_text = "Сделай коротко и по делу."
    elif style == "desc_premium":
        style_text = "Сделай премиально, аккуратно, без воды."
    else:
        style_text = "Сделай подробно, но читабельно."

    extra_text = "" if extra == "-" else f"Доп. требования: {extra}"

    await message.answer("⏳ Генерирую описание...")

    try:
        # Здесь модель для текста. Можно поменять на более дешёвую позже.
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=(
                "Ты помощник селлера OZON. "
                "Сгенерируй: 1) SEO-заголовок, 2) 5 буллетов преимуществ, 3) описание 2-4 абзаца, 4) ключевые слова.\n"
                "Не выдумывай характеристики: используй только то, что дал пользователь. Если данных мало — мягко укажи, чего не хватает.\n\n"
                f"Товар: {info}\n"
                f"Стиль: {style_text}\n"
                f"{extra_text}"
            ),
        )

        text_out = resp.output_text.strip()
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ В главное меню", callback_data="back_to_menu")
        kb.adjust(1)

        await state.clear()
        await message.answer(text_out, reply_markup=kb.as_markup())

    except Exception as e:
        err = str(e)
        kind = classify_openai_error_message(err)
        log_openai_error(
            context={"feature": "text_description", "style": style, "extra": extra[:80]},
            e=e,
        )
        await message.answer(user_friendly_error_text(kind))
        await state.clear()


# ---------------------------
# "Мои товары" (пока заглушка)
# ---------------------------
@dp.message(F.text == "💼 Мои товары")
async def my_items_placeholder(message: Message):
    await message.answer(
        "💼 Мои товары — скоро добавим.\n"
        "Там будет сохранение SKU / шаблонов описаний / быстрый пересчёт цен."
    )


# ---------------------------
# "Тарифы" (простая заглушка)
# ---------------------------
@dp.message(F.text == "💰 Тарифы")
async def tariffs(message: Message):
    await message.answer(
        "💰 Тарифы (пример):\n"
        "— Бесплатно: тест 3 генерации изображений\n"
        "— Пакеты: 50 / 100 / 300 изображений\n"
        "— Подписка: безлимит на тексты + скидка на изображения\n\n"
        "Если хочешь, сделаю это меню полностью рабочим."
    )


# ---------------------------
# Run
# ---------------------------
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
