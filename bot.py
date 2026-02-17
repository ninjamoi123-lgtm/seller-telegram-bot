import os
import base64
from dataclasses import dataclass
from typing import Optional
from PIL import Image
import io

from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message, CallbackQuery
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


def main_menu():
    kb = InlineKeyboardBuilder()
    kb.button(text="📊 Рассчитать прибыль (SKU)", callback_data="menu_profit")
    kb.button(text="🖼 Фото для карточки OZON", callback_data="menu_image")
    kb.adjust(1)
    return kb.as_markup()


@dp.message(Command("start"))
async def start(message: Message):
    await message.answer(
        "Привет! Я помогу:\n"
        "1) Посчитать чистую прибыль (точной математикой)\n"
        "2) Сделать фото для карточки (ИИ)\n\n"
        "Выбирай 👇",
        reply_markup=main_menu(),
    )


@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Ок, вернулись в меню 👇", reply_markup=main_menu())
    await cb.answer()


# ---------- Profit flow ----------
@dp.callback_query(F.data == "menu_profit")
async def profit_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ProfitFlow.cogs)
    await cb.message.answer("Введи себестоимость товара (₽). Например: 1500")
    await cb.answer()


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
        kb.button(text="⬅️ В меню", callback_data="back_to_menu")
        kb.adjust(1)

        await state.clear()
        await message.answer(text, reply_markup=kb.as_markup())
    except Exception:
        await message.answer("Не понял число. Введи, например: 6")


# ---------- Image flow ----------
def image_style_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="⚪ Белый фон (OZON)", callback_data="style_white")
    kb.button(text="✨ Премиум студия", callback_data="style_premium")
    kb.button(text="🌆 Lifestyle", callback_data="style_lifestyle")
    kb.button(text="⬅️ В меню", callback_data="back_to_menu")
    kb.adjust(1)
    return kb.as_markup()


@dp.callback_query(F.data == "menu_image")
async def image_start(cb: CallbackQuery, state: FSMContext):
    await state.set_state(ImageFlow.photo)
    await cb.message.answer("Загрузи фото товара (чёткое, товар полностью в кадре).")
    await cb.answer()


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
    base = "Professional e-commerce product photo. Keep the product realistic and consistent with the original. No extra objects."
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

    image = Image.open(io.BytesIO(raw_bytes)).convert("RGBA")
    png_buffer = io.BytesIO()
    image.save(png_buffer, format="PNG")
    input_image_bytes = png_buffer.getvalue()

    try:
        result = client.images.edit(
            model="gpt-image-1",
            image=[("image.png", input_image_bytes)],
            prompt=prompt,
            size="1024x1024",
        )
        b64 = result.data[0].b64_json
        out = base64.b64decode(b64)

        await message.answer_photo(out, caption="✅ Готово. Нажми /start чтобы сделать ещё.")
    except Exception as e:
        await message.answer(
            "Не получилось сгенерировать. Часто помогает другое фото или другой стиль.\n\n"
            f"Ошибка: {type(e).__name__}"
        )
    finally:
        await state.clear()


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
