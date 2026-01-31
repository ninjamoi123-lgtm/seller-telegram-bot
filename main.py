import os
import re
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters

from openai import OpenAI

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Нет TELEGRAM_BOT_TOKEN. Добавь переменную окружения в Render.")
if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY. Добавь переменную окружения в Render.")

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_PROMPT = (
    "Ты финансовый стратег маркетплейсов Ozon и WB.\n"
    "Твоя задача: анализ SKU по данным пользователя.\n"
    "Формат ответа строго:\n"
    "1) Чистая прибыль (₽) и маржа (%)\n"
    "2) Статус SKU (🟢🟡🔴)\n"
    "3) Рекомендация по цене (держать/поднять/снизить)\n"
    "4) Можно ли участвовать в акции (да/нет) + кратко почему\n"
    "5) Что будет при изменении цены на +5% и −10% (прибыль/маржа)\n"
    "Пиши кратко, цифрами, языком денег. Без воды."
)

TEMPLATE = (
    "Скопируй шаблон и заполни числа:\n\n"
    "SKU: ...\n"
    "Цена покупателя: 878\n"
    "Себестоимость: 380\n"
    "Комиссия МП: 33\n"
    "Логистика МП: 82.36\n"
    "Старт-цена: 1688\n"
    "Акция: 20\n"
    "Цены конкурентов: 1236, 735, 767\n"
)

KB = ReplyKeyboardMarkup(
    keyboard=[
        ["📋 Шаблон SKU", "📊 Посчитать"],
        ["📈 Что если +5%", "📉 Что если −10%"],
        ["🔥 Можно ли в акцию", "♻️ Очистить"],
    ],
    resize_keyboard=True
)

def _extract_number(text: str) -> float | None:
    # вытаскивает первое число из строки (учитывает запятую)
    m = re.search(r"(-?\d+(?:[.,]\d+)?)", text)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))

def parse_sku_block(text: str) -> dict:
    # принимает блок строк вида "Ключ: значение"
    data = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        k, v = line.split(":", 1)
        key = k.strip().lower()
        val = v.strip()

        if key in ["sku"]:
            data["sku"] = val
        elif "цена покуп" in key:
            data["price"] = _extract_number(val)
        elif "себестоим" in key:
            data["cogs"] = _extract_number(val)
        elif "комис" in key:
            data["fee_pct"] = _extract_number(val)
        elif "логист" in key:
            data["log"] = _extract_number(val)
        elif "старт" in key:
            data["start_price"] = _extract_number(val)
        elif "акц" in key:
            data["promo_pct"] = _extract_number(val)
        elif "конкур" in key:
            # числа через запятую/пробел
            nums = re.findall(r"\d+(?:[.,]\d+)?", val)
            data["competitors"] = [float(x.replace(",", ".")) for x in nums] if nums else []
    return data

def data_is_ok(d: dict) -> tuple[bool, str]:
    required = ["sku", "price", "cogs", "fee_pct", "log", "promo_pct"]
    missing = [k for k in required if (k not in d or d[k] is None or (k == "sku" and not d[k]))]
    if missing:
        return False, "Не хватает полей: " + ", ".join(missing)
    return True, ""

def make_user_payload(d: dict) -> str:
    comps = d.get("competitors", [])
    return (
        f"SKU: {d.get('sku')}\n"
        f"Цена покупателя: {d.get('price')}\n"
        f"Себестоимость: {d.get('cogs')}\n"
        f"Комиссия МП (%): {d.get('fee_pct')}\n"
        f"Логистика МП: {d.get('log')}\n"
        f"Старт-цена: {d.get('start_price')}\n"
        f"Акция (%): {d.get('promo_pct')}\n"
        f"Цены конкурентов: {', '.join(map(str, comps)) if comps else 'нет данных'}\n"
    )

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["sku_raw"] = ""
    context.user_data["sku_parsed"] = {}
    await update.message.reply_text("Я бот-аналитик SKU для OZON/WB. Жми кнопки 👇", reply_markup=KB)

async def handle_any_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "📋 Шаблон SKU":
        await update.message.reply_text(TEMPLATE, reply_markup=KB)
        return

    if text == "♻️ Очистить":
        context.user_data["sku_raw"] = ""
        context.user_data["sku_parsed"] = {}
        await update.message.reply_text("Ок, очистил. Вставь данные заново или жми “Шаблон SKU”.", reply_markup=KB)
        return

    # если пользователь прислал блок данных — запомним
    if ":" in text and any(k in text.lower() for k in ["sku", "цена", "себест", "комисс", "логист", "акц"]):
        context.user_data["sku_raw"] = text
        parsed = parse_sku_block(text)
        context.user_data["sku_parsed"] = parsed
        ok, reason = data_is_ok(parsed)
        if ok:
            await update.message.reply_text("Данные принял ✅ Жми “Посчитать”.", reply_markup=KB)
        else:
            await update.message.reply_text(f"Принял, но: {reason}\n\nЖми “Шаблон SKU” и заполни всё.", reply_markup=KB)
        return

    # действия кнопками
    action_map = {
        "📊 Посчитать": "Посчитай по этим данным.",
        "📈 Что если +5%": "Посчитай, что будет если цену покупателя увеличить на +5%.",
        "📉 Что если −10%": "Посчитай, что будет если цену покупателя снизить на −10%.",
        "🔥 Можно ли в акцию": "Ответь, можно ли участвовать в акции при текущих данных. Если нельзя — какая минимальная цена/маржа нужна.",
    }

    if text in action_map:
        parsed = context.user_data.get("sku_parsed", {}) or {}
        ok, reason = data_is_ok(parsed)
        if not ok:
            await update.message.reply_text(f"Сначала пришли данные SKU.\n{reason}\n\nЖми “Шаблон SKU”.", reply_markup=KB)
            return

        await update.message.chat.send_action(action="typing")

        user_payload = make_user_payload(parsed)
        user_task = action_map[text] + "\n\nДанные:\n" + user_payload

        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_task},
                ],
                temperature=0.2,
            )
            answer = resp.choices[0].message.content
            await update.message.reply_text(answer, reply_markup=KB)
        except Exception as e:
            await update.message.reply_text(f"Ошибка OpenAI: {e}", reply_markup=KB)
        return

    # если просто текст — объясним что делать
    await update.message.reply_text("Вставь блок SKU (как в шаблоне) или жми кнопки 👇", reply_markup=KB)

def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_any_text))
    app.run_polling()

if __name__ == "__main__":
    main()
