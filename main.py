import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters


# ========= НАСТРОЙКИ =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "bot.db")
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Нет TELEGRAM_BOT_TOKEN. Добавь переменную окружения TELEGRAM_BOT_TOKEN в Railway.")

TMP_DIR.mkdir(parents=True, exist_ok=True)


# ========= КНОПКИ =========
MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        ["📈 Прибыль за период"],
        ["📦 Загрузить себестоимость (SKU → ₽)"],
        ["⬅️ В меню"],
    ],
    resize_keyboard=True
)

MODE_KB = ReplyKeyboardMarkup(
    keyboard=[
        ["🟡 Деньги от OZON"],
        ["🟢 Чистая прибыль"],
        ["⬅️ В меню"],
    ],
    resize_keyboard=True
)

BACK_TO_MENU_KB = ReplyKeyboardMarkup(
    keyboard=[
        ["⬅️ В меню"],
    ],
    resize_keyboard=True
)


# ========= SQLITE =========
def db():
    return sqlite3.connect(DB_PATH)

def init_db():
    with db() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS cogs (
            tg_id INTEGER NOT NULL,
            sku TEXT NOT NULL,
            cogs REAL NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (tg_id, sku)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS profit_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER NOT NULL,
            mode TEXT NOT NULL,
            file_name TEXT NOT NULL,
            revenue REAL NOT NULL,
            deductions REAL NOT NULL,
            net_mp REAL NOT NULL,
            cogs_total REAL,
            net_profit REAL,
            margin REAL,
            created_at TEXT NOT NULL,
            note TEXT
        )
        """)

def upsert_cogs(tg_id: int, sku: str, cogs_val: float):
    with db() as conn:
        conn.execute(
            "INSERT INTO cogs(tg_id, sku, cogs, updated_at) VALUES(?,?,?,?) "
            "ON CONFLICT(tg_id, sku) DO UPDATE SET cogs=excluded.cogs, updated_at=excluded.updated_at",
            (tg_id, sku, float(cogs_val), datetime.utcnow().strftime("%Y-%m-%d"))
        )

def get_cogs_map(tg_id: int) -> dict:
    with db() as conn:
        rows = conn.execute("SELECT sku, cogs FROM cogs WHERE tg_id=?", (tg_id,)).fetchall()
    return {r[0]: float(r[1]) for r in rows}

def save_report(tg_id: int, payload: dict):
    with db() as conn:
        conn.execute("""
        INSERT INTO profit_reports(
            tg_id, mode, file_name, revenue, deductions, net_mp,
            cogs_total, net_profit, margin, created_at, note
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            tg_id,
            payload["mode"],
            payload["file_name"],
            float(payload["revenue"]),
            float(payload["deductions"]),
            float(payload["net_mp"]),
            payload.get("cogs_total"),
            payload.get("net_profit"),
            payload.get("margin"),
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            payload.get("note", ""),
        ))


# ========= УТИЛИТЫ =========
def money(x: float) -> str:
    if x is None:
        return "0 ₽"
    if abs(x - int(x)) < 1e-9:
        return f"{int(x)} ₽"
    return f"{x:.2f} ₽"

def pct(x: float) -> str:
    return f"{x:.2f}%"

def norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()

def parse_number(x):
    if x is None:
        return None
    s = str(x).replace("\u00A0", "").replace(" ", "").replace(",", ".").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


# ========= ЧТЕНИЕ XLSX =========
def load_xlsx_rows(path: str):
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows or len(rows) < 2:
        raise ValueError("Файл пустой или без данных.")
    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    data = rows[1:]
    return header, data

def find_col_index(header, keywords):
    for i, col in enumerate(header):
        nc = norm(col)
        for kw in keywords:
            if kw in nc:
                return i
    return None

def parse_report_xlsx(path: str):
    header, data = load_xlsx_rows(path)

    amount_idx = find_col_index(header, ["итого"])
    if amount_idx is None:
        amount_idx = find_col_index(header, ["сумм", "начисл", "amount"])

    if amount_idx is None:
        # запасной вариант: ищем колонку с наибольшим количеством чисел
        best_i, best_score = None, 0
        for i in range(len(header)):
            score = 0
            for r in data[:2000]:
                v = parse_number(r[i] if i < len(r) else None)
                if v is not None:
                    score += 1
            if score > best_score:
                best_score = score
                best_i = i
        amount_idx = best_i

    if amount_idx is None:
        raise ValueError("Не нашёл колонку с суммой/итого.")

    sku_idx = find_col_index(header, ["sku", "offer", "артикул"])
    qty_idx = find_col_index(header, ["кол", "quantity", "qty"])

    revenue = 0.0
    deductions = 0.0
    total = 0.0

    by_sku_amount = {}

    for r in data:
        if amount_idx >= len(r):
            continue
        amt = parse_number(r[amount_idx])
        if amt is None:
            continue

        total += amt
        if amt > 0:
            revenue += amt
        elif amt < 0:
            deductions += amt

        sku = ""
        if sku_idx is not None and sku_idx < len(r):
            sku = str(r[sku_idx]).strip() if r[sku_idx] is not None else ""

        if sku:
            by_sku_amount[sku] = by_sku_amount.get(sku, 0.0) + amt

    note = f"amount_col_idx={amount_idx} | sku_idx={sku_idx if sku_idx is not None else 'NOT_FOUND'} | qty_idx={qty_idx if qty_idx is not None else 'NOT_FOUND'}"
    return {
        "revenue": float(revenue),
        "deductions": float(deductions),
        "total": float(total),
        "by_sku_amount": by_sku_amount,
        "note": note,
        "header": header,
        "amount_idx": amount_idx,
        "sku_idx": sku_idx,
        "qty_idx": qty_idx,
        "data": data,
    }

def top_lines_dict(d: dict, n=5, ascending=False):
    if not d:
        return "нет данных"
    items = sorted(d.items(), key=lambda x: x[1], reverse=not ascending)[:n]
    return "\n".join([f"{k} — {money(float(v))}" for k, v in items])

def parse_cogs_xlsx(path: str):
    header, data = load_xlsx_rows(path)

    sku_idx = find_col_index(header, ["sku", "артикул", "offer"])
    cogs_idx = find_col_index(header, ["cogs", "себест", "себестоим", "cost"])

    if sku_idx is None or cogs_idx is None:
        raise ValueError("В файле себестоимости нужны колонки: sku и cogs (или 'артикул' и 'себестоимость').")

    rows = []
    for r in data:
        if sku_idx >= len(r) or cogs_idx >= len(r):
            continue
        sku = str(r[sku_idx]).strip() if r[sku_idx] is not None else ""
        cogs_val = parse_number(r[cogs_idx])
        if not sku or cogs_val is None:
            continue
        rows.append((sku, float(cogs_val)))
    return rows


# ========= BOT =========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Привет 👋\n\n"
        "Я считаю прибыль по отчёту OZON «Начисления».\n"
        "✅ Формат файлов: ТОЛЬКО Excel (.xlsx)\n\n"
        "Выбери действие ⬇️",
        reply_markup=MAIN_KB
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()
    print("TEXT:", repr(text), "STATE:", dict(context.user_data))

    # кнопки — в начале
    if text == "⬅️ В меню":
        context.user_data.clear()
        await update.message.reply_text("Меню ⬇️", reply_markup=MAIN_KB)
        return

    if text == "📈 Прибыль за период":
        context.user_data.clear()
        await update.message.reply_text(
            "Как считать?\n\n"
            "🟡 Деньги от OZON — итог по отчёту (в плюсе/в минусе)\n"
            "🟢 Чистая прибыль — нужна себестоимость SKU → ₽\n\n"
            "Пришли .xlsx файл после выбора режима ⬇️",
            reply_markup=MODE_KB
        )
        return

    if text == "🟡 Деньги от OZON":
        context.user_data.clear()
        context.user_data["mode"] = "mp_money"
        context.user_data["await_report"] = True
        await update.message.reply_text(
            "Ок. Пришли .xlsx отчёт OZON «Начисления» за нужный период.",
            reply_markup=MODE_KB
        )
        return

    if text == "🟢 Чистая прибыль":
        context.user_data.clear()
        context.user_data["mode"] = "net_profit"
        context.user_data["await_report"] = True
        await update.message.reply_text(
            "Ок. Пришли .xlsx отчёт OZON «Начисления» за нужный период.\n\n"
            "Если себестоимость ещё не загружал — сначала загрузи через «📦 Загрузить себестоимость (SKU → ₽)».",
            reply_markup=MODE_KB
        )
        return

    if text == "📦 Загрузить себестоимость (SKU → ₽)":
        context.user_data.clear()
        context.user_data["await_cogs"] = True
        await update.message.reply_text(
            "Пришли Excel (.xlsx) файл себестоимости.\n\n"
            "В таблице должны быть колонки:\n"
            "• sku (или Артикул/offer)\n"
            "• cogs (или Себестоимость)\n\n"
            "Пример заголовков: sku | cogs",
            reply_markup=BACK_TO_MENU_KB
        )
        return

    # если ждём файл
    if context.user_data.get("await_report"):
        await update.message.reply_text("Я жду .xlsx отчёт «Начисления». Пришли документом.", reply_markup=MODE_KB)
        return

    if context.user_data.get("await_cogs"):
        await update.message.reply_text("Я жду .xlsx файл себестоимости. Пришли документом.", reply_markup=BACK_TO_MENU_KB)
        return

    await update.message.reply_text("Выбери действие кнопкой ⬇️", reply_markup=MAIN_KB)

async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    tg_id = update.effective_user.id
    if not doc:
        return

    file_name = doc.file_name or "file"
    suffix = Path(file_name).suffix.lower()
    tg_file = await context.bot.get_file(doc.file_id)

    # ТОЛЬКО XLSX
    if suffix != ".xlsx":
        await update.message.reply_text("Я принимаю только Excel (.xlsx).", reply_markup=MAIN_KB)
        return

    # --- себестоимость ---
    if context.user_data.get("await_cogs"):
        local_path = str(TMP_DIR / f"cogs_{tg_id}_{int(datetime.utcnow().timestamp())}.xlsx")
        await tg_file.download_to_drive(custom_path=local_path)

        try:
            rows = parse_cogs_xlsx(local_path)
            count = 0
            for sku, cogs_val in rows:
                upsert_cogs(tg_id, sku, cogs_val)
                count += 1

            context.user_data.clear()
            await update.message.reply_text(f"✅ Загружено себестоимостей: {count} SKU", reply_markup=MAIN_KB)
        except Exception as e:
            await update.message.reply_text(f"Ошибка файла себестоимости: {e}", reply_markup=BACK_TO_MENU_KB)
        return

    # --- отчёт начисления ---
    if context.user_data.get("await_report"):
        local_path = str(TMP_DIR / f"report_{tg_id}_{int(datetime.utcnow().timestamp())}.xlsx")
        await tg_file.download_to_drive(custom_path=local_path)

        mode = context.user_data.get("mode", "mp_money")

        try:
            parsed = parse_report_xlsx(local_path)
            revenue = parsed["revenue"]
            deductions = parsed["deductions"]
            net_mp = parsed["total"]

            if mode == "mp_money":
                status = "🟢" if net_mp > 0 else "🔴"
                msg = (
                    "📈 Итоги по отчёту OZON «Начисления»\n\n"
                    f"Начислено (плюс): {money(revenue)}\n"
                    f"Удержания (минус): {money(deductions)}\n"
                    f"Итого от OZON: {money(net_mp)}\n"
                    f"Статус: {status}\n\n"
                    f"Тех.инфо: {parsed['note']}\n\n"
                )

                by_sku = parsed["by_sku_amount"]
                if by_sku:
                    msg += "ТОП-5 SKU по итогу:\n" + top_lines_dict(by_sku, 5, ascending=False) + "\n\n"
                    msg += "ТОП-5 SKU в минус:\n" + top_lines_dict(by_sku, 5, ascending=True) + "\n"
                else:
                    msg += "ТОП SKU: нет (не нашёл колонку SKU/offer_id/артикул)\n"

                save_report(tg_id, {
                    "mode": "mp_money",
                    "file_name": file_name,
                    "revenue": revenue,
                    "deductions": deductions,
                    "net_mp": net_mp,
                    "note": parsed["note"],
                })

                context.user_data.clear()
                await update.message.reply_text(msg, reply_markup=MAIN_KB)
                return

            # net_profit
            cogs_map = get_cogs_map(tg_id)
            if not cogs_map:
                status = "🟢" if net_mp > 0 else "🔴"
                msg = (
                    "🟢 Чистая прибыль\n\n"
                    "Себестоимость не загружена.\n"
                    "Показываю деньги от OZON (без себестоимости):\n\n"
                    f"Итого от OZON: {money(net_mp)}\n"
                    f"Статус: {status}\n\n"
                    "Загрузи себестоимость через «📦 Загрузить себестоимость (SKU → ₽)» и повтори расчёт."
                )
                save_report(tg_id, {
                    "mode": "net_profit",
                    "file_name": file_name,
                    "revenue": revenue,
                    "deductions": deductions,
                    "net_mp": net_mp,
                    "note": "NO_COGS | " + parsed["note"],
                })
                context.user_data.clear()
                await update.message.reply_text(msg, reply_markup=MAIN_KB)
                return

            # считаем себестоимость по SKU (qty пока не используем, если нет колонки qty — считаем 1)
            sku_idx = parsed["sku_idx"]
            qty_idx = parsed["qty_idx"]
            data = parsed["data"]

            if sku_idx is None:
                msg = (
                    "🟢 Чистая прибыль\n\n"
                    "В отчёте не нашёл SKU/offer_id/артикул — не могу применить себестоимость.\n"
                    "Проверь, что в выгрузке есть артикулы."
                )
                context.user_data.clear()
                await update.message.reply_text(msg, reply_markup=MAIN_KB)
                return

            cogs_total = 0.0
            by_sku_profit = {}

            for r in data:
                if sku_idx >= len(r):
                    continue
                sku = str(r[sku_idx]).strip() if r[sku_idx] is not None else ""
                if not sku:
                    continue

                amt = None
                if parsed["amount_idx"] < len(r):
                    amt = parse_number(r[parsed["amount_idx"]])
                if amt is None:
                    continue

                qty = 1.0
                if qty_idx is not None and qty_idx < len(r):
                    q = parse_number(r[qty_idx])
                    if q is not None and q > 0:
                        qty = float(q)

                c = float(cogs_map.get(sku, 0.0)) * qty
                cogs_total += c

                # прибыль по SKU = сумма начислений по SKU - себестоимость
                by_sku_profit[sku] = by_sku_profit.get(sku, 0.0) + (amt - c)

            net_profit = net_mp - cogs_total
            margin = (net_profit / revenue * 100.0) if revenue > 0 else 0.0
            status = "🔴" if net_profit <= 0 else ("🟡" if margin < 15 else "🟢")

            msg = (
                "🟢 Чистая прибыль по отчёту OZON «Начисления»\n\n"
                f"Итого от OZON: {money(net_mp)}\n"
                f"Себестоимость: {money(cogs_total)}\n\n"
                f"Чистая прибыль: {money(net_profit)}\n"
                f"Маржа: {pct(margin)}\n"
                f"Статус: {status}\n\n"
                f"Тех.инфо: {parsed['note']}\n\n"
                "ТОП-5 SKU по прибыли:\n"
                f"{top_lines_dict(by_sku_profit, 5, ascending=False)}\n\n"
                "ТОП-5 SKU в минус:\n"
                f"{top_lines_dict(by_sku_profit, 5, ascending=True)}\n"
            )

            save_report(tg_id, {
                "mode": "net_profit",
                "file_name": file_name,
                "revenue": revenue,
                "deductions": deductions,
                "net_mp": net_mp,
                "cogs_total": cogs_total,
                "net_profit": net_profit,
                "margin": margin,
                "note": parsed["note"],
            })

            context.user_data.clear()
            await update.message.reply_text(msg, reply_markup=MAIN_KB)

        except Exception as e:
            await update.message.reply_text(
                f"Не смог разобрать файл 😕\n\nОшибка: {e}\n\n"
                "Проверь, что это Excel (.xlsx) и это отчёт OZON «Начисления».",
                reply_markup=MODE_KB
            )
        return

    await update.message.reply_text("Я сейчас не жду файл. Нажми «📈 Прибыль за период».", reply_markup=MAIN_KB)


def main():
    init_db()
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.run_polling()


if __name__ == "__main__":
    main()
