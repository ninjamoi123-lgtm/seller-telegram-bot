import os
import re
import csv
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters


# ========= НАСТРОЙКИ =========
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
DB_PATH = os.getenv("DB_PATH", "bot.db")
TMP_DIR = Path(os.getenv("TMP_DIR", "/tmp"))

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Нет TELEGRAM_BOT_TOKEN. Добавь переменную окружения.")

TMP_DIR.mkdir(parents=True, exist_ok=True)


# ========= КНОПКИ (как ты хотел: 1 кнопка = 1 строка) =========
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


# ========= БАЗА SQLITE =========
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
    if abs(x - int(x)) < 1e-9:
        return f"{int(x)} ₽"
    return f"{x:.2f} ₽"

def pct(x: float) -> str:
    return f"{x:.2f}%"

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()

def _parse_number(x):
    if pd.isna(x):
        return None
    s = str(x).replace("\u00A0", "").replace(" ", "").replace(",", ".").strip()
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


# ========= ЧТЕНИЕ ОТЧЁТА (универсально) =========
def load_table(file_path: str) -> pd.DataFrame:
    p = Path(file_path)
    if p.suffix.lower() in (".xlsx", ".xls"):
        return pd.read_excel(file_path, engine="openpyxl")

    # csv
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        sample = f.read(4096)
    sep = ";" if sample.count(";") > sample.count(",") else ","
    return pd.read_csv(file_path, sep=sep, engine="python")

def find_amount_col(cols: list[str]) -> str | None:
    # сначала ищем "Итого" (самый лучший вариант)
    for c in cols:
        nc = _norm(c)
        if "итого" in nc:
            return c

    # потом любые суммы/начисления
    for c in cols:
        nc = _norm(c)
        if ("сумм" in nc) or ("начисл" in nc) or ("amount" in nc):
            return c

    return None

def find_sku_col(cols: list[str]) -> str | None:
    keys = ["sku", "offer", "артикул"]
    for c in cols:
        nc = _norm(c)
        if any(k in nc for k in keys):
            return c
    return None

def find_qty_col(cols: list[str]) -> str | None:
    keys = ["кол", "quantity", "qty"]
    for c in cols:
        nc = _norm(c)
        if any(k in nc for k in keys):
            return c
    return None

def parse_report(file_path: str) -> dict:
    df = load_table(file_path)
    if df.empty:
        raise ValueError("Файл пустой или не читается.")

    df.columns = [str(c).strip() for c in df.columns]
    cols = list(df.columns)

    amount_col = find_amount_col(cols)
    if not amount_col:
        # запасной вариант: выбираем колонку с максимальным числом чисел
        best, best_score = None, 0
        for c in cols:
            score = df[c].map(_parse_number).notna().sum()
            if score > best_score:
                best_score, best = score, c
        amount_col = best

    if not amount_col:
        raise ValueError("Не смог найти колонку с суммой/итого в отчёте.")

    sku_col = find_sku_col(cols)
    qty_col = find_qty_col(cols)

    df["_amount"] = df[amount_col].map(_parse_number)
    df = df[df["_amount"].notna()].copy()

    if sku_col:
        df["_sku"] = df[sku_col].astype(str).str.strip()
    else:
        df["_sku"] = ""

    if qty_col:
        df["_qty"] = df[qty_col].map(_parse_number).fillna(1).astype(float)
        df.loc[df["_qty"] <= 0, "_qty"] = 1.0
    else:
        df["_qty"] = 1.0

    total = float(df["_amount"].sum())
    revenue = float(df.loc[df["_amount"] > 0, "_amount"].sum())
    deductions = float(df.loc[df["_amount"] < 0, "_amount"].sum())

    by_sku_amount = None
    if sku_col:
        by_sku_amount = df.groupby("_sku")["_amount"].sum().sort_values(ascending=False)

    note = f"amount_col={amount_col} | sku_col={sku_col or 'NOT_FOUND'} | qty_col={qty_col or 'NOT_FOUND'}"
    return {"df": df, "total": total, "revenue": revenue, "deductions": deductions, "by_sku_amount": by_sku_amount, "note": note}

def top_lines(series: pd.Series | None, n: int = 5, ascending: bool = False) -> str:
    if series is None or series.empty:
        return "нет данных"
    s = series.sort_values(ascending=ascending).head(n)
    out = []
    for k, v in s.items():
        out.append(f"{k} — {money(float(v))}")
    return "\n".join(out)


# ========= BOT =========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        "Привет 👋\n\n"
        "Я считаю прибыль по отчёту OZON «Начисления».\n"
        "Период ты выбираешь в кабинете OZON сам, потом загружаешь файл сюда.\n\n"
        "Выбери действие ⬇️",
        reply_markup=MAIN_KB
    )

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

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
            "Выбери режим ⬇️",
            reply_markup=MODE_KB
        )
        return

    if text == "🟡 Деньги от OZON":
        context.user_data.clear()
        context.user_data["mode"] = "mp_money"
        context.user_data["await_report"] = True
        await update.message.reply_text(
            "Ок. Пришли файлом отчёт OZON «Начисления» за нужный период.\n"
            "Формат: .xlsx или .csv",
            reply_markup=MODE_KB
        )
        return

    if text == "🟢 Чистая прибыль":
        context.user_data.clear()
        context.user_data["mode"] = "net_profit"
        context.user_data["await_report"] = True
        await update.message.reply_text(
            "Ок. Пришли файлом отчёт OZON «Начисления» за нужный период.\n"
            "Формат: .xlsx или .csv\n\n"
            "Если себестоимость ещё не загружал — сначала загрузи через «📦 Загрузить себестоимость (SKU → ₽)».",
            reply_markup=MODE_KB
        )
        return

    if text == "📦 Загрузить себестоимость (SKU → ₽)":
        context.user_data.clear()
        context.user_data["await_cogs"] = True
        await update.message.reply_text(
            "Пришли CSV файл себестоимости в формате:\n\n"
            "sku,cogs\n"
            "ABC-123,380\n"
            "XYZ-777,1250\n\n"
            "Разделитель может быть ',' или ';'.",
            reply_markup=BACK_TO_MENU_KB
        )
        return

    # если ждём файл
    if context.user_data.get("await_report"):
        await update.message.reply_text("Жду файл отчёта (.xlsx или .csv). Пришли документом.", reply_markup=MODE_KB)
        return
    if context.user_data.get("await_cogs"):
        await update.message.reply_text("Жду CSV файл себестоимости (sku,cogs).", reply_markup=BACK_TO_MENU_KB)
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

    # --- себестоимость ---
    if context.user_data.get("await_cogs"):
        if suffix != ".csv":
            await update.message.reply_text("Нужен CSV файл (.csv).", reply_markup=BACK_TO_MENU_KB)
            return

        local_path = str(TMP_DIR / f"cogs_{tg_id}_{int(datetime.utcnow().timestamp())}.csv")
        await tg_file.download_to_drive(custom_path=local_path)

        try:
            with open(local_path, "r", encoding="utf-8", errors="ignore") as f:
                sample = f.read(4096)
            delim = ";" if sample.count(";") > sample.count(",") else ","

            count = 0
            with open(local_path, "r", encoding="utf-8", errors="ignore", newline="") as f:
                reader = csv.DictReader(f, delimiter=delim)
                if not reader.fieldnames:
                    raise ValueError("Не вижу заголовков. Нужны sku и cogs.")

                # нормализация заголовков
                fields = {_norm(x): x for x in reader.fieldnames}
                if "sku" not in fields or "cogs" not in fields:
                    raise ValueError("Нужны колонки: sku,cogs")

                sku_key = fields["sku"]
                cogs_key = fields["cogs"]

                for row in reader:
                    sku = (row.get(sku_key) or "").strip()
                    cogs_raw = row.get(cogs_key)
                    if not sku:
                        continue
                    cogs_val = _parse_number(cogs_raw)
                    if cogs_val is None:
                        continue
                    upsert_cogs(tg_id, sku, float(cogs_val))
                    count += 1

            context.user_data.clear()
            await update.message.reply_text(f"✅ Загружено себестоимостей: {count} SKU", reply_markup=MAIN_KB)

        except Exception as e:
            await update.message.reply_text(f"Ошибка CSV: {e}", reply_markup=BACK_TO_MENU_KB)
        return

    # --- отчёт ---
    if context.user_data.get("await_report"):
        if suffix not in (".xlsx", ".xls", ".csv"):
            await update.message.reply_text("Нужен файл .xlsx или .csv", reply_markup=MODE_KB)
            return

        local_path = str(TMP_DIR / f"report_{tg_id}_{int(datetime.utcnow().timestamp())}{suffix}")
        await tg_file.download_to_drive(custom_path=local_path)

        mode = context.user_data.get("mode", "mp_money")

        try:
            parsed = parse_report(local_path)
            revenue = parsed["revenue"]
            deductions = parsed["deductions"]
            net_mp = parsed["total"]

            # 🟡 режим
            if mode == "mp_money":
                status = "🟢" if net_mp > 0 else "🔴"
                msg = (
                    "📈 Итоги по отчёту OZON «Начисления»\n\n"
                    f"Начислено (плюс): {money(revenue)}\n"
                    f"Удержания (минус): {money(deductions)}\n"
                    f"Итого от OZON: {money(net_mp)}\n"
                    f"Статус: {status}\n\n"
                )

                if parsed["by_sku_amount"] is not None and not parsed["by_sku_amount"].empty:
                    msg += "ТОП-5 SKU по итогу:\n" + top_lines(parsed["by_sku_amount"], 5, ascending=False) + "\n\n"
                    msg += "ТОП-5 SKU в минус:\n" + top_lines(parsed["by_sku_amount"], 5, ascending=True) + "\n"
                else:
                    msg += "ТОП SKU: нет (в отчёте не нашёл колонку SKU/offer_id/артикул)\n"

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

            # 🟢 режим
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

            df = parsed["df"].copy()
            df = df[df["_sku"].astype(str).str.len() > 0].copy()

            if df.empty:
                msg = (
                    "🟢 Чистая прибыль\n\n"
                    "В отчёте не нашёл SKU/offer_id/артикул — не могу применить себестоимость.\n"
                    "Проверь, что это отчёт OZON «Начисления», и пришли снова."
                )
                context.user_data.clear()
                await update.message.reply_text(msg, reply_markup=MAIN_KB)
                return

            df["_cogs"] = df["_sku"].map(lambda s: cogs_map.get(s, 0.0)).astype(float)
            df["_cogs_sum"] = df["_cogs"] * df["_qty"]

            cogs_total = float(df["_cogs_sum"].sum())
            net_profit = net_mp - cogs_total
            margin = (net_profit / revenue * 100.0) if revenue > 0 else 0.0

            status = "🔴" if net_profit <= 0 else ("🟡" if margin < 15 else "🟢")

            # прибыль по sku
            amt_by_sku = df.groupby("_sku")["_amount"].sum()
            cogs_by_sku = df.groupby("_sku")["_cogs_sum"].sum()
            profit_by_sku = (amt_by_sku - cogs_by_sku)

            msg = (
                "🟢 Чистая прибыль по отчёту OZON «Начисления»\n\n"
                f"Итого от OZON: {money(net_mp)}\n"
                f"Себестоимость: {money(cogs_total)}\n\n"
                f"Чистая прибыль: {money(net_profit)}\n"
                f"Маржа: {pct(margin)}\n"
                f"Статус: {status}\n\n"
                "ТОП-5 SKU по прибыли:\n"
                f"{top_lines(profit_by_sku, 5, ascending=False)}\n\n"
                "ТОП-5 SKU в минус:\n"
                f"{top_lines(profit_by_sku, 5, ascending=True)}\n"
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
                "Проверь, что это отчёт OZON «Начисления» (.xlsx/.csv) и пришли снова.",
                reply_markup=MODE_KB
            )
        return

    # если файл прислали не в тот момент
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
