import os
import io
import re
import json
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

import pandas as pd
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import CommandStart, Command
from openai import OpenAI


# =========================
# ENV / CONFIG
# =========================
BOT_TOKEN = os.environ["BOT_TOKEN"]
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")

# defaults from env; can be changed via Telegram commands
TAX_RATE_DEFAULT = float(os.getenv("TAX_RATE", "0.06"))      # e.g. 0.06
COST_DEFAULT_ENV = float(os.getenv("COST_PER_UNIT", "4000")) # default cost if SKU cost not set

client = OpenAI(api_key=OPENAI_API_KEY)

# Runtime settings (changeable)
RUNTIME_TAX_RATE = TAX_RATE_DEFAULT
RUNTIME_COST_DEFAULT = COST_DEFAULT_ENV

# Persistent storage
COSTS_PATH = Path("costs.json")      # stores default cost + per-sku costs
OPS_MAP_PATH = Path("ops_map.json")  # stores op->kind mapping ("sale"/"return"/"other")

SKU_COSTS: Dict[str, float] = {}     # sku -> cost
OPS_MAP: Dict[str, str] = {}         # op_value -> "sale" | "return" | "other"


def load_costs():
    global SKU_COSTS, RUNTIME_COST_DEFAULT
    if COSTS_PATH.exists():
        try:
            data = json.loads(COSTS_PATH.read_text(encoding="utf-8"))
            RUNTIME_COST_DEFAULT = float(data.get("default_cost", RUNTIME_COST_DEFAULT))
            raw = data.get("sku_costs", {})
            SKU_COSTS = {str(k): float(v) for k, v in raw.items()}
        except Exception:
            SKU_COSTS = {}


def save_costs():
    payload = {"default_cost": RUNTIME_COST_DEFAULT, "sku_costs": SKU_COSTS}
    COSTS_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def load_ops_map():
    global OPS_MAP
    if OPS_MAP_PATH.exists():
        try:
            OPS_MAP = json.loads(OPS_MAP_PATH.read_text(encoding="utf-8"))
        except Exception:
            OPS_MAP = {}


def save_ops_map():
    OPS_MAP_PATH.write_text(json.dumps(OPS_MAP, ensure_ascii=False, indent=2), encoding="utf-8")


# =========================
# Column mapping
# =========================
def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("ё", "е")
    return s


def _find_col_fuzzy(columns, needles):
    cols = list(columns)
    ncols = [_norm(c) for c in cols]
    for nd in needles:
        ndn = _norm(nd)
        for c, cn in zip(cols, ncols):
            if ndn in cn:
                return c
    return None


@dataclass
class ColMap:
    sku: str
    amount: str
    qty: Optional[str] = None
    op: Optional[str] = None


def guess_columns_locally(df: pd.DataFrame) -> Optional[ColMap]:
    sku = _find_col_fuzzy(df.columns, ["sku", "артикул", "offer id", "id товара", "код товара"])
    amount = _find_col_fuzzy(df.columns, ["сумма итого", "итого, руб", "сумма итого, руб", "сумма, руб"])
    qty = _find_col_fuzzy(df.columns, ["количество", "шт"])
    op = _find_col_fuzzy(df.columns, ["операция", "тип начисления", "тип операции", "событие", "начисление"])
    if sku and amount:
        return ColMap(sku=sku, amount=amount, qty=qty, op=op)
    return None


async def guess_columns_with_openai(columns: List[str], sample_rows: List[dict]) -> Optional[ColMap]:
    schema = {
        "name": "ozon_accruals_column_map",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "sku": {"type": "string"},
                "amount": {"type": "string"},
                "qty": {"type": ["string", "null"]},
                "op": {"type": ["string", "null"]},
            },
            "required": ["sku", "amount", "qty", "op"],
        },
        "strict": True,
    }

    prompt = f"""
Ты помогаешь парсить XLSX отчет OZON Seller "Начисления".
Нужно вернуть JSON с точными названиями колонок:
- sku: колонка SKU/артикул/offer_id/код товара
- amount: колонка "Сумма итого, руб." (это деньги продавцу по строке)
- qty: колонка количества (если есть)
- op: колонка типа операции/начисления (если есть)
Если qty или op нет — верни null.

Колонки: {columns}
Примеры строк: {sample_rows}

Верни только JSON по схеме.
""".strip()

    try:
        resp = client.responses.create(
            model=OPENAI_MODEL,
            input=prompt,
            text={"format": {"type": "json_schema", "json_schema": schema}},
        )
        data = resp.output[0].content[0].parsed
        if data and data.get("sku") and data.get("amount"):
            return ColMap(sku=data["sku"], amount=data["amount"], qty=data.get("qty"), op=data.get("op"))
    except Exception:
        return None

    return None


# =========================
# OpenAI op classification (context-based)
# =========================
async def classify_operations_with_openai(op_samples: list[dict]) -> dict[str, str]:
    """
    op_samples: [
      {"op": "Название операции", "examples": [{"sku": "...", "amount": 123.45}, ...]},
      ...
    ]
    Returns mapping: op -> 'sale' | 'return' | 'other'
    """
    schema = {
        "name": "ozon_ops_classifier",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "mapping": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "op": {"type": "string"},
                            "kind": {"type": "string", "enum": ["sale", "return", "other"]},
                            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                        },
                        "required": ["op", "kind", "confidence"],
                    },
                }
            },
            "required": ["mapping"],
        },
        "strict": True,
    }

    prompt = f"""
Ты — бухгалтер/аналитик по OZON.
Даны операции из отчёта "Начисления" и примеры строк по каждой операции (SKU и "Сумма итого, руб.").

Определи для каждой операции:
- sale: продажа/реализация товара покупателю
- return: возврат/отмена/аннулирование продажи
- other: логистика, комиссия, услуги, штрафы, корректировки, продвижение и т.д.

Оценивай по смыслу и примерам начислений, не по ключевым словам.
Верни только JSON по схеме.

Данные:
{op_samples}
""".strip()

    resp = client.responses.create(
        model=OPENAI_MODEL,
        input=prompt,
        text={"format": {"type": "json_schema", "json_schema": schema}},
    )
    parsed = resp.output[0].content[0].parsed
    out = {}
    for item in parsed["mapping"]:
        out[str(item["op"])] = item["kind"]
    return out


# =========================
# Core calc (video method)
# =========================
def _to_number_series(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str)
         .str.replace("\u00a0", "", regex=False)
         .str.replace(" ", "", regex=False)
         .str.replace(",", ".", regex=False),
        errors="coerce"
    ).fillna(0.0)


def compute_video_method(
    df: pd.DataFrame,
    colmap: ColMap,
    default_cost_per_unit: float,
    sku_costs: Dict[str, float],
    tax_rate: float
) -> Dict[str, Any]:
    work = df.copy()
    work = work.dropna(how="all")

    # normalize key fields
    work[colmap.sku] = work[colmap.sku].fillna("").astype(str).str.strip()
    work[colmap.amount] = _to_number_series(work[colmap.amount])

    # revenue = sum(amount)
    revenue_total = float(work[colmap.amount].sum())
    tax_total = max(revenue_total, 0.0) * tax_rate  # video method: tax from revenue (accruals)

    # qty per row:
    # 1) if qty exists -> use it AND try to sign it by op mapping if op exists
    # 2) else if op exists -> sale=+1, return=-1, other=0 using OPS_MAP
    # 3) else -> qty=0 (cannot compute COGS)
    work["_row_qty"] = 0.0

    has_qty = bool(colmap.qty and colmap.qty in work.columns)
    has_op = bool(colmap.op and colmap.op in work.columns)

    if has_qty:
        work[colmap.qty] = _to_number_series(work[colmap.qty])

        if has_op:
            def _signed_qty(row) -> float:
                opv = str(row[colmap.op]).strip()
                kind = OPS_MAP.get(opv, "other")
                q = float(row[colmap.qty])
                if kind == "sale":
                    return abs(q) if q != 0 else 1.0
                if kind == "return":
                    return -abs(q) if q != 0 else -1.0
                return 0.0

            work["_row_qty"] = work.apply(_signed_qty, axis=1)
        else:
            work["_row_qty"] = work[colmap.qty]

    elif has_op:
        def _qty_from_op(opv: str) -> float:
            kind = OPS_MAP.get(str(opv).strip(), "other")
            if kind == "sale":
                return 1.0
            if kind == "return":
                return -1.0
            return 0.0

        work["_row_qty"] = work[colmap.op].apply(_qty_from_op).astype(float)

    else:
        work["_row_qty"] = 0.0

    # sold qty used for COGS = only sales rows (positive qty)
    sku_qty_sales = (
        work.loc[work["_row_qty"] > 0]
            .groupby(colmap.sku)["_row_qty"]
            .sum()
            .to_dict()
    )
    sold_qty_total = float(sum(sku_qty_sales.values()))

    # Per SKU revenue
    sku_rev = work.groupby(colmap.sku, dropna=False)[colmap.amount].sum().to_dict()

    def cost_for_sku(sku: str) -> float:
        return float(sku_costs.get(str(sku), default_cost_per_unit))

    # COGS per SKU (by sales count only)
    sku_cogs = {}
    cogs_total = 0.0
    for sku in sku_rev.keys():
        qty = float(sku_qty_sales.get(sku, 0.0))
        c = qty * cost_for_sku(sku)
        sku_cogs[sku] = c
        cogs_total += c

    # Allocate tax proportionally to positive revenue SKUs
    pos_rev_sum = sum(v for v in sku_rev.values() if float(v) > 0)
    sku_tax = {}
    for sku, rev in sku_rev.items():
        rev = float(rev)
        if rev > 0 and pos_rev_sum > 0:
            sku_tax[sku] = tax_total * (rev / pos_rev_sum)
        else:
            sku_tax[sku] = 0.0

    # Profit per SKU
    sku_profit = {}
    for sku in sku_rev.keys():
        sku_profit[sku] = float(sku_rev[sku]) - float(sku_cogs.get(sku, 0.0)) - float(sku_tax.get(sku, 0.0))

    profit_total = revenue_total - cogs_total - tax_total

    def r2(x): return float(round(float(x), 2))

    result = {
        "revenue_total": r2(revenue_total),
        "tax_total": r2(tax_total),
        "cogs_total": r2(cogs_total),
        "profit_total": r2(profit_total),
        "sold_qty_total": r2(sold_qty_total),
        "warning_no_qty": not (has_qty or has_op),
        "per_sku": []
    }

    for sku, rev in sorted(sku_rev.items(), key=lambda x: float(x[1]), reverse=True):
        sku_str = str(sku).strip()
        result["per_sku"].append({
            "sku": sku_str,
            "revenue": r2(rev),
            "sold_qty": r2(sku_qty_sales.get(sku_str, sku_qty_sales.get(sku, 0.0))),
            "cogs": r2(sku_cogs.get(sku_str, sku_cogs.get(sku, 0.0))),
            "tax": r2(sku_tax.get(sku_str, sku_tax.get(sku, 0.0))),
            "profit": r2(sku_profit.get(sku_str, sku_profit.get(sku, 0.0))),
        })

    return result


def format_answer(res: Dict[str, Any]) -> str:
    lines = []
    lines.append(f"1) Общая выручка (на р/с): {res['revenue_total']:.2f} ₽")
    lines.append(f"2) Общая чистая прибыль: {res['profit_total']:.2f} ₽")
    lines.append("3) По SKU:")
    for item in res["per_sku"]:
        lines.append(f"- {item['sku']}: выручка {item['revenue']:.2f} ₽, чистая прибыль {item['profit']:.2f} ₽")

    if res.get("warning_no_qty"):
        lines.append("")
        lines.append("⚠️ Не найдена колонка количества и колонка операции. Себестоимость не посчиталась.")
        lines.append("Пришли именно XLSX отчет OZON «Начисления» (полный).")

    return "\n".join(lines)


# =========================
# Telegram commands: costs
# =========================
def _parse_money(x: str) -> float:
    return float(str(x).replace(" ", "").replace("\u00a0", "").replace(",", "."))


async def cmd_costdefault(msg: Message):
    global RUNTIME_COST_DEFAULT
    parts = msg.text.strip().split()

    if len(parts) == 1:
        await msg.answer(
            f"Себестоимость по умолчанию: {RUNTIME_COST_DEFAULT:.2f} ₽\n"
            f"Установить: /costdefault 4000"
        )
        return

    try:
        val = _parse_money(parts[1])
        if val <= 0 or val > 10_000_000:
            await msg.answer("Некорректное значение. Пример: /costdefault 4000")
            return
        RUNTIME_COST_DEFAULT = val
        save_costs()
        await msg.answer(f"Готово. Себестоимость по умолчанию: {RUNTIME_COST_DEFAULT:.2f} ₽")
    except Exception:
        await msg.answer("Не понял число. Пример: /costdefault 4000")


async def cmd_costsku(msg: Message):
    parts = msg.text.strip().split()

    # /costsku -> list
    if len(parts) == 1:
        if not SKU_COSTS:
            await msg.answer(
                "Себестоимости по SKU не заданы.\n"
                "Задать: /costsku 2796688793 4200\n"
                f"По умолчанию: {RUNTIME_COST_DEFAULT:.2f} ₽ (/costdefault)"
            )
            return
        lines = ["Себестоимость по SKU:"]
        for k, v in sorted(SKU_COSTS.items(), key=lambda x: x[0]):
            lines.append(f"- {k}: {v:.2f} ₽")
        await msg.answer("\n".join(lines))
        return

    # /costsku <sku> -> show one
    if len(parts) == 2:
        sku = str(parts[1]).strip()
        val = SKU_COSTS.get(sku)
        if val is None:
            await msg.answer(
                f"Для SKU {sku} себестоимость не задана.\n"
                f"Будет использовано default: {RUNTIME_COST_DEFAULT:.2f} ₽\n"
                f"Задать: /costsku {sku} 4200"
            )
        else:
            await msg.answer(f"SKU {sku}: себестоимость {val:.2f} ₽")
        return

    # /costsku <sku> <cost> -> set
    try:
        sku = str(parts[1]).strip()
        val = _parse_money(parts[2])
        if val <= 0 or val > 10_000_000:
            await msg.answer("Некорректно. Пример: /costsku 2796688793 4200")
            return
        SKU_COSTS[sku] = val
        save_costs()
        await msg.answer(f"Готово. SKU {sku}: себестоимость {val:.2f} ₽")
    except Exception:
        await msg.answer("Формат: /costsku <SKU> <себестоимость>. Пример: /costsku 2796688793 4200")


async def cmd_costsku_del(msg: Message):
    parts = msg.text.strip().split()
    if len(parts) != 2:
        await msg.answer("Формат: /costsku_del <SKU>. Пример: /costsku_del 2796688793")
        return

    sku = str(parts[1]).strip()
    if sku in SKU_COSTS:
        SKU_COSTS.pop(sku, None)
        save_costs()
        await msg.answer(
            f"Удалил себестоимость SKU {sku}. Теперь используется default: {RUNTIME_COST_DEFAULT:.2f} ₽"
        )
    else:
        await msg.answer(f"Для SKU {sku} себестоимость не была задана.")


# =========================
# Telegram handlers
# =========================
async def start(msg: Message):
    await msg.answer(
        "Скинь XLSX отчет OZON «Начисления».\n"
        "Посчитаю по методике из видео:\n"
        "1) выручка (на р/с)\n"
        "2) чистая прибыль\n"
        "3) по каждому SKU (выручка и чистая прибыль)\n\n"
        "Себестоимость:\n"
        "- /costdefault 4000\n"
        "- /costsku <SKU> <COST>"
    )


async def read_xlsx_from_bytes(b: bytes) -> pd.DataFrame:
    return pd.read_excel(io.BytesIO(b), engine="openpyxl")


async def handle_xlsx_bytes(file_bytes: bytes) -> Tuple[Optional[str], Optional[str]]:
    try:
        df = await read_xlsx_from_bytes(file_bytes)
        df = df.dropna(how="all")

        # 1) local guess
        colmap = guess_columns_locally(df)

        # 2) fallback to OpenAI
        if colmap is None:
            cols = list(map(str, df.columns.tolist()))
            sample = df.head(10).fillna("").to_dict(orient="records")
            colmap = await guess_columns_with_openai(cols, sample)

        if colmap is None:
            return None, "Не смог распознать колонки (SKU / Сумма итого). Проверь, что это XLSX отчет 'Начисления'."

        # ---- Context-based operation mapping (sale/return/other) ----
        if colmap.op and colmap.op in df.columns:
            tmp = df.copy()
            tmp[colmap.amount] = _to_number_series(tmp[colmap.amount])
            tmp[colmap.op] = tmp[colmap.op].fillna("").astype(str).str.strip()
            tmp[colmap.sku] = tmp[colmap.sku].fillna("").astype(str).str.strip()

            unique_ops = tmp[colmap.op].loc[lambda s: s != ""].unique().tolist()
            missing = [op for op in unique_ops if op not in OPS_MAP]

            if missing:
                MAX_EXAMPLES_PER_OP = 4
                MAX_OPS_PER_CALL = 40

                op_samples_all = []
                for op in missing:
                    sub = tmp[tmp[colmap.op] == op][[colmap.sku, colmap.amount]].copy()

                    # prefer non-zero examples
                    sub_nz = sub[sub[colmap.amount] != 0].head(MAX_EXAMPLES_PER_OP)
                    if len(sub_nz) < MAX_EXAMPLES_PER_OP:
                        sub_any = sub.head(MAX_EXAMPLES_PER_OP - len(sub_nz))
                        sub_use = pd.concat([sub_nz, sub_any], ignore_index=True)
                    else:
                        sub_use = sub_nz

                    examples = []
                    for _, row in sub_use.iterrows():
                        examples.append({"sku": str(row[colmap.sku]), "amount": float(row[colmap.amount])})

                    op_samples_all.append({"op": str(op), "examples": examples})

                for i in range(0, len(op_samples_all), MAX_OPS_PER_CALL):
                    chunk = op_samples_all[i:i + MAX_OPS_PER_CALL]
                    new_map = await classify_operations_with_openai(chunk)
                    OPS_MAP.update(new_map)

                save_ops_map()

        # ---- Compute ----
        res = compute_video_method(
            df,
            colmap,
            default_cost_per_unit=RUNTIME_COST_DEFAULT,
            sku_costs=SKU_COSTS,
            tax_rate=RUNTIME_TAX_RATE
        )

        return format_answer(res), None

    except Exception as e:
        return None, f"Ошибка чтения/расчета: {e}"


async def on_document(msg: Message, bot: Bot):
    doc = msg.document
    if not doc:
        return

    if not doc.file_name.lower().endswith(".xlsx"):
        await msg.answer("Пришли файл в формате .xlsx (CSV не принимаю).")
        return

    tg_file = await bot.get_file(doc.file_id)
    buf = await bot.download_file(tg_file.file_path)
    file_bytes = buf.read()

    answer, error = await handle_xlsx_bytes(file_bytes)
    if error:
        await msg.answer(error)
    else:
        await msg.answer(answer)


# =========================
# App entry
# =========================
async def main():
    load_costs()
    load_ops_map()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    dp.message.register(start, CommandStart())
    dp.message.register(cmd_costdefault, Command("costdefault"))
    dp.message.register(cmd_costsku, Command("costsku"))
    dp.message.register(cmd_costsku_del, Command("costsku_del"))
    dp.message.register(on_document, F.document)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
