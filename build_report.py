"""
T-Invest Analytics Report Builder
Fetches data from all accounts via T-Invest API and creates an Excel report
with raw data sheets, pivot tables, charts, and formatting.
"""
import os
import sys
import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict

import requests
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, numbers
from openpyxl.chart import PieChart, BarChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl.chart.series import DataPoint
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.table import Table, TableStyleInfo

load_dotenv()

TOKEN = os.getenv("TINKOFF_TOKEN")
BASE_URL = "https://invest-public-api.tinkoff.ru/rest"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# ── Styles ──────────────────────────────────────────────────────────────
HEADER_FONT = Font(bold=True, color="FFFFFF", size=11)
HEADER_FILL = PatternFill(start_color="2B579A", end_color="2B579A", fill_type="solid")
HEADER_FILL_GREEN = PatternFill(start_color="217346", end_color="217346", fill_type="solid")
HEADER_FILL_ORANGE = PatternFill(start_color="C55A11", end_color="C55A11", fill_type="solid")
GREEN_FILL = PatternFill(start_color="E2EFDA", end_color="E2EFDA", fill_type="solid")
RED_FILL = PatternFill(start_color="FCE4EC", end_color="FCE4EC", fill_type="solid")
THIN_BORDER = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"), bottom=Side(style="thin"),
)
RUB_FMT = '#,##0.00 "₽"'
PCT_FMT = '0.00%'
DATE_FMT = 'DD.MM.YYYY'
DATETIME_FMT = 'DD.MM.YYYY HH:MM'


def api_call(service, method, body=None):
    """Make a REST API call to T-Invest."""
    url = f"{BASE_URL}/tinkoff.public.invest.api.contract.v1.{service}/{method}"
    resp = requests.post(url, headers=HEADERS, json=body or {})
    if resp.status_code != 200:
        print(f"API Error {resp.status_code} on {service}/{method}: {resp.text[:200]}")
        return None
    return resp.json()


def money_value(m):
    """Convert MoneyValue proto to float."""
    if not m:
        return 0.0
    units = int(m.get("units", 0))
    nano = int(m.get("nano", 0))
    return units + nano / 1_000_000_000


def quotation_value(q):
    """Convert Quotation proto to float."""
    if not q:
        return 0.0
    units = int(q.get("units", 0))
    nano = int(q.get("nano", 0))
    return units + nano / 1_000_000_000


def parse_ts(ts_str):
    """Parse ISO timestamp string to naive datetime (Excel-compatible)."""
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return dt.replace(tzinfo=None)  # Excel doesn't support tz
    except:
        return None


# ── Data Fetching ───────────────────────────────────────────────────────

def get_accounts():
    data = api_call("UsersService", "GetAccounts")
    if not data:
        return []
    return [a for a in data.get("accounts", []) if a.get("status") == "ACCOUNT_STATUS_OPEN"]


def get_portfolio(account_id):
    data = api_call("OperationsService", "GetPortfolio", {"accountId": account_id, "currency": "RUB"})
    return data


def get_operations(account_id, from_date, to_date):
    """Fetch operations in chunks (API limit ~1 year per request)."""
    all_ops = []
    cursor = ""
    while True:
        body = {
            "accountId": account_id,
            "from": from_date.isoformat(),
            "to": to_date.isoformat(),
            "state": "OPERATION_STATE_EXECUTED",
            "limit": 1000,
            "operationTypes": [],
            "withoutCommissions": False,
            "withoutTrades": False,
            "withoutOvernights": True,
        }
        if cursor:
            body["cursor"] = cursor
        data = api_call("OperationsService", "GetOperationsByCursor", body)
        if not data:
            break
        items = data.get("items", [])
        all_ops.extend(items)
        if not data.get("hasNext", False):
            break
        cursor = data.get("nextCursor", "")
        if not cursor:
            break
    return all_ops


def get_instrument_by_uid(uid):
    """Get instrument info by its UID."""
    data = api_call("InstrumentsService", "GetInstrumentBy", {
        "idType": "INSTRUMENT_ID_TYPE_UID",
        "id": uid,
    })
    return data.get("instrument") if data else None


# ── Excel Building ──────────────────────────────────────────────────────

def style_header_row(ws, row, num_cols, fill=HEADER_FILL):
    for col in range(1, num_cols + 1):
        cell = ws.cell(row=row, column=col)
        cell.font = HEADER_FONT
        cell.fill = fill
        cell.alignment = Alignment(horizontal="center", wrap_text=True)
        cell.border = THIN_BORDER


def auto_width(ws, min_width=10, max_width=35):
    for col in ws.columns:
        col_letter = get_column_letter(col[0].column)
        max_len = min_width
        for cell in col:
            if cell.value:
                max_len = max(max_len, min(len(str(cell.value)) + 2, max_width))
        ws.column_dimensions[col_letter].width = max_len


def add_table(ws, ref, name):
    """Add an Excel Table to the worksheet."""
    safe_name = name.replace(" ", "_").replace("-", "_")
    table = Table(displayName=safe_name, ref=ref)
    style = TableStyleInfo(
        name="TableStyleMedium9", showFirstColumn=False,
        showLastColumn=False, showRowStripes=True, showColumnStripes=False,
    )
    table.tableStyleInfo = style
    ws.add_table(table)


def build_portfolio_sheet(wb, all_positions, instruments_cache):
    """Sheet: Portfolio — all current positions across all accounts."""
    ws = wb.create_sheet("Портфель")
    headers = [
        "Счёт", "Тикер", "Название", "Тип", "Сектор", "Валюта",
        "Кол-во", "Средняя цена", "Текущая цена",
        "Стоимость", "P&L ₽", "P&L %", "Доля в портфеле %",
    ]
    ws.append(headers)
    style_header_row(ws, 1, len(headers))

    total_value = sum(p.get("total_value", 0) for p in all_positions)

    for i, pos in enumerate(all_positions, start=2):
        qty = pos.get("quantity", 0)
        avg_price = pos.get("avg_price", 0)
        cur_price = pos.get("cur_price", 0)
        value = pos.get("total_value", 0)
        pnl = pos.get("pnl", 0)
        pnl_pct = pos.get("pnl_pct", 0)
        share = value / total_value if total_value else 0

        ws.append([
            pos.get("account_name", ""),
            pos.get("ticker", ""),
            pos.get("name", ""),
            pos.get("instrument_type", ""),
            pos.get("sector", ""),
            pos.get("currency", ""),
            qty,
            avg_price,
            cur_price,
            value,
            pnl,
            pnl_pct,
            share,
        ])

        # Conditional fill for P&L
        pnl_cell = ws.cell(row=i, column=11)
        pnl_cell.fill = GREEN_FILL if pnl >= 0 else RED_FILL
        pnl_pct_cell = ws.cell(row=i, column=12)
        pnl_pct_cell.fill = GREEN_FILL if pnl_pct >= 0 else RED_FILL

    # Formatting
    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.border = THIN_BORDER
        row[7].number_format = RUB_FMT   # avg price
        row[8].number_format = RUB_FMT   # cur price
        row[9].number_format = RUB_FMT   # value
        row[10].number_format = RUB_FMT  # pnl
        row[11].number_format = PCT_FMT  # pnl %
        row[12].number_format = PCT_FMT  # share %

    if ws.max_row > 1:
        add_table(ws, f"A1:{get_column_letter(len(headers))}{ws.max_row}", "Portfolio")

    auto_width(ws)
    ws.freeze_panes = "A2"
    return ws


def build_operations_sheet(wb, all_operations):
    """Sheet: Operations — all executed operations."""
    ws = wb.create_sheet("Операции")
    headers = [
        "Дата", "Счёт", "Тикер", "Название", "Тип операции",
        "Кол-во", "Цена", "Сумма", "Валюта", "Комиссия",
    ]
    ws.append(headers)
    style_header_row(ws, 1, len(headers))

    for op in sorted(all_operations, key=lambda x: x.get("date") or "", reverse=True):
        ws.append([
            op.get("date"),
            op.get("account_name", ""),
            op.get("ticker", ""),
            op.get("name", ""),
            op.get("type_display", ""),
            op.get("quantity", ""),
            op.get("price", ""),
            op.get("payment", 0),
            op.get("currency", ""),
            op.get("commission", 0),
        ])

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.border = THIN_BORDER
        if row[0].value:
            row[0].number_format = DATETIME_FMT
        row[6].number_format = RUB_FMT
        row[7].number_format = RUB_FMT
        row[9].number_format = RUB_FMT

    if ws.max_row > 1:
        add_table(ws, f"A1:{get_column_letter(len(headers))}{ws.max_row}", "Operations")

    auto_width(ws)
    ws.freeze_panes = "A2"
    return ws


def build_dividends_sheet(wb, all_operations):
    """Sheet: Dividends & Coupons."""
    ws = wb.create_sheet("Дивиденды и купоны")
    headers = ["Дата", "Счёт", "Тикер", "Название", "Тип", "Сумма", "Валюта"]
    ws.append(headers)
    style_header_row(ws, 1, len(headers), HEADER_FILL_GREEN)

    div_ops = [op for op in all_operations if op.get("is_income")]
    for op in sorted(div_ops, key=lambda x: x.get("date") or "", reverse=True):
        ws.append([
            op.get("date"),
            op.get("account_name", ""),
            op.get("ticker", ""),
            op.get("name", ""),
            op.get("type_display", ""),
            op.get("payment", 0),
            op.get("currency", ""),
        ])

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.border = THIN_BORDER
        if row[0].value:
            row[0].number_format = DATETIME_FMT
        row[5].number_format = RUB_FMT

    if ws.max_row > 1:
        add_table(ws, f"A1:{get_column_letter(len(headers))}{ws.max_row}", "Dividends")

    auto_width(ws)
    ws.freeze_panes = "A2"
    return ws


def build_commissions_sheet(wb, all_operations):
    """Sheet: Commissions summary."""
    ws = wb.create_sheet("Комиссии")
    headers = ["Месяц", "Счёт", "Комиссия"]
    ws.append(headers)
    style_header_row(ws, 1, len(headers), HEADER_FILL_ORANGE)

    # Aggregate commissions by month and account
    comm_by_month = defaultdict(float)
    for op in all_operations:
        c = abs(op.get("commission", 0))
        if c > 0 and op.get("date"):
            d = op["date"]
            key = (d.strftime("%Y-%m") if isinstance(d, datetime) else str(d)[:7], op.get("account_name", ""))
            comm_by_month[key] += c

    for (month, account), total in sorted(comm_by_month.items()):
        ws.append([month, account, total])

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.border = THIN_BORDER
        row[2].number_format = RUB_FMT

    if ws.max_row > 1:
        add_table(ws, f"A1:{get_column_letter(len(headers))}{ws.max_row}", "Commissions")

    auto_width(ws)
    return ws


def build_summary_sheet(wb, all_positions, all_operations):
    """Sheet: Summary dashboard with charts."""
    ws = wb.create_sheet("Сводка")

    # ── Section 1: Portfolio by type ──
    ws["A1"] = "Портфель по типу актива"
    ws["A1"].font = Font(bold=True, size=14)

    type_totals = defaultdict(float)
    for p in all_positions:
        type_totals[p.get("instrument_type", "Другое")] += p.get("total_value", 0)

    ws["A3"] = "Тип актива"
    ws["B3"] = "Стоимость"
    ws["C3"] = "Доля"
    style_header_row(ws, 3, 3)

    total_val = sum(type_totals.values())
    row = 4
    for t, v in sorted(type_totals.items(), key=lambda x: -x[1]):
        ws.cell(row=row, column=1, value=t)
        ws.cell(row=row, column=2, value=v).number_format = RUB_FMT
        ws.cell(row=row, column=3, value=v / total_val if total_val else 0).number_format = PCT_FMT
        row += 1

    # Pie chart: by type
    if len(type_totals) > 0:
        pie = PieChart()
        pie.title = "Портфель по типу актива"
        pie.style = 10
        data = Reference(ws, min_col=2, min_row=3, max_row=row - 1)
        cats = Reference(ws, min_col=1, min_row=4, max_row=row - 1)
        pie.add_data(data, titles_from_data=True)
        pie.set_categories(cats)
        pie.width = 18
        pie.height = 12
        dl = DataLabelList()
        dl.showPercent = True
        pie.dataLabels = dl
        ws.add_chart(pie, "E3")

    # ── Section 2: Portfolio by sector ──
    sector_start = row + 2
    ws.cell(row=sector_start, column=1, value="Портфель по секторам").font = Font(bold=True, size=14)

    sector_totals = defaultdict(float)
    for p in all_positions:
        sector_totals[p.get("sector", "Другое") or "Другое"] += p.get("total_value", 0)

    ws.cell(row=sector_start + 2, column=1, value="Сектор")
    ws.cell(row=sector_start + 2, column=2, value="Стоимость")
    ws.cell(row=sector_start + 2, column=3, value="Доля")
    style_header_row(ws, sector_start + 2, 3)

    row = sector_start + 3
    for s, v in sorted(sector_totals.items(), key=lambda x: -x[1]):
        ws.cell(row=row, column=1, value=s)
        ws.cell(row=row, column=2, value=v).number_format = RUB_FMT
        ws.cell(row=row, column=3, value=v / total_val if total_val else 0).number_format = PCT_FMT
        row += 1

    # Pie chart: by sector
    if len(sector_totals) > 0:
        pie2 = PieChart()
        pie2.title = "Портфель по секторам"
        pie2.style = 10
        data = Reference(ws, min_col=2, min_row=sector_start + 2, max_row=row - 1)
        cats = Reference(ws, min_col=1, min_row=sector_start + 3, max_row=row - 1)
        pie2.add_data(data, titles_from_data=True)
        pie2.set_categories(cats)
        pie2.width = 18
        pie2.height = 12
        dl2 = DataLabelList()
        dl2.showPercent = True
        pie2.dataLabels = dl2
        ws.add_chart(pie2, f"E{sector_start}")

    # ── Section 3: P&L by account ──
    pnl_start = row + 2
    ws.cell(row=pnl_start, column=1, value="P&L по счетам").font = Font(bold=True, size=14)

    account_pnl = defaultdict(float)
    account_value = defaultdict(float)
    for p in all_positions:
        account_pnl[p.get("account_name", "")] += p.get("pnl", 0)
        account_value[p.get("account_name", "")] += p.get("total_value", 0)

    ws.cell(row=pnl_start + 2, column=1, value="Счёт")
    ws.cell(row=pnl_start + 2, column=2, value="Стоимость")
    ws.cell(row=pnl_start + 2, column=3, value="P&L")
    style_header_row(ws, pnl_start + 2, 3)

    row = pnl_start + 3
    for acc in sorted(account_value.keys()):
        ws.cell(row=row, column=1, value=acc)
        ws.cell(row=row, column=2, value=account_value[acc]).number_format = RUB_FMT
        pnl_cell = ws.cell(row=row, column=3, value=account_pnl[acc])
        pnl_cell.number_format = RUB_FMT
        pnl_cell.fill = GREEN_FILL if account_pnl[acc] >= 0 else RED_FILL
        row += 1

    # ── Section 4: Income by month (dividends + coupons) ──
    inc_start = row + 2
    ws.cell(row=inc_start, column=1, value="Доходы по месяцам (дивиденды + купоны)").font = Font(bold=True, size=14)

    income_by_month = defaultdict(float)
    for op in all_operations:
        if op.get("is_income") and op.get("date"):
            d = op["date"]
            month = d.strftime("%Y-%m") if isinstance(d, datetime) else str(d)[:7]
            income_by_month[month] += op.get("payment", 0)

    ws.cell(row=inc_start + 2, column=1, value="Месяц")
    ws.cell(row=inc_start + 2, column=2, value="Доход")
    style_header_row(ws, inc_start + 2, 2, HEADER_FILL_GREEN)

    row = inc_start + 3
    for month, total in sorted(income_by_month.items()):
        ws.cell(row=row, column=1, value=month)
        ws.cell(row=row, column=2, value=total).number_format = RUB_FMT
        row += 1

    # Bar chart: income by month
    if len(income_by_month) > 1:
        bar = BarChart()
        bar.type = "col"
        bar.title = "Дивиденды и купоны по месяцам"
        bar.style = 10
        data = Reference(ws, min_col=2, min_row=inc_start + 2, max_row=row - 1)
        cats = Reference(ws, min_col=1, min_row=inc_start + 3, max_row=row - 1)
        bar.add_data(data, titles_from_data=True)
        bar.set_categories(cats)
        bar.width = 28
        bar.height = 14
        bar.shape = 4
        ws.add_chart(bar, f"D{inc_start}")

    # ── Section 5: Top P&L positions ──
    top_start = row + 2
    ws.cell(row=top_start, column=1, value="Топ-10 позиций по P&L").font = Font(bold=True, size=14)

    ws.cell(row=top_start + 2, column=1, value="Тикер")
    ws.cell(row=top_start + 2, column=2, value="Название")
    ws.cell(row=top_start + 2, column=3, value="P&L")
    ws.cell(row=top_start + 2, column=4, value="P&L %")
    style_header_row(ws, top_start + 2, 4)

    sorted_positions = sorted(all_positions, key=lambda x: abs(x.get("pnl", 0)), reverse=True)[:10]
    row = top_start + 3
    for p in sorted_positions:
        ws.cell(row=row, column=1, value=p.get("ticker", ""))
        ws.cell(row=row, column=2, value=p.get("name", ""))
        pnl_cell = ws.cell(row=row, column=3, value=p.get("pnl", 0))
        pnl_cell.number_format = RUB_FMT
        pnl_cell.fill = GREEN_FILL if p.get("pnl", 0) >= 0 else RED_FILL
        ws.cell(row=row, column=4, value=p.get("pnl_pct", 0)).number_format = PCT_FMT
        row += 1

    auto_width(ws)
    return ws


def build_account_summary_sheet(wb, all_positions, all_operations):
    """Sheet: Per-account summary."""
    ws = wb.create_sheet("По счетам")
    headers = [
        "Счёт", "Тип", "Стоимость портфеля", "P&L",
        "Дивиденды получено", "Купоны получено", "Комиссии уплачено",
    ]
    ws.append(headers)
    style_header_row(ws, 1, len(headers))

    account_data = {}
    for p in all_positions:
        acc = p.get("account_name", "")
        if acc not in account_data:
            account_data[acc] = {
                "type": p.get("account_type_display", ""),
                "value": 0, "pnl": 0, "divs": 0, "coupons": 0, "commissions": 0,
            }
        account_data[acc]["value"] += p.get("total_value", 0)
        account_data[acc]["pnl"] += p.get("pnl", 0)

    for op in all_operations:
        acc = op.get("account_name", "")
        if acc not in account_data:
            continue
        if op.get("type_display") == "Дивиденды":
            account_data[acc]["divs"] += op.get("payment", 0)
        elif op.get("type_display") == "Купоны":
            account_data[acc]["coupons"] += op.get("payment", 0)
        account_data[acc]["commissions"] += abs(op.get("commission", 0))

    for acc, d in sorted(account_data.items()):
        ws.append([
            acc, d["type"], d["value"], d["pnl"],
            d["divs"], d["coupons"], d["commissions"],
        ])

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
        for cell in row:
            cell.border = THIN_BORDER
        for col in range(2, 7):
            row[col].number_format = RUB_FMT

    auto_width(ws)
    return ws


# ── Operation type mapping ──────────────────────────────────────────────

OPERATION_TYPES = {
    "OPERATION_TYPE_BUY": "Покупка",
    "OPERATION_TYPE_SELL": "Продажа",
    "OPERATION_TYPE_COUPON": "Купоны",
    "OPERATION_TYPE_DIVIDEND": "Дивиденды",
    "OPERATION_TYPE_DIVIDEND_TAX": "Налог на дивиденды",
    "OPERATION_TYPE_BROKER_FEE": "Комиссия брокера",
    "OPERATION_TYPE_TAX": "Налог",
    "OPERATION_TYPE_BOND_REPAYMENT": "Погашение облигации",
    "OPERATION_TYPE_BOND_REPAYMENT_FULL": "Погашение облигации",
    "OPERATION_TYPE_INPUT": "Пополнение",
    "OPERATION_TYPE_OUTPUT": "Вывод",
    "OPERATION_TYPE_OVERNIGHT": "Овернайт",
    "OPERATION_TYPE_ACCRUING_VARMARGIN": "Вариационная маржа",
    "OPERATION_TYPE_WRITING_OFF_VARMARGIN": "Списание вариационной маржи",
    "OPERATION_TYPE_TAX_CORRECTION": "Корректировка налога",
    "OPERATION_TYPE_BENEFIT_TAX": "Налог на доход",
    "OPERATION_TYPE_SERVICE_FEE": "Сервисная комиссия",
    "OPERATION_TYPE_INP_MULTI": "Пополнение (мульти)",
    "OPERATION_TYPE_OUT_MULTI": "Вывод (мульти)",
    "OPERATION_TYPE_BOND_TAX": "Налог на купон",
}

INCOME_TYPES = {
    "OPERATION_TYPE_COUPON", "OPERATION_TYPE_DIVIDEND",
    "OPERATION_TYPE_BOND_REPAYMENT", "OPERATION_TYPE_BOND_REPAYMENT_FULL",
}

INSTRUMENT_TYPE_MAP = {
    "share": "Акции",
    "bond": "Облигации",
    "etf": "Фонды",
    "currency": "Валюта",
    "futures": "Фьючерсы",
    "option": "Опционы",
    "sp": "Структурные продукты",
}

ACCOUNT_TYPE_MAP = {
    "ACCOUNT_TYPE_TINKOFF": "Брокерский",
    "ACCOUNT_TYPE_TINKOFF_IIS": "ИИС",
}


# ── Main ────────────────────────────────────────────────────────────────

def main():
    print("Подключаюсь к T-Invest API...")
    accounts = get_accounts()
    if not accounts:
        print("Не удалось получить список счетов!")
        sys.exit(1)

    print(f"Найдено {len(accounts)} открытых счетов")

    instruments_cache = {}
    all_positions = []
    all_operations = []

    for acc in accounts:
        acc_id = acc["id"]
        acc_name = acc.get("name", acc_id)
        acc_type = ACCOUNT_TYPE_MAP.get(acc.get("type", ""), acc.get("type", ""))
        print(f"\n── {acc_name} ({acc_type}) ──")

        # Fetch portfolio
        print("  Загружаю портфель...")
        portfolio = get_portfolio(acc_id)
        if portfolio:
            positions = portfolio.get("positions", [])
            print(f"  Позиций: {len(positions)}")

            for pos in positions:
                uid = pos.get("instrumentUid", "")
                ticker = pos.get("ticker", pos.get("figi", ""))

                # Get instrument details (for sector, name, currency)
                if uid and uid not in instruments_cache:
                    inst = get_instrument_by_uid(uid)
                    if inst:
                        instruments_cache[uid] = inst

                inst_info = instruments_cache.get(uid, {})
                name = inst_info.get("name", ticker)
                sector = inst_info.get("sector", "")
                inst_type = INSTRUMENT_TYPE_MAP.get(
                    pos.get("instrumentType", ""), pos.get("instrumentType", "")
                )
                currency = inst_info.get("currency", "rub").upper()

                qty = quotation_value(pos.get("quantity"))
                avg_price = money_value(pos.get("averagePositionPrice"))
                cur_price = money_value(pos.get("currentPrice"))
                # For bonds, use nominal percentage
                value = money_value(pos.get("currentNkd", {})) + qty * cur_price
                expected_yield = quotation_value(pos.get("expectedYield"))

                cost_basis = qty * avg_price
                pnl = expected_yield
                pnl_pct = pnl / cost_basis if cost_basis else 0

                all_positions.append({
                    "account_name": acc_name,
                    "account_type_display": acc_type,
                    "ticker": ticker,
                    "name": name,
                    "instrument_type": inst_type,
                    "sector": sector,
                    "currency": currency,
                    "quantity": qty,
                    "avg_price": avg_price,
                    "cur_price": cur_price,
                    "total_value": qty * cur_price,
                    "pnl": pnl,
                    "pnl_pct": pnl_pct,
                })

        # Fetch operations (last 5 years)
        print("  Загружаю операции...")
        to_date = datetime.now(timezone.utc)
        from_date = to_date - timedelta(days=365 * 5)
        ops = get_operations(acc_id, from_date, to_date)
        print(f"  Операций: {len(ops)}")

        for op in ops:
            op_type = op.get("type", op.get("operationType", ""))
            type_display = OPERATION_TYPES.get(op_type, op_type)

            # Use ticker/name directly from operation (already in response)
            ticker = op.get("ticker", op.get("figi", ""))
            name = op.get("name", op.get("description", ""))

            payment = money_value(op.get("payment"))
            commission = money_value(op.get("commission"))
            quantity = int(op.get("quantity", 0) or 0)
            price = money_value(op.get("price"))
            currency = op.get("currency", "rub").upper()
            date = parse_ts(op.get("date"))

            all_operations.append({
                "date": date,
                "account_name": acc_name,
                "ticker": ticker,
                "name": name,
                "type": op_type,
                "type_display": type_display,
                "quantity": quantity if quantity else "",
                "price": price if price else "",
                "payment": payment,
                "currency": currency,
                "commission": commission,
                "is_income": op_type in INCOME_TYPES,
            })

    # Build Excel
    print(f"\nВсего позиций: {len(all_positions)}")
    print(f"Всего операций: {len(all_operations)}")
    print("\nСоздаю Excel-отчёт...")

    wb = Workbook()
    # Remove default sheet
    wb.remove(wb.active)

    build_summary_sheet(wb, all_positions, all_operations)
    build_account_summary_sheet(wb, all_positions, all_operations)
    build_portfolio_sheet(wb, all_positions, instruments_cache)
    build_operations_sheet(wb, all_operations)
    build_dividends_sheet(wb, all_operations)
    build_commissions_sheet(wb, all_operations)

    output_path = "/opt/t-invest/t-invest-analytics.xlsx"
    wb.save(output_path)
    print(f"\nГотово! Файл сохранён: {output_path}")


if __name__ == "__main__":
    main()
