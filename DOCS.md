# T-Invest Analytics Report Builder v2

## Полная техническая документация

---

## 1. Обзор проекта

**T-Invest Analytics Report Builder** — это автономный Python-скрипт, который генерирует инвестиционный Excel-отчёт аналитического качества (уровня Bloomberg/Tinkoff Pulse Pro) на основе данных из T-Invest REST API (бывший Тинькофф Инвестиции).

**Для кого:** частные инвесторы, использующие брокера Т-Банк (Тинькофф), которым нужна глубокая аналитика портфеля за пределами того, что предлагает мобильное приложение.

**Что делает:**
- Подключается к T-Invest API по токену (read-only)
- Загружает все открытые счета, текущие позиции, операции и исторические свечи
- Рассчитывает XIRR, FIFO P&L, месячную доходность, просадки, налоговую нагрузку
- Реконструирует историю стоимости портфеля по месячным снимкам
- Формирует многолистовый Excel-файл с таблицами, графиками и conditional formatting

**Период анализа:** с `01.01.2024` (настраивается через `ANALYSIS_START`).

**Выходной файл:** `/opt/t-invest/t-invest-analytics_YYYYMMDD_HHMMSS.xlsx`

---

## 2. Архитектура

### Структура файла

Файл `build_report.py` (~2060 строк) организован в четыре логических слоя:

```
┌─────────────────────────────────────────┐
│            КОНФИГУРАЦИЯ                 │  строки 1–147
│  Константы, стили, маппинги типов       │
├─────────────────────────────────────────┤
│            API-СЛОЙ                     │  строки 149–250
│  Session, api_call, get_*, parse_*      │
├─────────────────────────────────────────┤
│            РАСЧЁТЫ                      │  строки 252–520
│  XIRR, FIFO P&L, portfolio history,    │
│  monthly returns, drawdowns, taxes      │
├─────────────────────────────────────────┤
│            EXCEL-ГЕНЕРАЦИЯ              │  строки 523–1768
│  Хелперы + 12 sheet-builder функций    │
├─────────────────────────────────────────┤
│            MAIN                         │  строки 1770–2060
│  Оркестрация: fetch → calc → build      │
└─────────────────────────────────────────┘
```

### Поток данных

```
T-Invest REST API
      │
      ▼
 ┌──────────┐     ┌───────────┐     ┌──────────────┐
 │ Accounts │────▶│ Portfolio  │────▶│ all_positions│
 │          │     │ Operations │────▶│ all_operations│
 │          │     │ Instruments│────▶│ instruments_ │
 │          │     │ Candles    │────▶│  cache       │
 └──────────┘     └───────────┘     └──────┬───────┘
                                           │
                           ┌───────────────┼───────────────┐
                           ▼               ▼               ▼
                     ┌──────────┐   ┌───────────┐   ┌────────────┐
                     │  XIRR    │   │ FIFO P&L  │   │ Portfolio  │
                     │ cashflows│   │closed_lots │   │  history   │
                     └────┬─────┘   └─────┬─────┘   └──────┬─────┘
                          │               │                │
                          └───────────────┼────────────────┘
                                          ▼
                                   ┌─────────────┐
                                   │  Excel (16+ │
                                   │   листов)   │
                                   └─────────────┘
```

---

## 3. API-слой

### Базовая конфигурация

```python
BASE_URL = "https://invest-public-api.tinkoff.ru/rest"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}
```

Используется `requests.Session()` для переиспользования TCP-соединений (session reuse).

### Функция `api_call(service, method, body=None)`

Универсальный вызов T-Invest REST API:

```python
url = f"{BASE_URL}/tinkoff.public.invest.api.contract.v1.{service}/{method}"
resp = SESSION.post(url, json=body or {})
```

Все вызовы — POST (даже для чтения), возвращается JSON или `None` при ошибке.

### Endpoints

| Функция | Service | Method | Описание |
|---------|---------|--------|----------|
| `get_accounts()` | `UsersService` | `GetAccounts` | Список открытых счетов |
| `get_portfolio(account_id)` | `OperationsService` | `GetPortfolio` | Текущий портфель (позиции, P&L) |
| `get_operations(account_id, from, to)` | `OperationsService` | `GetOperationsByCursor` | Исполненные операции за период |
| `get_instrument_by_uid(uid)` | `InstrumentsService` | `GetInstrumentBy` | Справочная информация об инструменте |
| `get_candles(figi, from, to)` | `MarketDataService` | `GetCandles` | Исторические свечи (дневной интервал) |

### Пагинация (cursor-based)

Функция `get_operations` реализует cursor-based пагинацию:

```python
body = {
    "accountId": account_id,
    "from": from_date.isoformat(),
    "to": to_date.isoformat(),
    "state": "OPERATION_STATE_EXECUTED",
    "limit": 1000,             # макс. элементов за запрос
    "cursor": cursor,          # пустая строка для первого запроса
    ...
}
```

Цикл продолжается пока `data.get("hasNext")` равно `True`, обновляя `cursor = data.get("nextCursor")`.

### Rate limiting и chunking

Для свечей (`get_candles`) реализовано:
- **Chunking по 364 дня** — API ограничивает период запроса для дневных свечей
- **Sleep 0.3 сек** между запросами чанков — защита от rate limiting
- **Параллельная загрузка** тикеров через `ThreadPoolExecutor(max_workers=4)` в `main()`

---

## 4. Формат данных T-Invest API

### MoneyValue / Quotation

T-Invest API возвращает денежные значения и котировки в формате `{units, nano}`:

```json
{
  "units": "123",
  "nano": 450000000
}
```

Это число `123.45`. Поле `nano` — дробная часть, 9 знаков (наносекунды в аналогии).

Парсинг:

```python
def money_value(m):
    if not m:
        return 0.0
    return int(m.get("units", 0)) + int(m.get("nano", 0)) / 1_000_000_000
```

`quotation_value` — алиас той же функции, семантически используется для количеств.

### Парсинг timestamp

```python
def parse_ts(ts_str):
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return dt.replace(tzinfo=None)  # все даты хранятся как naive UTC
```

Все временные метки внутри системы — naive datetime (без tzinfo), подразумевается UTC.

---

## 5. Алгоритмы расчётов

### 5.1 XIRR (Extended Internal Rate of Return)

XIRR — годовая доходность портфеля с учётом пополнений/выводов в произвольные даты.

**Формула NPV:**

```
NPV(r) = Σ CFi / (1 + r) ^ ((di - d0) / 365.25)
```

где:
- `CFi` — денежный поток (пополнение как отрицательное, текущая стоимость как положительное)
- `di` — дата потока
- `d0` — дата самого раннего потока
- `r` — искомая ставка

**Реализация:**

```python
def xirr(cashflows):
    d0 = min(cf[0] for cf in cashflows)

    def npv(rate):
        return sum(cf / ((1 + rate) ** ((d - d0).days / 365.25))
                   for d, cf in cashflows)

    return brentq(npv, -0.99, 10.0, maxiter=1000)
```

Используется метод Брента (`scipy.optimize.brentq`) для нахождения корня `NPV(r) = 0` на интервале `[-0.99, 10.0]` (от -99% до +1000% годовых). При неудаче пробуется более узкий интервал `[-0.5, 5.0]`.

**Формирование cashflows:**
- Пополнения (`DEPOSIT_TYPES`) → отрицательные (деньги «уходят» от инвестора в портфель)
- Выводы (`WITHDRAWAL_TYPES`) → положительные (деньги «возвращаются»)
- Финальный cashflow → текущая стоимость портфеля на сегодня (положительный)

XIRR считается как для всего портфеля, так и отдельно для каждого счёта.

### 5.2 FIFO P&L (реализованная прибыль/убыток)

Функция `calculate_realized_pnl(operations)` реализует FIFO-метод сопоставления покупок и продаж.

**Алгоритм:**

1. Операции группируются по тикеру
2. Внутри тикера сортируются по дате
3. Покупки помещаются в очередь `deque` (FIFO): `{date, qty, price, account, name}`
4. При продаже из головы очереди забираются лоты:

```python
while remaining > 0 and fifo_queue:
    lot = fifo_queue[0]
    matched = min(remaining, lot["qty"])
    pnl = matched * (sell_price - lot["price"])
    # ...
    lot["qty"] -= matched
    remaining -= matched
    if lot["qty"] <= 0:
        fifo_queue.popleft()
```

**Годовая доходность закрытого лота:**

```
annual_return = (sell_price / buy_price) ^ (365 / days_held) - 1
```

Результат — список словарей `closed_lots` с полями: `ticker`, `buy_date`, `sell_date`, `quantity`, `buy_price`, `sell_price`, `pnl`, `pnl_pct`, `days_held`, `annual_return`.

### 5.3 Реконструкция истории портфеля

Функция `reconstruct_portfolio_history(operations, candles_cache)` восстанавливает стоимость портфеля на конец каждого месяца, используя ledger-подход.

**Алгоритм:**

1. **Построение сетки дат**: все month-end от `ANALYSIS_START` до сегодня + текущая дата
2. **Позиционный ledger**: `positions = defaultdict(float)` — {ticker: qty}
3. Линейный проход по операциям (один раз, `op_idx` не сбрасывается):
   - `BUY` → `positions[ticker] += qty`
   - `SELL` → `positions[ticker] -= qty`
   - `DEPOSIT/WITHDRAWAL` → обновление кумулятивных сумм
4. **Оценка стоимости** на каждый month-end через `bisect_right`:

```python
idx = bisect_right(dates, me) - 1
price = prices[idx] if idx >= 0 else 0
total_value += qty * price
```

`bisect_right` обеспечивает O(log N) поиск ближайшей цены закрытия на дату, не превышающую month-end.

**Результат:** список `{date, value, cum_deposits, cum_withdrawals}`.

### 5.4 Месячная доходность (cash-flow adjusted)

```python
def calculate_monthly_returns(portfolio_history):
    net_flow = (curr["cum_deposits"] - prev["cum_deposits"]
                - (curr["cum_withdrawals"] - prev["cum_withdrawals"]))
    ret = (curr["value"] - prev["value"] - net_flow) / prev["value"]
```

Формула исключает влияние пополнений/выводов на расчёт доходности. Результат — словарь `{(year, month): return}`.

### 5.5 Drawdown (просадка от максимума)

```python
def calculate_drawdowns(portfolio_history):
    peak = 0
    for h in portfolio_history:
        if h["value"] > peak:
            peak = h["value"]
        drawdowns.append((h["value"] / peak - 1) if peak > 0 else 0)
```

Peak tracking: на каждом шаге отслеживается исторический максимум, просадка = `(текущее / максимум) - 1`. Всегда <= 0.

### 5.6 Trailing 12-month income

```python
def trailing_12m_income(operations):
    t12m_start = datetime.now().replace(tzinfo=None) - timedelta(days=365)
    return sum(
        op.get("payment", 0) for op in operations
        if op.get("type") in INCOME_TYPES and op.get("date") and op["date"] >= t12m_start
    )
```

Сумма всех дивидендов, купонов и погашений облигаций за последние 365 дней.

### 5.7 Налоговая агрегация

```python
def aggregate_taxes(operations):
    # Группировка: taxes[year][tax_type_display] += abs(payment)
```

Группирует уплаченные налоги по году и типу (НДФЛ, налог на дивиденды, на купоны, корректировки).

Дополнительно рассчитывается:
- **Потенциальный НДФЛ** на нереализованную прибыль: `unrealized_gain * 0.13`
- **Трекер ИИС**: для счетов с «ИИС» в названии считается потенциальный вычет типа А: `min(deposits, 400_000) * 0.13`

---

## 6. Excel-генерация

### 6.1 Описание листов

Отчёт содержит фиксированные листы (11 шт.) + динамические листы для каждой позиции с доступными свечами.

#### Tier 1: Executive Summary

| # | Лист | Функция | Содержимое |
|---|------|---------|------------|
| 1 | **Дашборд** | `build_dashboard()` | KPI-блок (стоимость, P&L, XIRR, доходы за 12 мес, внесено/выведено, реализованный P&L, дивиденды+купоны), график стоимости портфеля (AreaChart), pie-chart аллокации по типам, stacked bar-chart дохода по месяцам, top-10 позиций с DataBar |
| 2 | **Доходность** | `build_returns_sheet()` | Таблица XIRR по счетам, heatmap месячной доходности (год x месяц, ColorScale), график кумулятивной доходности (LineChart), график просадки от максимума (AreaChart) |
| 3 | **Денежные потоки** | `build_cashflows_sheet()` | Таблица месячных потоков (пополнения, выводы, дивиденды, купоны, комиссии, налоги, нетто), график портфель vs внесённые средства, годовая сводка |

#### Tier 2: Аналитика

| # | Лист | Функция | Содержимое |
|---|------|---------|------------|
| 4 | **Портфель** | `build_portfolio_sheet()` | Все текущие позиции: счёт, тикер, название, тип, сектор, валюта, кол-во, средняя цена (broker + FIFO), текущая цена, стоимость, P&L, доля, дивиденды, див. доходность. DataBar на стоимости и доле, ColorScale на P&L% |
| 5 | **Закрытые позиции** | `build_closed_positions_sheet()` | FIFO-реализованный P&L: даты покупки/продажи, цены, P&L, дней в позиции, годовая доходность. Bar-chart топ winners/losers. Сводка по тикерам (кол-во сделок, объёмы, ср. дней) |
| 6 | **Дивиденды и купоны** | `build_dividends_sheet()` | Полный список доходов (дата, счёт, тикер, тип, сумма, год, квартал). Heatmap тикер x месяц (ColorScale white→green). Bar-chart доходов по годам |
| 7 | **Налоги** | `build_taxes_sheet()` | Налоги по годам и типам. Потенциальный НДФЛ на нереализованную прибыль (13%). Трекер ИИС (тип А) с расчётом вычета (макс 400 000 руб/год) |
| 8 | **По счетам** | `build_accounts_sheet()` | Сводка по каждому счёту: тип, стоимость, P&L, XIRR, пополнения, выводы, дивиденды, купоны, комиссии, налоги. Bar-chart сравнения |

#### Tier 3: Данные

| # | Лист | Функция | Содержимое |
|---|------|---------|------------|
| 9 | **Операции** | `build_operations_sheet()` | Все операции хронологически: дата, счёт, тикер, тип, группа, кол-во, цена, сумма, валюта, комиссия, running balance. ColorScale на суммах |
| 10 | **Комиссии** | `build_commissions_sheet()` | Комиссии помесячно по счетам: сумма комиссии, объём торгов, % от объёма. Bar-chart комиссий по месяцам |
| 11 | **Справочник** | `build_instruments_sheet()` | Все использованные инструменты: UID, FIGI, тикер, название, тип, сектор, валюта, страна, лот |
| 12 | **Портфель (история)** | `build_history_sheet()` | Месячные снимки: дата, стоимость, кумулятивные вложения/выводы, доходность %, просадка % |

#### Динамические листы позиций

| # | Лист | Функция | Содержимое |
|---|------|---------|------------|
| 13+ | **📈 {TICKER}** | `build_position_sheet()` | Мини-саммари позиции (сектор, тип, цены, P&L). LineChart с ценой закрытия + маркеры покупок (зелёные треугольники) и продаж (красные ромбы). Таблица сделок по данному тикеру |

### 6.2 Стили и форматирование

#### Палитра

| Константа | Цвет | Hex | Использование |
|-----------|-------|-----|---------------|
| `COLOR_POS` | Тёмно-зелёный | `#2E7D32` | Положительный P&L |
| `COLOR_NEG` | Тёмно-красный | `#C62828` | Отрицательный P&L |
| `COLOR_LINE` | Тёмно-серый | `#37474F` | Линии графиков, primary series |
| `COLOR_AREA` | Светло-серый | `#B0BEC5` | Заливка area-графиков, secondary series |

#### Шрифты

| Константа | Параметры | Где используется |
|-----------|-----------|------------------|
| `HEADER_FONT` | Bold, 10pt | Заголовки таблиц |
| `KPI_FONT` | Bold, 13pt | Значения KPI на дашборде |
| `KPI_LABEL_FONT` | Gray (#808080), 8pt | Подписи KPI |
| `TITLE_FONT` | Bold, 12pt | Заголовки листов |
| `SUBTITLE_FONT` | Bold, 10pt | Подзаголовки секций |

#### Форматы чисел

| Константа | Формат | Пример |
|-----------|--------|--------|
| `RUB_FMT` | `#,##0.00 "₽"` | `1 234 567.89 ₽` |
| `RUB_FMT_INT` | `#,##0 "₽"` | `1 234 568 ₽` |
| `PCT_FMT` | `0.00%` | `12.34%` |
| `DATE_FMT` | `DD.MM.YYYY` | `15.03.2026` |
| `DATETIME_FMT` | `DD.MM.YYYY HH:MM` | `15.03.2026 14:30` |

#### Заливки

- `HEADER_FILL` — светло-серый `#D9D9D9` (единый для всех заголовков, несмотря на наличие именованных `HEADER_FILL_GREEN`, `HEADER_FILL_ORANGE`, `HEADER_FILL_RED`, `HEADER_FILL_PURPLE` — все указывают на один цвет)
- `LIGHT_GREY_FILL` — `#F5F5F5` (зебра-строки)

#### Границы

- `BOTTOM_BORDER` — тонкая серая линия снизу (заголовки)
- `THIN_BORDER` — hair-линия снизу `#D9D9D9` (строки данных)

#### Conditional Formatting

| Тип | Функция | Описание |
|-----|---------|----------|
| `ColorScaleRule` | `add_pnl_color_scale()` | Красно-бело-зелёная шкала: `#FFCDD2` (min) → `#FFFFFF` (0) → `#C8E6C9` (max) |
| `DataBarRule` | Inline | Серые бары (`#B0BEC5`) для визуализации долей и стоимостей |
| `ColorScaleRule` | Inline (дивиденды) | Бело-зелёная: `#FFFFFF` (0) → `#63BE7B` (max) |

### 6.3 Типы графиков

| Тип | Хелпер | Параметры по умолчанию | Где используется |
|-----|--------|------------------------|------------------|
| `AreaChart` | `add_portfolio_chart()` | 18x9, style 2, legend bottom | Дашборд, Денежные потоки |
| `AreaChart` | Inline (drawdown) | 16x9, style 2 | Доходность |
| `LineChart` | `make_line_chart()` | 16x9, style 2, legend bottom | Кумулятивная доходность, Позиции |
| `BarChart` | `make_bar_chart()` | 16x9, col, style 2, legend bottom | Доходы, P&L, комиссии, счета |
| `PieChart` | `make_pie_chart()` | 12x9, style 2, showPercent=True | Аллокация |

Для листов позиций используется комбинированный LineChart с тремя series:
- **Series 0**: Цена закрытия — сплошная линия (`COLOR_LINE`, ширина ~2pt)
- **Series 1**: Покупки — треугольные маркеры (зелёные, `symbol='triangle'`, size 7), без линии
- **Series 2**: Продажи — ромбовидные маркеры (красные, `symbol='diamond'`, size 7), без линии

### 6.4 Таблицы (Table objects)

Функция `add_table(ws, ref, name)`:

```python
table = Table(displayName=safe_name, ref=ref)
style = TableStyleInfo(
    name="TableStyleLight1",
    showFirstColumn=False,
    showLastColumn=False,
    showRowStripes=True,      # зебра-строки
    showColumnStripes=False,
)
```

Имена таблиц очищаются от пробелов, дефисов и точек (замена на `_`).

Используемые таблицы: `CashFlows`, `Portfolio`, `ClosedPositions`, `DividendsCoupons`, `CommissionsDetail`, `Operations`, `Instruments`, `PortfolioHistory`.

### 6.5 Вспомогательные функции Excel

| Функция | Описание |
|---------|----------|
| `style_header_row(ws, row, num_cols, fill)` | Применяет `HEADER_FONT`, заливку, center+wrap, `BOTTOM_BORDER` к строке |
| `auto_width(ws, min_width=10, max_width=35)` | Автоподбор ширины колонок по содержимому |
| `write_kpi(ws, row, col, label, value, fmt)` | KPI-блок: маленькая метка сверху, большое значение снизу |
| `add_pnl_color_scale(ws, cell_range)` | Трёхцветная шкала red-white-green с центром на 0 |

---

## 7. Конфигурация

### Файл `.env`

```env
TINKOFF_TOKEN=your_token_here
```

Токен получается в личном кабинете T-Invest: https://www.tbank.ru/invest/settings/api/

Токен должен иметь разрешение на чтение (read-only). Полный доступ не требуется.

### Константа `ANALYSIS_START`

```python
ANALYSIS_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
```

Начало периода анализа. Все операции и свечи загружаются начиная с этой даты. Для изменения периода — отредактировать значение в коде.

### Маппинги типов операций

- `OPERATION_TYPES` — маппинг `API_TYPE → "Человеческое название"` (19 типов)
- `OPERATION_GROUPS` — маппинг `API_TYPE → "Группа"` (Торговля, Доход, Налоги, Комиссии, Денежные потоки, Прочее)
- `INCOME_TYPES` — множество типов, считающихся доходом (купоны, дивиденды, погашения)
- `TAX_TYPES` — множество налоговых типов
- `BUY_SELL_TYPES` — покупки и продажи
- `DEPOSIT_TYPES`, `WITHDRAWAL_TYPES` — пополнения и выводы (включая мульти-варианты)
- `INSTRUMENT_TYPE_MAP` — перевод типов инструментов (`share → Акции`, `bond → Облигации`, ...)
- `ACCOUNT_TYPE_MAP` — типы счетов (`ACCOUNT_TYPE_TINKOFF → Брокерский`, `ACCOUNT_TYPE_TINKOFF_IIS → ИИС`)

---

## 8. Зависимости

### pip-пакеты (requirements.txt)

| Пакет | Версия | Назначение |
|-------|--------|------------|
| `requests` | * | HTTP-клиент для REST API |
| `openpyxl` | * | Генерация Excel-файлов (.xlsx): таблицы, графики, стили, conditional formatting |
| `python-dotenv` | * | Загрузка переменных окружения из `.env` |

### Неявные зависимости (не в requirements.txt, но используются)

| Пакет | Назначение |
|-------|------------|
| `scipy` | `scipy.optimize.brentq` — метод Брента для решения уравнения NPV=0 при расчёте XIRR |

### Стандартная библиотека Python

`os`, `sys`, `time`, `bisect` (bisect_right), `concurrent.futures` (ThreadPoolExecutor, as_completed), `datetime`, `collections` (defaultdict, deque).

---

## 9. Запуск

### Предварительные шаги

1. Получить токен T-Invest API (read-only) в настройках: https://www.tbank.ru/invest/settings/api/

2. Создать файл `.env` (или скопировать `.env.example`):
   ```
   TINKOFF_TOKEN=t.xxxxxxxxxxxxxxxxxxxxx
   ```

3. Установить зависимости:
   ```bash
   cd /opt/t-invest
   source venv/bin/activate  # или создать: python3 -m venv venv
   pip install requests openpyxl python-dotenv scipy
   ```

### Запуск

```bash
cd /opt/t-invest
source venv/bin/activate
python build_report.py
```

### Вывод в консоль

```
══════════════════════════════════════════════════════════
  T-Invest Analytics Report Builder v2
══════════════════════════════════════════════════════════

Подключаюсь к T-Invest API...
Найдено 2 открытых счетов

── Брокерский (Брокерский) ──
  Загружаю портфель...
  Позиций: 15
  Загружаю операции...
  Операций: 342

── ИИС (ИИС) ──
  ...

── Расчёт аналитики ──
  Считаю XIRR...
  XIRR общий: 18.45%
  Считаю реализованный P&L (FIFO)...
  Закрытых лотов: 28

── Загружаю исторические цены ──
  Загружаю свечи для 20 инструментов (параллельно)...
  SBER: 290 свечей
  LKOH: 290 свечей
  ...

  Реконструкция истории портфеля...
  Снимков: 15

══════════════════════════════════════════════════════════
  Всего позиций: 22
  Всего операций: 485
  Создаю Excel-отчёт...
══════════════════════════════════════════════════════════
  Создаю листы позиций...

✅ Готово! Файл сохранён: /opt/t-invest/t-invest-analytics_20260315_143025.xlsx
   Листов: 31
   Размер: 845 KB
```

### Формат выходного файла

- **Формат:** `.xlsx` (Office Open XML)
- **Имя:** `t-invest-analytics_YYYYMMDD_HHMMSS.xlsx`
- **Расположение:** `/opt/t-invest/`
- **Размер:** зависит от количества инструментов и операций (типично 500 KB — 2 MB)
- **Листов:** 12 фиксированных + по одному на каждый инструмент с историческими свечами
- **Первый активный лист:** «Дашборд»
- **Freeze panes:** включён на листах Денежные потоки (A4), Портфель (C2), Дивиденды (A5), Операции (A2), Справочник (A2), Портфель (история) (A2)
