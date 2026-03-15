# T-Invest Analytics Report Builder

## Проект

Python-скрипт для генерации Excel-отчёта с инвестиционной аналитикой из T-Invest API (Тинькофф Инвестиции).

## Структура

```
build_report.py   — единственный файл, ~2000 строк, 4 слоя: API → Расчёты → Excel-хелперы → Sheet builders
.env              — TINKOFF_TOKEN (read-only API ключ, НЕ коммитить)
DOCS.md           — полная техническая документация
requirements.txt  — зависимости
venv/             — виртуальное окружение Python 3.12
```

## Запуск

```bash
source venv/bin/activate
python build_report.py
```

Результат: `t-invest-analytics_YYYYMMDD_HHMMSS.xlsx` (каждый запуск — новый файл).

## Ключевые алгоритмы

- **XIRR** — scipy.optimize.brentq на NPV-функции, cashflows = депозиты (−) + текущая стоимость (+)
- **FIFO P&L** — deque-очередь покупок, matching при продажах
- **История портфеля** — ledger позиций + bisect_right по свечам на month-end
- **Месячная доходность** — cash-flow adjusted returns

## Стиль Excel

Строгий финансовый: серая палитра, компактные графики (16×9), шрифт 10pt, минимум цвета. P&L: приглушённый красный/зелёный. Без ярких вкладок.

## API

- `requests.Session()` — reuse TCP/TLS
- Свечи загружаются параллельно (`ThreadPoolExecutor`, 4 воркера)
- Чанки по 364 дня для дневных свечей
- `time.sleep(0.3)` между чанками (rate limit)

## Важно

- `.env` в `.gitignore` — токен не должен попасть в репозиторий
- `ANALYSIS_START` = 01.01.2024 — начало периода анализа
- Все даты naive UTC (timezone strip в parse_ts)
- Операции сортируются один раз в main(), не пересортировывать в sheet builders
