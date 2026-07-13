# Market Pulse FX

An end-to-end Data Engineering project that ingests daily foreign exchange rates, stores them in PostgreSQL, calculates daily market movements, and generates a simple market summary with Apache Airflow.

---

## Business Objective

The goal of this project is to automate the collection and analysis of daily foreign exchange data.

The pipeline separates raw market data from analytical outputs. It demonstrates how two Airflow DAGs can work together through Asset-based scheduling while PostgreSQL stores both detailed market movements and daily summary insights.

---

## Architecture

```text
                 Frankfurter API
                        │
                        ▼
                DAG 1: fx_ingest
        ┌──────────────────────────┐
        │ create_tables()          │
        │          ↓               │
        │ fetch_fx_rates()         │
        │          ↓               │
        │ load_fx_rates()          │
        │          ↓               │
        │ log_run                  │
        └──────────┬───────────────┘
                   │
                   ▼
              PostgreSQL
        ┌──────────────────────┐
        │ fx_rates             │
        │ fx_daily_moves       │
        │ fx_insights          │
        └──────────┬───────────┘
                   │
                   ▼
      Asset: market_pulse_fx_rates
                   │
                   ▼
              DAG 2: fx_insights
        ┌──────────────────────────────┐
        │ calculate_daily_moves()      │
        │          ├──────────────────► load_daily_moves()
        │          │
        │          ▼
        │ generate_market_insight()    │
        │          │
        │          ▼
        │ load_market_insight()        │
        │          │
        │          ▼
        │ log_run                      │
        └──────────────────────────────┘
```

---

## Tech Stack

| Category            | Technology                         |
| :------------------ | :--------------------------------- |
| Programming         | Python                             |
| Workflow orchestration | Apache Airflow 3                |
| Database            | PostgreSQL                         |
| Containers          | Docker and Docker Compose          |
| Database driver     | psycopg2                           |
| HTTP client         | Requests                           |
| Data source         | Frankfurter FX API                 |
| Version control     | Git and GitHub                     |

---

## Project Structure

```text
market-pulse-fx/
├── dags/
│   ├── fx_ingest.py
│   └── fx_insights.py
├── logs/
├── plugins/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

> The real `.env` file should not be committed to GitHub.

---

## Database Schema

### `fx_rates`

Stores normalized daily FX rates retrieved from the Frankfurter API.

| Column             | Type      | Description                                      |
| :----------------- | :-------- | :----------------------------------------------- |
| `rate_date`        | DATE      | Trading date                                     |
| `pair`             | TEXT      | Standard market currency pair                    |
| `base_currency`    | TEXT      | Base currency                                    |
| `quote_currency`   | TEXT      | Quote currency                                   |
| `rate`             | NUMERIC   | Normalized market exchange rate                  |
| `raw_api_base`     | TEXT      | Original API base currency                       |
| `raw_api_quote`    | TEXT      | Original API quote currency                      |
| `raw_api_rate`     | NUMERIC   | Original exchange rate returned by the API       |
| `created_at`       | TIMESTAMP | Row creation timestamp                           |

Primary key:

```text
(rate_date, pair)
```

---

### `fx_daily_moves`

Stores daily percentage movement for every currency pair.

| Column              | Type      | Description                              |
| :------------------ | :-------- | :--------------------------------------- |
| `rate_date`         | DATE      | Trading date                             |
| `pair`              | TEXT      | Currency pair                            |
| `pct_change`        | NUMERIC   | Daily percentage change                  |
| `direction`         | TEXT      | `UP`, `DOWN`, or `FLAT`                  |
| `volatility_flag`   | TEXT      | `NORMAL` or `HIGH`                       |
| `created_at`        | TIMESTAMP | Row creation or update timestamp         |

Primary key:

```text
(rate_date, pair)
```

---

### `fx_insights`

Stores one summarized market insight for each trading day.

| Column              | Type      | Description                                      |
| :------------------ | :-------- | :----------------------------------------------- |
| `insight_date`      | DATE      | Trading date                                     |
| `top_mover_pair`    | TEXT      | Currency pair with the largest absolute move     |
| `top_mover_pct`     | NUMERIC   | Percentage move of the top mover                 |
| `market_regime`     | TEXT      | `NORMAL` or `HIGH_VOLATILITY`                    |
| `high_touch_flag`   | BOOLEAN   | Indicates whether the market needs extra attention |
| `summary`           | TEXT      | Human-readable daily market summary              |
| `created_at`        | TIMESTAMP | Row creation or update timestamp                 |

Primary key:

```text
insight_date
```

---

## DAG 1: `fx_ingest`

The `fx_ingest` DAG runs on weekdays and handles ingestion.

```text
create_tables()
        ↓
fetch_fx_rates()
        ↓
load_fx_rates()
        ↓
log_run
```

### Responsibilities

- Creates the required PostgreSQL tables if they do not exist
- Retrieves daily FX rates from the Frankfurter API
- Converts API output into standard market currency-pair formats
- Inverts selected currency rates when required
- Inserts or updates rows in `fx_rates`
- Publishes the `market_pulse_fx_rates` Airflow Asset
- Writes a completion message to the Airflow log

### Currency-pair normalization

The API uses USD as the base currency. Some market-standard pairs therefore need to be inverted.

| API quote | Market pair | Invert rate |
| :-------- | :---------- | :---------- |
| EUR       | EURUSD      | Yes         |
| GBP       | GBPUSD      | Yes         |
| AUD       | AUDUSD      | Yes         |
| NZD       | NZDUSD      | Yes         |
| JPY       | USDJPY      | No          |
| CHF       | USDCHF      | No          |
| CAD       | USDCAD      | No          |
| MYR       | USDMYR      | No          |
| THB       | USDTHB      | No          |

---

## DAG 2: `fx_insights`

The `fx_insights` DAG is triggered when the `market_pulse_fx_rates` Asset is updated.

```text
                    calculate_daily_moves()
                       /               \
                      ↓                 ↓
          load_daily_moves()   generate_market_insight()
                                        ↓
                              load_market_insight()
                      \                 /
                       \               /
                              log_run
```

### Responsibilities

- Finds the latest available FX rate date
- Finds the previous available FX rate date
- Compares rates for matching currency pairs
- Calculates daily percentage changes
- Classifies movement as `UP`, `DOWN`, or `FLAT`
- Flags large daily changes as `HIGH`
- Finds the top mover by absolute percentage change
- Generates one daily market summary
- Stores detailed movements in `fx_daily_moves`
- Stores the daily summary in `fx_insights`

---

## Business Logic

### Daily percentage change

```text
pct_change = ((current_rate - previous_rate) / previous_rate) × 100
```

### Direction

| Condition         | Direction |
| :---------------- | :-------- |
| `pct_change > 0`  | `UP`      |
| `pct_change < 0`  | `DOWN`    |
| `pct_change == 0` | `FLAT`    |

### Volatility flag

| Condition                  | Flag     |
| :------------------------- | :------- |
| `abs(pct_change) > 1`      | `HIGH`   |
| Otherwise                  | `NORMAL` |

### Market regime

| Condition                             | Market regime      |
| :------------------------------------ | :----------------- |
| At least 3 pairs are flagged `HIGH`   | `HIGH_VOLATILITY`  |
| Otherwise                             | `NORMAL`           |

### Top mover

The top mover is the pair with the largest absolute percentage change.

```text
top_mover = max(abs(pct_change))
```

---

## Features

### Data Engineering

- Multi-DAG pipeline
- Airflow TaskFlow API
- Asset-based scheduling
- REST API ingestion
- PostgreSQL storage
- SQL self-join
- XCom-based data passing
- Explicit and automatic task dependencies

### Data Quality and Reliability

- PostgreSQL UPSERT operations
- Idempotent daily loads
- Transaction commit and rollback
- Cursor and connection cleanup
- Empty-data checks with `AirflowSkipException`
- API timeout and HTTP error handling

### Business Analytics

- FX pair normalization
- Daily percentage-change calculation
- Direction classification
- Volatility flagging
- Top-mover detection
- Daily market-regime classification
- Human-readable market summary

### Infrastructure

- Dockerized Airflow environment
- Dockerized PostgreSQL
- Persistent PostgreSQL volume
- Environment-variable configuration
- Separate Airflow metadata and project databases

---

## How to Run

### 1. Clone the repository

```bash
git clone git@github.com:popupwhatup/market-pulse-fx.git
cd market-pulse-fx
```

### 2. Create the environment file

Create a file named `.env` in the project root.

```env
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_DB=airflow
PROJECT_DB=market_pulse
```

Do not commit the real `.env` file.

A safer pattern is to commit `.env.example` and copy it locally:

```bash
cp .env.example .env
```

Then update the values inside `.env`.

### 3. Build and start the containers

```bash
docker compose up -d --build
```

To recreate containers after changing environment variables:

```bash
docker compose up -d --build --force-recreate
```

### 4. Create the project database

```bash
docker compose exec postgres \
psql -U your_postgres_user -d airflow \
-c "CREATE DATABASE market_pulse;"
```

Run this command only if the `market_pulse` database does not already exist.

### 5. Open Airflow

```text
http://localhost:8084
```

Use the administrator credentials generated or configured in your local Airflow environment.

### 6. Run the pipeline

Trigger:

```text
fx_ingest
```

After `load_fx_rates()` updates the Asset, Airflow will automatically trigger:

```text
fx_insights
```

---

## Useful Commands

Check container status:

```bash
docker compose ps
```

View Airflow logs:

```bash
docker compose logs airflow
```

Check the project database:

```bash
docker compose exec postgres \
psql -U your_postgres_user -d market_pulse
```

List tables:

```sql
\dt
```

View FX rates:

```sql
SELECT *
FROM fx_rates
ORDER BY rate_date DESC, pair;
```

View daily movements:

```sql
SELECT *
FROM fx_daily_moves
ORDER BY rate_date DESC, pair;
```

View market insights:

```sql
SELECT *
FROM fx_insights
ORDER BY insight_date DESC;
```

Stop the containers:

```bash
docker compose down
```

Stop the containers and remove volumes:

```bash
docker compose down -v
```

> Warning: removing volumes deletes the local PostgreSQL data.

---

## Sample Output

### Daily FX movements

| Rate date  | Pair    | Change | Direction | Volatility |
| :--------- | :------ | -----: | :-------- | :--------- |
| 2026-07-10 | EURUSD  |  0.35% | UP        | NORMAL     |
| 2026-07-10 | GBPUSD  | -0.22% | DOWN      | NORMAL     |
| 2026-07-10 | USDJPY  | -1.25% | DOWN      | HIGH       |

### Daily market insight

| Field               | Value         |
| :------------------ | :------------ |
| Insight date        | 2026-07-10    |
| Top mover pair      | USDJPY        |
| Top mover change    | -1.25%        |
| Market regime       | NORMAL        |
| High-touch flag     | False         |

> USDJPY was the top mover today with a -1.25% move. Market volatility remains normal.

The market regime remains `NORMAL` in this example because fewer than three pairs are flagged as `HIGH`.

---

## Lessons Learned

This project provided hands-on experience with:

- Designing a multi-DAG Airflow pipeline
- Connecting independent DAGs with Airflow Assets
- Passing task outputs through XCom
- Understanding automatic dependencies created by task arguments
- Creating explicit dependencies for tasks without upstream arguments
- Reading and writing PostgreSQL data with psycopg2
- Using `fetchone()` and `fetchall()`
- Using SQL self-joins to compare two trading dates
- Implementing idempotent loads with `ON CONFLICT`
- Managing PostgreSQL transactions
- Separating detailed analytical data from summary-level insights
- Running Airflow and PostgreSQL with Docker Compose
- Managing configuration with environment variables

---

## Known Limitations

### Latest-date processing

The current implementation uses:

```sql
SELECT MAX(rate_date)
FROM fx_rates;
```

This means `fx_insights` always analyzes the latest date available in the database.

This works during the normal daily pipeline because the Asset is updated immediately after that day's ingestion. However, historical backfills may trigger several DAG runs that all process the same latest database date.

### Basic volatility logic

The current volatility rule is intentionally simple:

```text
abs(pct_change) > 1%
```

It does not yet use rolling volatility, historical distributions, or pair-specific thresholds.

### Basic market insight

The current insight is a daily headline rather than a complete market report.

Detailed results for all pairs remain available in `fx_daily_moves`.

### Local development only

The current setup is designed for local learning and development. It does not yet include cloud deployment, centralized secret management, monitoring, or production-grade alerting.

---

## Future Improvements

### Scheduling and backfills

- Process the specific Asset event date instead of always using `MAX(rate_date)`
- Pass Asset event metadata into `fx_insights`
- Improve historical backfill behavior
- Add stronger validation for missing trading dates

### Market analytics

- Add 5-day and 20-day rolling volatility
- Add moving-average crossover signals
- Add consecutive up/down streak detection
- Add breakout detection
- Add z-score anomaly detection
- Add an `fx_signals` table
- Add pair-specific volatility thresholds

### Data quality

- Add row-count checks
- Add duplicate checks
- Add null-value validation
- Add schema validation
- Add automated data-quality tests

### Testing

- Add Python unit tests
- Add SQL integration tests
- Add DAG import tests
- Add API mocking
- Add end-to-end pipeline tests

### Reporting

- Build a Streamlit dashboard
- Build a Power BI dashboard
- Add email or Slack notifications
- Add a daily HTML or PDF market report

### Deployment

- Add CI/CD with GitHub Actions
- Deploy Airflow and PostgreSQL to cloud infrastructure
- Use a managed database
- Add secret management
- Add monitoring and failure alerts

---

## Security Notes

- Do not commit `.env`
- Do not publish real usernames or passwords
- Use `.env.example` for documentation
- Use stronger secrets outside local development
- Use a secret manager in production environments

---

## Disclaimer

This project is for educational and portfolio purposes only.

The generated market summary is not financial advice and should not be used as the sole basis for trading or investment decisions.
