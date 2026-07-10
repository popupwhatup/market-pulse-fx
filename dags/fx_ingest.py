from __future__ import annotations

from datetime import datetime
import requests
import psycopg2
import os

from airflow.sdk import dag, task, Asset
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException

fx_rates_asset = Asset("market_pulse_fx_rates")


DB_CONFIG = {
    "host": os.environ["POSTGRES_HOST"],   
    "port": int(os.environ["POSTGRES_PORT"]),
    "dbname": os.environ["PROJECT_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

@dag(
    dag_id="fx_ingest",
    start_date=datetime(2026, 7, 1),
    schedule="30 16 * * 1-5",
    catchup=False,
    tags=["project4", "fx", "ingest"]
)

def fx_ingest():

    @task
    def create_tables():
        conn = None
        cursor = None

        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fx_rates (
                    rate_date DATE NOT NULL,
                    pair TEXT NOT NULL,
                    base_currency TEXT NOT NULL,
                    quote_currency TEXT NOT NULL,
                    rate NUMERIC NOT NULL,
                    raw_api_base TEXT NOT NULL,
                    raw_api_quote TEXT NOT NULL,
                    raw_api_rate NUMERIC NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (rate_date, pair)
                );            
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fx_daily_moves (
                    rate_date DATE NOT NULL,
                    pair TEXT NOT NULL,
                    pct_change NUMERIC,
                    direction TEXT,
                    volatility_flag TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (rate_date, pair)           
                );
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fx_insights (
                    insight_date DATE PRIMARY KEY,
                    top_mover_pair TEXT,
                    top_mover_pct NUMERIC,
                    market_regime TEXT,
                    high_touch_flag BOOLEAN,
                    summary TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP         
                );
            """)
            conn.commit()
        finally:
            if cursor:
                cursor.close()
            if conn:    
                conn.close()

    
    @task
    def fetch_fx_rates(ds=None):
        #อันนี้คือวิธีที่เราทำให้ projects ก่อนหน้านี้ คือเราไปหยิบ ds มาเอง
        #def fetch_fx_rates():
        #context = get_current_context() 
        #ds = context["ds"] หรือ context.get("ds")

        url = (
            f"https://api.frankfurter.dev/v1/{ds}"
            "?base=USD&symbols=EUR,GBP,JPY,CHF,CAD,AUD,NZD,MYR,THB"
        )

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        data = response.json()

        if "rates" not in data or not data["rates"]:
            raise AirflowSkipException(f"No FX rates returned for {ds}")

        market_pairs = {
            "EUR": {"pair": "EURUSD", "invert": True},
            "GBP": {"pair": "GBPUSD", "invert": True},
            "AUD": {"pair": "AUDUSD", "invert": True},
            "NZD": {"pair": "NZDUSD", "invert": True},
            "JPY": {"pair": "USDJPY", "invert": False},
            "CHF": {"pair": "USDCHF", "invert": False},
            "CAD": {"pair": "USDCAD", "invert": False},
            "MYR": {"pair": "USDMYR", "invert": False},
            "THB": {"pair": "USDTHB", "invert": False},
        }

        rows = []

        for currency, raw_rate in data["rates"].items():
            pair = market_pairs[currency]["pair"]

            #rate = 1 / raw_rate if market_pairs[currency]["invert"] else raw_rate
            #or สร้าง default : rate = raw_rate ก่อน if แล้วไม่ต้องเขียน else
            
            if market_pairs[currency]["invert"]: # or if market_pairs[currency]["invert"] == True:
                rate = 1 / raw_rate
            else:
                rate = raw_rate
            
            row = {
                "rate_date": data["date"],
                "pair": pair,
                "base_currency": pair[:3],
                "quote_currency": pair[3:],
                "rate": rate,
                "raw_api_base": data["base"],
                "raw_api_quote": currency,
                "raw_api_rate": raw_rate,
            }
            rows.append(row)
        
        return rows
             

    @task(outlets=[fx_rates_asset])
    def load_fx_rates(rows):
        conn = None
        cursor = None

        try:
            conn = get_connection()
            cursor = conn.cursor()

            for row in rows:
                cursor.execute("""
                    INSERT INTO fx_rates (
                        rate_date,
                        pair,
                        base_currency,
                        quote_currency,
                        rate,
                        raw_api_base,
                        raw_api_quote,
                        raw_api_rate           
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (rate_date, pair)
                    DO UPDATE SET
                        base_currency = EXCLUDED.base_currency,
                        quote_currency = EXCLUDED.quote_currency,
                        rate = EXCLUDED.rate,
                        raw_api_base = EXCLUDED.raw_api_base,
                        raw_api_quote = EXCLUDED.raw_api_quote,
                        raw_api_rate = EXCLUDED.raw_api_rate;
                """, (
                    row["rate_date"],
                    row["pair"],
                    row["base_currency"],
                    row["quote_currency"],
                    row["rate"],
                    row["raw_api_base"],
                    row["raw_api_quote"],
                    row["raw_api_rate"],
                ))
            conn.commit()
            print(f"Loaded {len(rows)} FX rate rows.")

        except Exception:
            if conn:
                conn.rollback()
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    
    log_run = BashOperator(
        task_id="log_run",
        bash_command="echo 'fx_ingest completed for {{ ds }}'",
    )

    
    created = create_tables()
    rates = fetch_fx_rates()
    loaded = load_fx_rates(rates)

    created >> rates >> loaded >> log_run

fx_ingest()