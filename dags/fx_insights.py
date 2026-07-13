from __future__ import annotations

from airflow.sdk import dag, task, Asset, get_current_context
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowSkipException


from datetime import datetime
import psycopg2
import os

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
    dag_id="fx_insights",
    start_date=datetime(2026, 7, 1),
    schedule=[fx_rates_asset],
    catchup=False,
    tags=["project4", "fx", "insight"],
)

def fx_insights():

    @task
    def calculate_daily_moves():
        conn = None
        cursor = None

        try:
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT MAX(rate_date)
                FROM fx_rates;
            """)
            current_date = cursor.fetchone()[0]

            if current_date is None:
                raise AirflowSkipException("No FX rates found in fx_rates")
            
            #วันล่าสุดที่น้อยกว่าวันปัจจุบัน < %s
            cursor.execute("""
                SELECT MAX(rate_date)
                FROM fx_rates
                WHERE rate_date < %s; 
            """, (current_date,))
            previous_date = cursor.fetchone()[0]

            if previous_date is None:
                raise AirflowSkipException(f"No previous FX rates found before {current_date}")
            
            cursor.execute("""
                SELECT 
                    current.rate_date,
                    current.pair,
                    current.rate AS current_rate,
                    previous.rate AS previous_rate
                FROM fx_rates current
                JOIN fx_rates previous
                    ON current.pair = previous.pair
                WHERE current.rate_date = %s
                    AND previous.rate_date = %s;
            """, (current_date, previous_date))

            rate_rows = cursor.fetchall()

            moves = []
            for rate_date, pair, current_rate, previous_rate in rate_rows:
                pct_change = ((current_rate - previous_rate) / previous_rate) * 100

                if pct_change > 0:
                    direction = "UP"
                elif pct_change < 0:
                    direction = "DOWN"
                else:
                    direction = "FLAT"

                if abs(pct_change) > 1:
                    volatility_flag = "HIGH"
                else:
                    volatility_flag = "NORMAL"

                row = {
                    "rate_date": rate_date.isoformat(),
                    "pair": pair,
                    "pct_change": str(pct_change),
                    "direction": direction,
                    "volatility_flag": volatility_flag, 
                }
                moves.append(row)

            if not moves:
                raise AirflowSkipException(
                    f"No matching FX pairs found between {current_date} and {previous_date}"
                )
            
            print(f"Calculated {len(moves)} daily move rows for {current_date}")

            return moves
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
            
                
    @task
    def load_daily_moves(moves):
        conn = None
        cursor = None

        try:
            conn = get_connection()
            cursor = conn.cursor()

            for item in moves:
                cursor.execute("""
                    INSERT INTO fx_daily_moves (
                        rate_date,
                        pair,
                        pct_change,
                        direction,
                        volatility_flag           
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (rate_date, pair)
                    DO UPDATE SET
                        pct_change = EXCLUDED.pct_change,
                        direction = EXCLUDED.direction,
                        volatility_flag = EXCLUDED.volatility_flag,
                        created_at = CURRENT_TIMESTAMP;
                """, (
                    item["rate_date"],
                    item["pair"],
                    item["pct_change"],
                    item["direction"],
                    item["volatility_flag"],
                ))
            conn.commit()
            print(f"Inserted {len(moves)} daily move rows")

        except Exception:
            if conn:
                conn.rollback()
            raise

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()


    @task
    def generate_market_insight(moves):

        if not moves:
            raise AirflowSkipException("No moves available to generate market insight")

        high_vol_count = len([item for item in moves if item["volatility_flag"] == "HIGH"])
        top_mover = max(moves, key=lambda item: abs(float(item["pct_change"])))
        market_regime = "HIGH_VOLATILITY" if high_vol_count >= 3 else "NORMAL"
        high_touch_flag = market_regime == "HIGH_VOLATILITY"
        
        if high_touch_flag:
            summary = f"{top_mover['pair']} was the top mover today with a {float(top_mover['pct_change']):.2f}% move. Market volatility is elevated." 
        else:
            summary = f"{top_mover['pair']} was the top mover today with a {float(top_mover['pct_change']):.2f}% move. Market volatility remains normal."
        insight = {
            "insight_date": moves[0]["rate_date"],
            "top_mover_pair": top_mover["pair"],
            "top_mover_pct": top_mover["pct_change"],
            "market_regime": market_regime,
            "high_touch_flag": high_touch_flag,
            "summary": summary
        }

        return insight
    

    @task
    def load_market_insight(insight):
        conn = None
        cursor = None

        try:
            conn = get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO fx_insights (
                    insight_date,
                    top_mover_pair,
                    top_mover_pct,
                    market_regime,
                    high_touch_flag,
                    summary           
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (insight_date)
                DO UPDATE SET
                    top_mover_pair = EXCLUDED.top_mover_pair,
                    top_mover_pct = EXCLUDED.top_mover_pct,
                    market_regime = EXCLUDED.market_regime,
                    high_touch_flag = EXCLUDED.high_touch_flag,
                    summary = EXCLUDED.summary,
                    created_at = CURRENT_TIMESTAMP;
            """, (
                insight["insight_date"],
                insight["top_mover_pair"],
                insight["top_mover_pct"],
                insight["market_regime"],
                insight["high_touch_flag"],
                insight["summary"],
            ))
            conn.commit()
            print(f"Loaded market insight for {insight['insight_date']} into fx_insights")

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
        bash_command ="echo 'fx_insights completed successfully'",
    )


    moves = calculate_daily_moves()
    loaded_moves = load_daily_moves(moves)
    insight = generate_market_insight(moves)
    loaded_insight = load_market_insight(insight)

    [loaded_moves, loaded_insight] >> log_run

fx_insights()