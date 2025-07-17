import pandas as pd
import psycopg2
from datetime import datetime
from config import DB_URI, EXPORT_PATH

def log_to_db(conn, process_name, start_time, end_time, row_countt, commentt, affected_rows=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO logs.etl_log (process_name, start_time, end_time, row_countt, commentt, affected_rows)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (process_name, start_time, end_time, row_countt, commentt, affected_rows))
        conn.commit()

if __name__ == "__main__":
    start_time = datetime.now()
    process_name = "export_f101_to_csv"

    try:
        conn = psycopg2.connect(DB_URI)
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM dm.dm_f101_round_f")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=columns)

        row_count = len(df)
        df.to_csv(EXPORT_PATH, index=False)

        end_time = datetime.now()
        log_to_db(conn, process_name, start_time, end_time, row_count, "Экспорт успешно завершен", affected_rows=row_count)
        conn.close()

        print("Экспорт завершён")

    except Exception as e:
        end_time = datetime.now()
        try:
            conn = psycopg2.connect(DB_URI)
            log_to_db(conn, process_name, start_time, end_time, 0, f"Ошибка экспорта: {str(e)}", affected_rows=0)
            conn.close()
        except Exception as log_err:
            print("Ошибка при логировании:", log_err)
        print("Ошибка при экспорте:", str(e))
