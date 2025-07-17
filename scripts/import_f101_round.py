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
    process_name = "import_f101_from_csv"

    try:
        df = pd.read_csv(EXPORT_PATH)
        row_count = len(df)

        conn = psycopg2.connect(DB_URI)
        cur = conn.cursor()

        cur.execute("DELETE FROM dm.dm_f101_round_f_v2")
        conn.commit()

        import io
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cur.copy_expert(f"""
            COPY dm.dm_f101_round_f_v2 ({','.join(df.columns)})
            FROM STDIN WITH CSV
        """, buffer)

        conn.commit()
        end_time = datetime.now()
        log_to_db(conn, process_name, start_time, end_time, row_count, "Импорт успешно завершен", affected_rows=row_count)
        conn.close()

        print("Импорт завершён")

    except Exception as e:
        end_time = datetime.now()
        try:
            conn = psycopg2.connect(DB_URI)
            log_to_db(conn, process_name, start_time, end_time, 0, f"Ошибка импорта: {str(e)}", affected_rows=0)
            conn.close()
        except:
            print("Ошибка при логировании")
        print("Ошибка при импорте:", str(e))
