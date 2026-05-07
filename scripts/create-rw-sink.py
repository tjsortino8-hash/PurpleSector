import psycopg2
import time

for i in range(10):
    try:
        conn = psycopg2.connect(host='localhost', port=4566, user='root', dbname='dev', connect_timeout=5)
        break
    except Exception:
        time.sleep(1)
        if i == 9:
            print('⚠ Could not connect to RisingWave, skipping sink creation')
            exit(0)

cur = conn.cursor()

cur.execute("SHOW SINKS;")
sinks = [r[0] for r in cur.fetchall()]
if 'public.laps_postgres_upsert' in sinks or 'laps_postgres_upsert' in sinks:
    print('✓ Sink already exists')
    conn.close()
    exit(0)

try:
    cur.execute("""
CREATE SINK IF NOT EXISTS laps_postgres_upsert
FROM completed_laps
WITH (
  connector = 'jdbc',
  jdbc.url = 'jdbc:postgresql://postgres:5432/purplesector',
  user = 'purplesector',
  password = 'devpassword',
  table.name = 'laps',
  type = 'append-only',
  force_append_only = 'true'
);
""")
    conn.commit()
    print('✓ RisingWave sink created')
except Exception as e:
    print(f'⚠ Failed to create sink: {e}')

conn.close()
