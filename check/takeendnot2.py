import os
import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder

# ===== Patch paramiko DSSKey cho sshtunnel =====
if not hasattr(paramiko, 'DSSKey'):
    try:
        from paramiko.dsskey import DSSKey as _DSSKey
    except Exception:
        try:
            from paramiko.pkey import DSSKey as _DSSKey
        except Exception:
            _DSSKey = None
    if _DSSKey:
        paramiko.DSSKey = _DSSKey
    else:
        try:
            from paramiko.pkey import PKey as _BasePKey
        except Exception:
            _BasePKey = object
        class DSSKey(_BasePKey):
            pass
        paramiko.DSSKey = DSSKey

# ===== Cấu hình SSH & PostgreSQL =====
SSH_HOST = 'dat.shlx.vn'
SSH_PORT = 22
SSH_USER = 'root'
SSH_PASSWORD = 'R6lC%*sDpd7u'

DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'shlx'
DB_PASSWORD = '123456'
DB_NAME = 'shlx'

# ===== Lấy session_id từ log =====
def get_session_ids_from_log(log_path):
    session_ids = set()
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 5:
                session_ids.add(parts[4])
    return session_ids

# ===== Lấy danh sách session_id không phải state=2 =====
def query_sessions_not2(session_ids):
    results = []
    sql = "SELECT session_id FROM trainee_outdoor_sessions WHERE session_id=%s AND state!=2"
    
    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASSWORD,
        remote_bind_address=(DB_HOST, DB_PORT)
    ) as tunnel:
        conn = psycopg2.connect(
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        try:
            with conn.cursor() as cur:
                for sid in session_ids:
                    cur.execute(sql, (sid,))
                    row = cur.fetchone()
                    if row:
                        results.append(row[0])
        finally:
            conn.close()
    return results

# ===== Ghi file TXT chỉ có session_id =====
def write_output(results, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        for sid in results:
            f.write(f"{sid}\n")

# ===== MAIN =====
def main():
    log_path = os.path.join(os.path.dirname(__file__), "test09.log")
    session_ids = get_session_ids_from_log(log_path)
    print(f"Tìm thấy {len(session_ids)} session_id trong log.")

    results_not2 = query_sessions_not2(session_ids)
    print(f"Tìm thấy {len(results_not2)} session không phải state=2.")

    base_name = os.path.splitext(os.path.basename(log_path))[0]
    output_path = os.path.join(os.path.dirname(__file__), f"{base_name}_not2.txt")
    write_output(results_not2, output_path)
    print("XONG. File xuất:", output_path)

if __name__ == "__main__":
    main()
