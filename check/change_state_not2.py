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

# ===== SSH + PostgreSQL config =====
SSH_HOST = 'dat.shlx.vn'
SSH_PORT = 22
SSH_USER = 'root'
SSH_PASSWORD = 'R6lC%*sDpd7u'

DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'shlx'
DB_PASSWORD = '123456'
DB_NAME = 'shlx'

# ===== Đọc session_id từ file =====
def read_session_ids(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

# ===== Update nhiều session =====
def update_sessions_state(session_ids):
    if not session_ids:
        print("❌ Không có session_id nào để update.")
        return

    sql_update = """
        UPDATE trainee_outdoor_sessions
        SET state = -1,
            archived_url = ''
        WHERE session_id = ANY(%s)
    """

    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASSWORD,
        remote_bind_address=(DB_HOST, DB_PORT)
    ) as tunnel:

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=tunnel.local_bind_port,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )

        try:
            with conn.cursor() as cur:
                cur.execute(sql_update, (session_ids,))
                updated_count = cur.rowcount
                conn.commit()
                print(f"✅ UPDATED {updated_count} record(s) for {len(session_ids)} session_id(s)")
        finally:
            conn.close()

# ===== MAIN =====
def main():
    file_path = os.path.join(os.path.dirname(__file__), "test09_not2.txt")
    session_ids = read_session_ids(file_path)
    print(f"Tìm thấy {len(session_ids)} session_id trong file.")
    update_sessions_state(session_ids)

if __name__ == "__main__":
    main()
