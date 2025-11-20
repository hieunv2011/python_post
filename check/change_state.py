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
    if _DSSKey is not None:
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

# ===== Lấy các session_id từ SELECT gốc =====
def get_sessions_to_update(session_ids):
    results = []
    sql = """
        SELECT 
            tos.session_id,
            tos.state
        FROM trainee_outdoor_sessions tos
        WHERE tos.session_id = %s
          AND tos.state = 2
    """
    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASSWORD,
        remote_bind_address=(DB_HOST, DB_PORT)
    ) as tunnel:
        local_port = tunnel.local_bind_port
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=local_port,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        try:
            with conn.cursor() as cur:
                for sid in session_ids:
                    cur.execute(sql, (sid,))
                    rows = cur.fetchall()
                    for r in rows:
                        results.append(r[0])  # session_id
        finally:
            conn.close()
    return results

# ===== Update state và archived_url =====
def update_state(session_ids):
    if not session_ids:
        print("Không có session nào để update.")
        return

    sql_update = """
        UPDATE trainee_outdoor_sessions
        SET state = 1,
            archived_url = ''
        WHERE session_id = %s
    """

    print(f"Tổng {len(session_ids)} session sẽ update state=1 và archived_url=''")
    confirm = input("Bạn có muốn tiếp tục? (y/n): ").strip().lower()
    if confirm != "y":
        print("Hủy thao tác update.")
        return

    with SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER,
        ssh_password=SSH_PASSWORD,
        remote_bind_address=(DB_HOST, DB_PORT)
    ) as tunnel:
        local_port = tunnel.local_bind_port
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=local_port,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        try:
            with conn.cursor() as cur:
                for sid in session_ids:
                    cur.execute(sql_update, (sid,))
                    print(f"✅ session_id={sid} → state=1, archived_url=''")
                conn.commit()
        finally:
            conn.close()

# ===== Main =====
def main():
    log_path = os.path.join(os.path.dirname(__file__), "test07.log")
    session_ids_all = set()

    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 6:
                continue
            session_ids_all.add(parts[4])

    print(f"Tìm thấy {len(session_ids_all)} session_id từ log.")

    session_ids_to_update = get_sessions_to_update(session_ids_all)
    print(f"Tổng {len(session_ids_to_update)} session_id sẽ được update.")

    update_state(session_ids_to_update)

if __name__ == "__main__":
    main()
