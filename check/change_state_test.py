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

# ===== Update state cho session cụ thể =====
def update_single_session_state(session_id):
    sql_update = "UPDATE trainee_outdoor_sessions SET state = 1 WHERE session_id = %s AND state = 2"

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
                cur.execute(sql_update, (session_id,))
                updated_count = cur.rowcount
                conn.commit()
                print(f"✅ session_id={session_id} đã được update state=1. Số bản ghi thay đổi: {updated_count}")
        finally:
            conn.close()

# ===== Main =====
def main():
    target_session_id = "5cd66632-40bf-496e-a74d-ee33253a93ba"
    update_single_session_state(target_session_id)

if __name__ == "__main__":
    main()
