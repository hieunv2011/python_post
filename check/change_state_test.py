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


# ===== Update 1 session duy nhất =====
def update_single_session_state(session_id):

    sql_select = """
        SELECT session_id, state, archived_url
        FROM trainee_outdoor_sessions
        WHERE session_id = %s
    """

    sql_update = """
        UPDATE trainee_outdoor_sessions
        SET state = 1,
            archived_url = ''
        WHERE session_id = %s AND state = 2
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

                # ---- Log trước update
                cur.execute(sql_select, (session_id,))
                before = cur.fetchone()
                print("🔎 BEFORE UPDATE:", before)

                # ---- Thực hiện update
                cur.execute(sql_update, (session_id,))
                updated_count = cur.rowcount
                conn.commit()

                print(f"✅ UPDATED {updated_count} record(s) for session_id={session_id}")

                # ---- Log sau update
                cur.execute(sql_select, (session_id,))
                after = cur.fetchone()
                print("📌 AFTER UPDATE:", after)

        finally:
            conn.close()


# ===== Main =====
def main():
    target_session_id = "449d9e90-5192-4772-a993-57ec2a748ed3"
    update_single_session_state(target_session_id)

if __name__ == "__main__":
    main()
