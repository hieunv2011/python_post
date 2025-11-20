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

# ===== Đọc session_id từ log với lọc timestamp <= 1763361120 =====
# def get_session_ids_from_log(log_path, max_timestamp=1763361120):
#     session_ids = set()
#     with open(log_path, "r", encoding="utf-8") as f:
#         for line in f:
#             parts = line.strip().split()
#             if len(parts) < 6:
#                 continue
#             try:
#                 timestamp = int(parts[5])
#             except ValueError:
#                 continue
#             if timestamp <= max_timestamp:
#                 session_ids.add(parts[4])
#     return session_ids

def get_session_ids_from_log(log_path):
    session_ids = set()
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 6:
                continue
            # Không cần kiểm tra timestamp
            session_ids.add(parts[4])
    return session_ids


# ===== Query database với count logs và last timestamp =====
def query_sessions(session_ids):
    results = []
    sql = """
        SELECT 
            tos.session_id,
            tos.state,
            b.name AS branch_name,
            COUNT(tol.id) AS log_count,
            MAX(tol.event_date) AS last_timestamp
        FROM trainee_outdoor_sessions tos
        JOIN trainees t ON t.id = tos.trainee_id
        JOIN courses c ON c.id = t.course_id
        JOIN branches b ON b.id = c.branch_id
        LEFT JOIN trainee_outdoor_gps_logs tol ON tol.session_id = tos.session_id
        WHERE tos.session_id = %s
          AND tos.state = 2
        GROUP BY tos.session_id, tos.state, b.name
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
                        results.append({
                            "session_id": r[0],
                            "state": r[1],
                            "branch_name": r[2],
                            "log_count": r[3],
                            "last_timestamp": r[4]
                        })
        finally:
            conn.close()
    return results

# ===== Ghi file txt với sắp xếp theo tên trung tâm alphabet =====
def write_output(results, output_path):
    results_sorted = sorted(results, key=lambda x: x['branch_name'].lower())
    with open(output_path, "w", encoding="utf-8") as f:
        # Header
        f.write("session_id | branch_name | state | log_count | last_timestamp\n")
        for r in results_sorted:
            f.write(f"{r['session_id']} | {r['branch_name']} | {r['state']} | {r['log_count']} | {r['last_timestamp']}\n")

# ===== Main =====
def main():
    log_path = os.path.join(os.path.dirname(__file__), "test07.log")
    output_path = os.path.join(os.path.dirname(__file__), "sessions_state_2.txt")

    print("Đang đọc session_id từ log...")
    session_ids = get_session_ids_from_log(log_path)
    print(f"Tìm thấy {len(session_ids)} session_id")

    print("Đang query database...")
    results = query_sessions(session_ids)
    print(f"Tìm thấy {len(results)} session có state = 2.")

    print("Đang ghi file output...")
    write_output(results, output_path)

    print(f"HOÀN TẤT. File xuất: {output_path}")

if __name__ == "__main__":
    main()
