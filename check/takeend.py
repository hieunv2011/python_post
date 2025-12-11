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


# ===== Đọc session_id từ log =====
def get_session_ids_from_log(log_path):
    session_ids = set()
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 6:
                continue
            session_ids.add(parts[4])
    return session_ids


# ===== Query database cho mọi state =====
def query_sessions(session_ids):
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
        GROUP BY tos.session_id, tos.state, b.name
    """

    results_state_1 = []
    results_state_2 = []
    other_state = []          # state khác 1,2
    missing_sessions = []     # có trong log nhưng không có trong DB

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
                    rows = cur.fetchall()

                    if not rows:
                        missing_sessions.append(sid)
                        continue

                    for r in rows:
                        state = r[1]
                        data = {
                            "session_id": r[0],
                            "state": r[1],
                            "branch_name": r[2],
                            "log_count": r[3],
                            "last_timestamp": r[4]
                        }

                        if state == 1:
                            results_state_1.append(data)
                        elif state == 2:
                            results_state_2.append(data)
                        else:
                            other_state.append(data)

        finally:
            conn.close()

    return results_state_1, results_state_2, other_state, missing_sessions


# ===== Xuất file TXT theo nhóm =====
def write_output(results_state_1, results_state_2, other_state, missing_sessions, output_path):
    with open(output_path, "w", encoding="utf-8") as f:

        # Tổng kết
        f.write(f"Tổng session STATE = 1: {len(results_state_1)}\n")
        f.write(f"Tổng session STATE = 2: {len(results_state_2)}\n")
        f.write(f"Tổng session STATE khác (không phải 1,2): {len(other_state)}\n")
        f.write(f"Tổng session KHÔNG có trong DB: {len(missing_sessions)}\n")
        f.write("\n=============================\n")

        # STATE = 1
        f.write("====== STATE = 1 ======\n")
        f.write("=============================\n\n")
        f.write("session_id | branch_name | log_count | last_timestamp\n")
        for r in sorted(results_state_1, key=lambda x: x["branch_name"].lower()):
            f.write(f"{r['session_id']} | {r['branch_name']} | {r['log_count']} | {r['last_timestamp']}\n")

        # STATE = 2
        f.write("\n=============================\n")
        f.write("====== STATE = 2 ======\n")
        f.write("=============================\n\n")
        f.write("session_id | branch_name | log_count | last_timestamp\n")
        for r in sorted(results_state_2, key=lambda x: x["branch_name"].lower()):
            f.write(f"{r['session_id']} | {r['branch_name']} | {r['log_count']} | {r['last_timestamp']}\n")

        # STATE khác
        if other_state:
            f.write("\n=============================\n")
            f.write("====== STATE KHÁC ======\n")
            f.write("=============================\n\n")
            f.write("session_id | state | branch_name | log_count | last_timestamp\n")
            for r in sorted(other_state, key=lambda x: x["branch_name"].lower()):
                f.write(f"{r['session_id']} | {r['state']} | {r['branch_name']} | {r['log_count']} | {r['last_timestamp']}\n")

        # Missing
        if missing_sessions:
            f.write("\n=============================\n")
            f.write("====== KHÔNG TỒN TẠI TRONG DB ======\n")
            f.write("=============================\n\n")
            for sid in sorted(missing_sessions):
                f.write(f"{sid}\n")


# ===== MAIN =====
def main():
    log_path = os.path.join(os.path.dirname(__file__), "test09.log")
    output_path = os.path.join(os.path.dirname(__file__), "sessions_full_report.txt")

    print("Đang đọc session_id từ log...")
    session_ids = get_session_ids_from_log(log_path)
    print(f"Tìm thấy {len(session_ids)} session_id")

    print("Đang query database...")
    results_state_1, results_state_2, other_state, missing_sessions = query_sessions(session_ids)

    print(f"STATE 1: {len(results_state_1)} session")
    print(f"STATE 2: {len(results_state_2)} session")
    print(f"STATE KHÁC: {len(other_state)} session")
    print(f"KHÔNG TỒN TẠI TRONG DB: {len(missing_sessions)} session")

    print("Đang ghi file output...")
    write_output(results_state_1, results_state_2, other_state, missing_sessions, output_path)

    print("XONG. Xuất file:", output_path)


if __name__ == "__main__":
    main()
