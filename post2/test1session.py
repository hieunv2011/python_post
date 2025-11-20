import os
import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
import requests
import time

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

# ===== JWT cố định =====
JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjI2NywiZXhwIjoxNzY0NTYxODE0fQ.9BLpbjJJyPFr1yHMLJZhXnIrdZi2ncyhLxoKVGe0b2c"

# ===== Session cần test =====
TEST_SESSION_ID = "27ca366e-99da-4da3-b47e-0e65d6c494eb"

# ===== Hàm parse log =====
def parse_line(parts):
    if len(parts) < 11:
        return None
    return {
        "sn": parts[0],
        "v": parts[1],
        "iid": parts[2],
        "tid": parts[3],
        "timestamp": int(parts[5]),
        "lat": parts[6],
        "lng": parts[7],
        "velocity": parts[8],
        "distance": parts[9],
        "distance2": parts[10],
        "direction": parts[11] if len(parts) > 11 else "0",
        "session_id": parts[4],
        "session_state": parts[12] if len(parts) > 12 else "1",
        "face_id": parts[13] if len(parts) > 13 else "1",
        "face_image": " ".join(parts[14:]) if len(parts) > 14 else ""
    }

# ===== Hàm POST batch =====
def post_data_batch(url, records, dry_run=False):
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    body_lines = []

    for r in records:
        body_fields = [
            str(r["timestamp"]), r["lat"], r["lng"],
            r["velocity"], r["distance"], r["distance2"],
            r["direction"], r["session_id"], r["session_state"], r["face_id"]
        ]
        body_lines.append(" ".join(body_fields))

    body = "\n".join(body_lines)

    print("\n--- POST BATCH ---")
    print("URL:", url)
    print("BODY:\n", body)

    if dry_run:
        print("[DRY-RUN] Không gửi thật.")
        return

    try:
        response = requests.post(url, headers=headers, data=body)
        print("Kết quả:", response.status_code, response.text)
    except Exception as e:
        print("Lỗi POST:", e)

# ===== Update face_image =====
def update_face_image(records_with_image, cur=None, dry_run=False):
    if not records_with_image or cur is None:
        print("\nKhông có bản ghi nào có ảnh để UPDATE.")
        return

    print(f"\n=== BẮT ĐẦU UPDATE ẢNH ({len(records_with_image)} bản ghi) ===")

    for r in records_with_image:
        event_date = datetime.utcfromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        sql = """
            UPDATE trainee_outdoor_gps_logs
            SET face_image = %s
            WHERE session_id = %s
              AND event_date = %s
              AND face_id = 1
        """

        print("\n--- UPDATE ---")
        print("SQL:", sql.replace("\n", " "))
        print("PARAMS:", (r["face_image"], r["session_id"], event_date))

        if dry_run:
            print("[DRY-RUN] Không UPDATE thật.")
        else:
            cur.execute(sql, (r["face_image"], r["session_id"], event_date))
            print("UPDATE OK")

# ===== Main =====
def main():
    file_path = os.path.join(os.path.dirname(__file__), "test07.log")

    # Đọc log
    lines = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            parsed = parse_line(parts)
            if parsed and parsed["session_id"] == TEST_SESSION_ID:
                lines.append(parsed)

    if not lines:
        print("❌ Không tìm thấy session cần test")
        return

    lines.sort(key=lambda x: x["timestamp"])

    total = len(lines)
    with_image = sum(1 for r in lines if r["face_image"])
    no_image = total - with_image

    print("\n========================")
    print("SESSION KIỂM TRA:", TEST_SESSION_ID)
    print("Tổng bản ghi:", total)
    print("Có ảnh:", with_image)
    print("Không ảnh:", no_image)
    print("========================\n")

    confirm = input("Gửi POST và UPDATE thật? (y/n): ").strip().lower()
    dry_run = confirm != "y"

    # ==== SSH TUNNEL + DB ====
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
                first = lines[0]
                url = f"https://jira.shlx.vn/v1/logs?sn={first['sn']}&iid={first['iid']}&tid={first['tid']}&v={first['v']}"

                # POST toàn bộ session 1 lần
                post_data_batch(url, lines, dry_run=dry_run)

                # Chỉ những record có face_image mới UPDATE ảnh
                records_with_image = [r for r in lines if r["face_image"]]
                update_face_image(records_with_image, cur, dry_run=dry_run)

                if not dry_run:
                    conn.commit()

        finally:
            conn.close()


if __name__ == "__main__":
    main()
