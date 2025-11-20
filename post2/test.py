# import os
# import paramiko
# import psycopg2
# from sshtunnel import SSHTunnelForwarder
# from datetime import datetime
# import requests
# import time

# # ===== Patch paramiko DSSKey cho sshtunnel =====
# if not hasattr(paramiko, 'DSSKey'):
#     try:
#         from paramiko.dsskey import DSSKey as _DSSKey
#     except Exception:
#         try:
#             from paramiko.pkey import DSSKey as _DSSKey
#         except Exception:
#             _DSSKey = None
#     if _DSSKey is not None:
#         paramiko.DSSKey = _DSSKey
#     else:
#         try:
#             from paramiko.pkey import PKey as _BasePKey
#         except Exception:
#             _BasePKey = object
#         class DSSKey(_BasePKey):
#             pass
#         paramiko.DSSKey = DSSKey

# # ===== Cấu hình SSH & PostgreSQL =====
# SSH_HOST = 'dat.shlx.vn'
# SSH_PORT = 22
# SSH_USER = 'root'
# SSH_PASSWORD = 'R6lC%*sDpd7u'
# DB_HOST = 'localhost'
# DB_PORT = 5432
# DB_USER = 'shlx'
# DB_PASSWORD = '123456'
# DB_NAME = 'shlx'

# # ===== JWT cố định =====
# JWT_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOjI2NywiZXhwIjoxNzY0NTYxODE0fQ.9BLpbjJJyPFr1yHMLJZhXnIrdZi2ncyhLxoKVGe0b2c"

# PROCESS_FILE = os.path.join(os.path.dirname(__file__), "process.txt")

# # ===== Hàm parse log =====
# def parse_line(parts):
#     if len(parts) < 11:
#         return None
#     return {
#         "sn": parts[0],
#         "v": parts[1],
#         "iid": parts[2],
#         "tid": parts[3],
#         "timestamp": int(parts[5]),
#         "lat": parts[6],
#         "lng": parts[7],
#         "velocity": parts[8],
#         "distance": parts[9],
#         "distance2": parts[10],
#         "direction": parts[11] if len(parts) > 11 else "0",
#         "session_id": parts[4],
#         "session_state": parts[12] if len(parts) > 12 else "1",
#         "face_id": parts[13] if len(parts) > 13 else "1",
#         "face_image": " ".join(parts[14:]) if len(parts) > 14 else ""
#     }

# # ===== Hàm POST batch cho 1 session =====
# def post_session(url, records, dry_run=False):
#     headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
#     body_lines = []

#     for r in records:
#         # Không gửi face_image trong POST
#         body_fields = [
#             str(r["timestamp"]), r["lat"], r["lng"],
#             r["velocity"], r["distance"], r["distance2"],
#             r["direction"], r["session_id"], r["session_state"], r["face_id"]
#         ]
#         body_lines.append(" ".join(body_fields))

#     body = "\n".join(body_lines)
#     print(f"\n--- POST session ({len(records)} bản ghi) ---")
#     print("URL:", url)
#     print("BODY:\n", body)

#     if dry_run:
#         print("[DRY-RUN] Không gửi POST thật.")
#         return

#     try:
#         response = requests.post(url, headers=headers, data=body)
#         if response.status_code == 200:
#             print("✅ POST thành công")
#         else:
#             print(f"⚠️ POST lỗi: {response.status_code} {response.text}")
#     except Exception as e:
#         print(f"❌ POST thất bại: {e}")

# # ===== Update face_image =====
# def update_face_image(records_with_image, cur=None, dry_run=False):
#     if not records_with_image:
#         print("\nKhông có bản ghi nào có ảnh để UPDATE.")
#         return

#     print(f"\n=== BẮT ĐẦU UPDATE ẢNH ({len(records_with_image)} bản ghi) ===")
#     for r in records_with_image:
#         event_date = datetime.utcfromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
#         sql = """
#         UPDATE trainee_outdoor_gps_logs
#         SET face_image = %s
#         WHERE session_id = %s
#           AND event_date = %s
#           AND face_id = 1
#         """
#         if dry_run:
#             print(f"[DRY-RUN] UPDATE face_image session_id={r['session_id']} event_date={event_date}")
#         else:
#             cur.execute(sql, (r["face_image"], r["session_id"], event_date))
#             print(f"✅ UPDATE face_image session_id={r['session_id']} event_date={event_date}")

# # ===== Main =====
# def main():
#     # Đọc log
#     file_path = os.path.join(os.path.dirname(__file__), "test.log")
#     lines = []
#     with open(file_path, "r", encoding="utf-8") as f:
#         for line in f:
#             parts = line.strip().split()
#             parsed = parse_line(parts)
#             if parsed:
#                 lines.append(parsed)
#     if not lines:
#         print("❌ Không có bản ghi nào trong file log")
#         return

#     # Nhóm theo session
#     sessions = {}
#     for r in lines:
#         sessions.setdefault(r["session_id"], []).append(r)

#     # Load session đã post
#     processed_sessions = set()
#     if os.path.exists(PROCESS_FILE):
#         with open(PROCESS_FILE, "r") as f:
#             processed_sessions = set(line.strip() for line in f if line.strip())

#     confirm = input("Bạn có muốn thực sự POST và UPDATE tất cả dữ liệu không? (y/n): ").strip().lower()
#     dry_run = confirm != 'y'

#     with SSHTunnelForwarder(
#         (SSH_HOST, SSH_PORT),
#         ssh_username=SSH_USER,
#         ssh_password=SSH_PASSWORD,
#         remote_bind_address=(DB_HOST, DB_PORT)
#     ) as tunnel:
#         local_port = tunnel.local_bind_port
#         conn = psycopg2.connect(
#             host='127.0.0.1', port=local_port,
#             user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
#         )
#         try:
#             with conn.cursor() as cur:
#                 total_sessions = len(sessions)
#                 for idx, (sid, records) in enumerate(sessions.items(), start=1):
#                     if sid in processed_sessions:
#                         print(f"⚠️ Bỏ qua session đã xử lý: {sid}")
#                         continue

#                     records.sort(key=lambda x: x["timestamp"])
#                     first = records[0]
#                     url = f"https://jira.shlx.vn/v1/logs?sn={first['sn']}&iid={first['iid']}&tid={first['tid']}&v={first['v']}"
#                     remaining = total_sessions - idx
#                     print(f"\n=== Bắt đầu session {sid} ({idx}/{total_sessions}), còn {remaining} session nữa ===")

#                     # POST toàn bộ session 1 lần
#                     post_session(url, records, dry_run=dry_run)

#                     # Lấy record có ảnh để UPDATE
#                     records_with_image = [r for r in records if r["face_image"]]
#                     update_face_image(records_with_image, cur=cur, dry_run=dry_run)

#                     # Ghi session đã post
#                     with open(PROCESS_FILE, "a") as f:
#                         f.write(sid + "\n")

#                     print(f"=== Hoàn tất session {sid} ===\n")

#                     # Delay giữa các session
#                     time.sleep(0.5)

#                 if not dry_run:
#                     conn.commit()
#         finally:
#             conn.close()

# if __name__ == "__main__":
#     main()

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
    print(f"\n--- POST BATCH ({len(records)} bản ghi) ---")
    print("URL:", url)
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
    file_path = os.path.join(os.path.dirname(__file__), "test.log")
    session_file = os.path.join(os.path.dirname(__file__), "session_list.txt")

    # Đọc tất cả session từ session_list.txt
    with open(session_file, "r", encoding="utf-8") as f:
        sessions = [line.strip() for line in f if line.strip()]

    print(f"Tổng số session trong file: {len(sessions)}")

    # SSH + DB
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
                # Vòng lặp qua từng session
                total_sessions = len(sessions)
                for idx, session_id in enumerate(sessions, 1):
                    print(f"\n[{idx}/{total_sessions}] Xử lý session: {session_id}")

                    # Đọc log file và lọc ra session hiện tại
                    lines = []
                    with open(file_path, "r", encoding="utf-8") as f_log:
                        for line in f_log:
                            parts = line.strip().split()
                            parsed = parse_line(parts)
                            if parsed and parsed["session_id"] == session_id:
                                lines.append(parsed)

                    if not lines:
                        print("❌ Không tìm thấy bản ghi cho session này")
                        continue

                    lines.sort(key=lambda x: x["timestamp"])

                    first = lines[0]
                    url = f"https://jira.shlx.vn/v1/logs?sn={first['sn']}&iid={first['iid']}&tid={first['tid']}&v={first['v']}"

                    print(f"--- POST dữ liệu session {session_id} ---")
                    post_data_batch(url, lines, dry_run=False)

                    # Chỉ update những record có face_image
                    records_with_image = [r for r in lines if r["face_image"]]
                    if records_with_image:
                        print(f"--- UPDATE ảnh session {session_id} ---")
                        update_face_image(records_with_image, cur, dry_run=False)

                    conn.commit()
                    print(f"✅ Hoàn tất session {session_id}, còn lại {total_sessions - idx} session\n")

        finally:
            conn.close()


if __name__ == "__main__":
    main()
