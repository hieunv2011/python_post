import os
import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
import requests
import time  # thêm time để giới hạn 1s giữa các POST

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

# ===== Hàm POST dữ liệu =====
def post_data(url, record, jwt_token, with_image=False):
    headers = {"Authorization": f"Bearer {jwt_token}"}
    body_fields = [
        str(record["timestamp"]), record["lat"], record["lng"],
        record["velocity"], record["distance"], record["distance2"],
        record["direction"], record["session_id"], record["session_state"], record["face_id"]
    ]
    body = " ".join(body_fields)
    if with_image:
        print(f"📤 POST CÓ ảnh: {body}")
    else:
        print(f"📤 POST KHÔNG ảnh: {body}")

    try:
        response = requests.post(url, headers=headers, data=body)
        if response.status_code == 200:
            print("✅ POST thành công")
        else:
            print(f"⚠️ POST lỗi: {response.status_code} {response.text}")
    except Exception as e:
        print(f"❌ POST thất bại: {e}")

# ===== Update face_image =====
def update_face_image(records_with_image):
    if not records_with_image:
        return
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(DB_HOST, DB_PORT)
        ) as tunnel:
            local_port = tunnel.local_bind_port
            conn = psycopg2.connect(
                host='127.0.0.1', port=local_port,
                user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME
            )
            try:
                with conn.cursor() as cur:
                    for r in records_with_image:
                        event_date = datetime.utcfromtimestamp(r["timestamp"]).strftime("%Y-%m-%d %H:%M:%S")
                        sql = """
                        UPDATE trainee_outdoor_gps_logs
                        SET face_image = %s
                        WHERE session_id = %s
                          AND event_date = %s
                          AND session_state = 1
                          AND face_id = 1
                        """
                        cur.execute(sql, (r["face_image"], r["session_id"], event_date))
                        print(f"✅ UPDATE face_image session_id={r['session_id']} event_date={event_date}")
                    conn.commit()
            finally:
                conn.close()
    except Exception as e:
        print("❌ Kết nối hoặc update thất bại:", e)

# ===== Main =====
def main():
    file_path = os.path.join(os.path.dirname(__file__), "test.log")
    lines = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            parsed = parse_line(parts)
            if parsed:
                lines.append(parsed)

    lines.sort(key=lambda x: x["timestamp"])

    if not lines:
        print("❌ Không có bản ghi trong file test.log")
        return

    # Lấy URL từ bản ghi đầu tiên
    first = lines[0]
    url = f"https://jira.shlx.vn/v1/logs?sn={first['sn']}&iid={first['iid']}&tid={first['tid']}&v={first['v']}"
    print(f"# POST URL: {url}")
    print(f"# Tổng số bản ghi: {len(lines)}\n")

    records_with_image = []
    for r in lines:
        if r["face_image"]:
            post_data(url, r, JWT_TOKEN, with_image=True)
            records_with_image.append(r)
        else:
            post_data(url, r, JWT_TOKEN, with_image=False)
        time.sleep(1)  # giới hạn 1 giây giữa các POST

    update_face_image(records_with_image)

if __name__ == "__main__":
    main()
