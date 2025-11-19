import importlib
import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder
from datetime import datetime

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

# ===== Cấu hình SSH =====
SSH_HOST = 'dat.shlx.vn'
SSH_PORT = 22
SSH_USER = 'root'
SSH_PASSWORD = 'R6lC%*sDpd7u'

# ===== Cấu hình PostgreSQL =====
DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'shlx'
DB_PASSWORD = '123456'
DB_NAME = 'shlx'

# ===== Dữ liệu input dạng dòng =====
input_data = """1763362609 10.659149483333332 106.46513661666668 3200 264 679 317 aa0b4bc7-d584-4dee-ac3e-58202ca80f66 1 1 /2025/11/17/576802/aa0b4bc7-d584-4dee-ac3e-58202ca80f66/999d2d43f4ed4c889e3a32847051c7aa.jpg"""

def parse_input_line(line):
    parts = line.strip().split()
    if len(parts) < 11:  # giảm từ 14 xuống 11
        return None
    return {
        "timestamp": int(parts[0]),
        "lat": parts[1],
        "lng": parts[2],
        "velocity": parts[3],
        "distance": parts[4],
        "distance2": parts[5],
        "direction": parts[6],
        "session_id": parts[7],
        "session_state": parts[8],
        "face_id": parts[9],
        "face_image": " ".join(parts[10:])  # phần còn lại là ảnh
    }


def update_face_image():
    records = [parse_input_line(line) for line in input_data.split("\n") if parse_input_line(line)]
    if not records:
        print("❌ Không có dữ liệu hợp lệ")
        return

    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(DB_HOST, DB_PORT)
        ) as tunnel:
            local_port = tunnel.local_bind_port
            print(f"✅ SSH tunnel mở thành công! Local port: {local_port}")

            conn = psycopg2.connect(
                host='127.0.0.1',
                port=local_port,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME
            )

            try:
                with conn.cursor() as cur:
                    for r in records:
                        # Chuyển epoch -> YYYY-MM-DD HH:MM:SS
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
                        print(f"Đã update session_id={r['session_id']} event_date={event_date}")
                    conn.commit()
                    print("✅ Cập nhật hoàn tất!")
            finally:
                conn.close()
    except Exception as e:
        print("❌ Kết nối hoặc update thất bại:", e)

if __name__ == "__main__":
    update_face_image()
