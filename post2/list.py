import os

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

def main():
    file_path = os.path.join(os.path.dirname(__file__), "test08.log")
    session_list_file = os.path.join(os.path.dirname(__file__), "session_list.txt")

    lines = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            parsed = parse_line(parts)
            if parsed:
                lines.append(parsed)

    if not lines:
        print("❌ Không có bản ghi nào trong file log")
        return

    # Lấy tất cả session_id duy nhất
    sessions = sorted(set(r["session_id"] for r in lines))

    print(f"Tổng số session: {len(sessions)}")
    for idx, sid in enumerate(sessions, start=1):
        print(f"{idx}. {sid}")

    # Xuất file session_list.txt
    with open(session_list_file, "w", encoding="utf-8") as f:
        for sid in sessions:
            f.write(sid + "\n")

    print(f"\n✅ Danh sách session đã được lưu vào: {session_list_file}")

if __name__ == "__main__":
    main()
