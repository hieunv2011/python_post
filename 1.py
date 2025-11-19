import os

# Lấy đường dẫn file test.log cùng thư mục với file python
file_path = os.path.join(os.path.dirname(__file__), "test.log")
lines = []

# Hàm parse 1 dòng và chuyển sang định dạng server yêu cầu
def parse_line_for_server(parts):
    if len(parts) < 14:
        return None  # bỏ dòng thiếu trường
    face_image = " ".join(parts[14:]) if len(parts) > 14 else ""
    return {
        "timestamp": parts[5],  # giữ dạng string
        "lat": parts[6],
        "lng": parts[7],
        "velocity": parts[8],
        "distance": parts[9],
        "distance2": parts[10],
        "direction": parts[11],
        "session_id": parts[4],
        "session_state": parts[12],
        "face_id": parts[13],
        "face_image": face_image
    }

# Đọc file test.log
with open(file_path, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        parsed = parse_line_for_server(parts)
        if parsed:
            lines.append(parsed)

# Nhập session_id cần lọc
sid = input("Nhập session_id để lọc: ").strip()

# Lọc theo session_id
filtered = [row for row in lines if row["session_id"] == sid]

# Sắp xếp theo timestamp giảm dần
filtered.sort(key=lambda x: int(x["timestamp"]), reverse=True)

# Hiển thị số lượng bản ghi
print(f"Số lượng bản ghi tìm thấy: {len(filtered)}\n")

# Xuất ra file TXT/LOG dạng server chấp nhận
output_file = f"{sid}.txt"
with open(output_file, "w", encoding="utf-8") as f:
    for row in filtered:
        line = " ".join([
            row["timestamp"], row["lat"], row["lng"], row["velocity"],
            row["distance"], row["distance2"], row["direction"],
            row["session_id"], row["session_state"], row["face_id"], row["face_image"]
        ])
        f.write(line + "\n")

print(f"Kết quả đã được lưu ra file: {output_file}")
