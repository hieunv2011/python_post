import os

# Lấy đường dẫn file test.log cùng thư mục với file python
file_path = os.path.join(os.path.dirname(__file__), "test.log")
lines = []

# Hàm parse 1 dòng
def parse_line_for_server(parts):
    if len(parts) < 14:
        return None  # bỏ dòng thiếu trường
    face_image = " ".join(parts[14:]) if len(parts) > 14 else ""
    return {
        "sn": parts[0],
        "v": parts[1],
        "iid": parts[2],
        "tid": parts[3],
        "session_id": parts[4],
        "timestamp": parts[5],
        "lat": parts[6],
        "lng": parts[7],
        "velocity": parts[8],
        "distance": parts[9],
        "distance2": parts[10],
        "direction": parts[11],
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

# Chỉ lấy session_id đích danh
sid = "85347634-dbde-4d1c-81a6-ae8b655c8104"

# Lọc theo session_id và timestamp >= 1763361109
filtered = [row for row in lines if row["session_id"] == sid]

# Sắp xếp theo timestamp giảm dần
filtered.sort(key=lambda x: int(x["timestamp"]), reverse=True)

# Thống kê số lượng
total_count = len(filtered)
count_with_image = sum(1 for r in filtered if r["face_image"])
count_without_image = total_count - count_with_image

# Xuất ra file TXT
output_file = f"{sid}.txt"
with open(output_file, "w", encoding="utf-8") as f:
    f.write(f"# Tổng số bản ghi: {total_count}\n")
    f.write(f"# Số bản ghi CÓ ảnh: {count_with_image}\n")
    f.write(f"# Số bản ghi KHÔNG ảnh: {count_without_image}\n\n")

    # URL duy nhất ở đầu file
    if filtered:
        first = filtered[0]
        url = f"https://jira.shlx.vn/v1/logs?sn={first['sn']}&iid={first['iid']}&tid={first['tid']}&v={first['v']}"
        f.write("# POST URL:\n")
        f.write(url + "\n\n")

    f.write("# Body dạng raw text (mỗi dòng một bản ghi):\n")
    f.write("# timestamp latitude longitude velocity distance distance2 direction session_id session_state face_id face_image\n\n")
    
    # Bản ghi có ảnh
    f.write("# === Bản ghi CÓ ảnh ===\n")
    for row in filtered:
        if row["face_image"]:
            fields = [
                row["timestamp"], row["lat"], row["lng"], row["velocity"],
                row["distance"], row["distance2"], row["direction"],
                row["session_id"], row["session_state"], row["face_id"], row["face_image"]
            ]
            f.write(" ".join(fields) + "\n")
    
    # Bản ghi không có ảnh
    f.write("\n# === Bản ghi KHÔNG có ảnh ===\n")
    for row in filtered:
        if not row["face_image"]:
            fields = [
                row["timestamp"], row["lat"], row["lng"], row["velocity"],
                row["distance"], row["distance2"], row["direction"],
                row["session_id"], row["session_state"], row["face_id"]
            ]
            f.write(" ".join(fields) + "\n")

print(f"Kết quả đã được lưu ra file: {output_file}")
print(f"Tổng: {total_count}, Có ảnh: {count_with_image}, Không ảnh: {count_without_image}")
