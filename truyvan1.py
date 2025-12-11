#!/usr/bin/env python3
import os
import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import sys
import requests
import json
import io
from datetime import datetime
from json import JSONDecodeError

# =============================
# CẤU HÌNH SSH & DATABASE
# =============================
SSH_HOST = 'dat.shlx.vn'
SSH_PORT = 22
SSH_USER = 'root'
SSH_PASSWORD = 'R6lC%*sDpd7u'

DB_HOST = 'localhost'
DB_PORT = 5432
DB_USER = 'shlx'
DB_PASSWORD = '123456'
DB_NAME = 'shlx'

# =============================
# API TOKEN
# =============================
JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOjI2NywidHlwZSI6Miwic3YiOiJhRFB2biIsImV4cCI6MTc2NjM3Njk0Nn0.q85lq-s3BFaqFxAtrYOmXRKrEAo42W5OUAmoZ01399w"
API_URL_BASE = "https://admin.lms.shlx.vn/v1/admin/trainees/"

if not hasattr(paramiko, 'DSSKey'):
    try:
        from paramiko.dsskey import DSSKey as _DSSKey
        paramiko.DSSKey = _DSSKey
    except:
        pass

# =============================
# SQL QUERIES
# =============================
SQL_QUERY_TRAINEE_INFO = """
SELECT
    t.id AS trainee_id,
    t.ho_va_ten,
    t.ma_dk,
    t.ngay_sinh,
    c.ma_khoa_hoc,
    c.ma_hang_dao_tao,
    b.name AS branch_name
FROM trainees t
JOIN courses c ON t.course_id = c.id
JOIN branches b ON c.branch_id = b.id
WHERE t.ma_dk = %(ma_dk)s;
"""

SQL_QUERY_EXAM_CONFIG = """
SELECT let.config
FROM lms_exam_trainees let
JOIN trainees t ON let.trainee_id = t.id
WHERE t.ma_dk = %(ma_dk)s AND let.exam_id = %(exam_id)s;
"""

SQL_QUERY_EXAM_ATTEMPT = """
SELECT lea.config
FROM lms_exam_attempts lea
JOIN trainees t ON lea.trainee_id = t.id
WHERE t.ma_dk = %(ma_dk)s AND lea.exam_id = %(exam_id)s;
"""

SQL_QUERY_EXAM_INFO = """
SELECT name FROM lms_exams WHERE id = %(exam_id)s;
"""

SQL_QUERY_EXAM_SCORE = """
SELECT score FROM lms_exam_trainees
WHERE trainee_id = %(trainee_id)s AND exam_id = %(exam_id)s;
"""

# =============================
# API HELPERS
# =============================
def fetch_trainee_data(trainee_id):
    url = f"{API_URL_BASE}{trainee_id}"
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        return res.json()
    except Exception:
        return None

def download_image_to_stream(url):
    if not url:
        return None
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        res.raise_for_status()
        return io.BytesIO(res.content)
    except Exception:
        return None

# =============================
# PARSE QUESTION CONFIG
# =============================
def parse_exam_questions(config_data):
    questions = []
    if not config_data:
        return questions

    try:
        data = json.loads(config_data) if isinstance(config_data, str) else config_data
    except Exception:
        return []

    q_list = data.get("questions", [])
    if not isinstance(q_list, list):
        return []

    for q_main in q_list:
        try:
            inner = q_main.get("content")
            if isinstance(inner, str):
                inner = json.loads(inner)
            inner = inner or {}
            questions.append({
                "id": q_main.get("id"),
                "content": inner.get("content"),
                "options": inner.get("options", [])
            })
        except Exception:
            continue

    return questions

def parse_attempt_answers(config_data):
    if not config_data:
        return {}
    try:
        data = json.loads(config_data) if isinstance(config_data, str) else config_data
    except Exception:
        return {}
    return data.get("answers", {})

# =============================
# MAIN REPORT FUNCTION
# =============================
def process_single_trainee_report(ma_dk_input, exam_id_input, conn, output_dir="ketquatruyvan_info"):
    ma_dk = str(ma_dk_input).strip()
    # ensure exam_id is int for queries
    exam_id = int(exam_id_input)

    # 1) Lấy thông tin học viên
    df_info = pd.read_sql(SQL_QUERY_TRAINEE_INFO, conn, params={'ma_dk': ma_dk})
    if df_info.empty:
        print(f"[WARN] Không tìm thấy học viên {ma_dk}")
        return

    info = df_info.iloc[0]
    # ensure trainee_id is int
    trainee_id = int(info['trainee_id'])

    # 2) Lấy thông tin bài thi
    df_exam_info = pd.read_sql(SQL_QUERY_EXAM_INFO, conn, params={'exam_id': exam_id})
    exam_name = df_exam_info.iloc[0]["name"] if not df_exam_info.empty else ""

    # 3) Điểm của học viên
    df_score = pd.read_sql(SQL_QUERY_EXAM_SCORE, conn,
                           params={'trainee_id': trainee_id, 'exam_id': exam_id})
    score_value = df_score.iloc[0]["score"] if not df_score.empty else ""

    # 4) Lấy ảnh từ API
    trainee_data = fetch_trainee_data(trainee_id)
    portrait_stream = None
    if trainee_data and trainee_data.get("anh_chan_dung"):
        portrait_stream = download_image_to_stream(trainee_data["anh_chan_dung"])

    # 5) Lấy danh sách câu hỏi
    df_cfg = pd.read_sql(SQL_QUERY_EXAM_CONFIG, conn,
                         params={'ma_dk': ma_dk, 'exam_id': exam_id})
    exam_questions = parse_exam_questions(df_cfg.iloc[0]["config"]) if not df_cfg.empty else []

    # 6) Lấy danh sách câu trả lời (attempt)
    df_attempt = pd.read_sql(SQL_QUERY_EXAM_ATTEMPT, conn,
                             params={'ma_dk': ma_dk, 'exam_id': exam_id})
    answers_map = parse_attempt_answers(df_attempt.iloc[0]["config"]) if not df_attempt.empty else {}

    # Tạo folder output cho học viên
    safe_name = "".join(c for c in info["ho_va_ten"] if c.isalnum() or c == " ").strip()
    student_folder = os.path.join(output_dir, f"{ma_dk}_{safe_name}")
    os.makedirs(student_folder, exist_ok=True)
    # filepath = os.path.join(student_folder, f"{ma_dk}_{exam_id}.xlsx")
    safe_exam_name = "".join(c for c in exam_name if c.isalnum() or c in " _-").strip()
    filepath = os.path.join(student_folder, f"{ma_dk}_{safe_exam_name}.xlsx")

    # =============================
    # GHI EXCEL
    # =============================
    with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:
        wb = writer.book
        sh = wb.add_worksheet("BaoCao")

        base_font = "Times New Roman"

        header_fmt = wb.add_format({
            'bold': True, 'font_size': 12, 'font_name': base_font, 'border': 1, 'align': 'center', 'valign': 'vcenter'
        })
        border = wb.add_format({'font_name': base_font, 'font_size': 12, 'border': 1, 'valign': 'vcenter'})
        wrap_border = wb.add_format({'font_name': base_font, 'font_size': 12, 'border': 1,
                                     'text_wrap': True, 'valign': 'top'})
        question_fmt = wb.add_format({'font_name': base_font, 'font_size': 12, 'border': 1,
                                      'bold': True, 'text_wrap': True, 'valign': 'top'})
        option_fmt = wb.add_format({'font_name': base_font, 'font_size': 11, 'border': 1,
                                    'text_wrap': True, 'valign': 'top'})
        correct_fmt = wb.add_format({
            'font_name': base_font, 'font_size': 11, 'border': 1,
            'text_wrap': True, 'bg_color': '#C6EFCE', 'font_color': '#006100', 'valign': 'top'
        })
        xmark_fmt = wb.add_format({
            'font_name': base_font, 'font_size': 12, 'border': 1,
            'align': 'center', 'valign': 'vcenter'
        })
        table_header = wb.add_format({
            'font_name': base_font, 'font_size': 12, 'border': 1,
            'bold': True, 'align': 'center', 'valign': 'vcenter'
        })

        sh.set_column("A:A", 25)
        sh.set_column("B:B", 80)
        sh.set_column("C:C", 15)

        # --- PHẦN THÔNG TIN ---
        row = 1
        # merge A..C for title
        sh.merge_range(row, 0, row, 2, "THÔNG TIN HỌC VIÊN", header_fmt)
        row += 2

        fields = [
            ("Họ và tên", info["ho_va_ten"]),
            ("Mã học viên", info["ma_dk"]),
            ("Ngày sinh", info["ngay_sinh"].strftime("%d/%m/%Y") if pd.notnull(info["ngay_sinh"]) else ""),
            ("Khóa học", info["ma_khoa_hoc"]),
            ("Hạng đào tạo", info["ma_hang_dao_tao"]),
            ("Cơ sở", info["branch_name"])
        ]

        for label, value in fields:
            sh.write(row, 0, label, border)
            sh.write(row, 1, value, border)
            # keep column C empty but bordered
            sh.write(row, 2, "", border)
            row += 1

        # Ảnh (nếu có) - nằm trên sheet và được nhúng vào file excel
        if portrait_stream:
            # chèn ảnh tại cột C, hàng 2 (tương đối)
            sh.insert_image(2, 2, "portrait.jpg", {
                'image_data': portrait_stream, 'x_scale': 0.3, 'y_scale': 0.3
            })

        row += 1

        # --- TÊN BÀI THI + ĐIỂM (cùng 1 dòng) ---
        # merge A..B for label, C for score
        sh.write(row, 0, "Kết quả bài kiểm tra", header_fmt)
        sh.write(row, 1, exam_name if exam_name else "", header_fmt)
        sh.write(row, 2, f"Điểm: {score_value}", header_fmt)
        row += 2

        # --- BẢNG CÂU HỎI ---
        sh.write(row, 0, "Câu hỏi", table_header)
        sh.write(row, 1, "Nội dung", table_header)
        sh.write(row, 2, "Đáp án đã chọn", table_header)
        row += 1

        # --- VÒNG LẶP CÂU HỎI ---
        for q in exam_questions:
            qid = q.get("id")
            qid_str = str(qid)

            ans_entry = answers_map.get(qid_str, {})
            user_ans = ans_entry.get("answer")
            correct_ans = ans_entry.get("correct")

            # Dòng câu hỏi (ID)
            sh.write(row, 0, f"ID {qid}", question_fmt)
            sh.write(row, 1, q.get("content", ""), question_fmt)
            sh.write(row, 2, "", border)
            row += 1

            # Các option (ghi từng option trên 1 dòng)
            opts = q.get("options") or []
            for idx, opt in enumerate(opts):
                opt_no = idx + 1
                fmt = correct_fmt if (correct_ans is not None and opt_no == int(correct_ans)) else option_fmt

                sh.write(row, 0, "", border)
                sh.write(row, 1, f"{opt_no}. {opt}", fmt)

                if user_ans is not None and int(user_ans) == opt_no:
                    sh.write(row, 2, "X", xmark_fmt)
                else:
                    sh.write(row, 2, "", border)

                row += 1

            row += 1  # khoảng cách giữa các câu

    print(f"✔ Tạo file thành công: {filepath}")

# =============================
# READ INPUT EXCEL và CHẠY CHO NHIỀU MA_DK
# =============================
def process_from_input_file(input_xlsx="c1003.xlsx", output_dir="ketquatruyvan_info"):
    # Đọc file input nằm cùng thư mục với script
    if not os.path.exists(input_xlsx):
        print(f"[ERROR] Không tìm thấy file input: {input_xlsx}")
        return

    try:
        df = pd.read_excel(input_xlsx, header=None, engine='openpyxl')
    except Exception as e:
        print(f"[ERROR] Lỗi đọc file excel {input_xlsx}: {e}")
        return

    # Lấy MA_DK từ cột A, bắt đầu từ hàng A8 (index 7). Nếu muốn lấy nhiều hơn, nó lấy từ A8 đến hết không rỗng.
    ma_dk_series = df.iloc[7:, 0].dropna().astype(str).str.strip()
    ma_dk_list = [m for m in ma_dk_series.tolist() if m]

    if not ma_dk_list:
        print("[WARN] Không tìm thấy mã đăng ký trong file input (từ A8 trở xuống).")
        return

    # 4 mã khoá cố định
    exam_ids = [228, 229, 230, 231]

    # Mở SSHTunnel + kết nối DB 1 lần
    try:
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

            for ma_dk in ma_dk_list:
                print(f"\n--> Xử lý MA_DK={ma_dk}")
                for exam_id in exam_ids:
                    try:
                        process_single_trainee_report(ma_dk, exam_id, conn, output_dir=output_dir)
                    except Exception as e:
                        print(f"[ERROR] Lỗi khi xử lý MA_DK={ma_dk}, exam_id={exam_id}: {e}")

            conn.close()
    except Exception as e:
        print(f"[ERROR] Lỗi chung (SSH/DB): {e}")

# =============================
# MAIN
# =============================
if __name__ == "__main__":
    # Nếu muốn truyền file input khác: python truyvan_full.py otherfile.xlsx
    input_file = sys.argv[1] if len(sys.argv) > 1 else "c1003.xlsx"
    process_from_input_file(input_xlsx=input_file, output_dir="ketquatruyvan_info")
