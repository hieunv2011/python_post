import os
import paramiko
import psycopg2
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import sys
from datetime import datetime
import requests
import json
import io

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

THEORY_REQUIREMENTS = {
    2: {"B.01": 8.00, "B": 18.00, "C1": 18.00},
    3: {"B.01": 14.00, "B": 20.00, "C1": 20.00},
    1: {"B.01": 20.00, "B": 20.00, "C1": 20.00},
    7: {"B.01": 4.00, "B": 4.00, "C1": 4.00},
    4: {"B.01": 90.00, "B": 90.00, "C1": 90.00}
}

# ===== Cấu hình API và JWT =====
JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOjI2NywidHlwZSI6Miwic3YiOiJhRFB2biIsImV4cCI6MTc2NTg0OTgxMH0.orUKyQ2N0HRmeEPP-WjfhtZOzkCWfK0ibmtZTBtUES8"
API_URL_BASE = "https://admin.lms.shlx.vn/v1/admin/trainees/"

# Patch paramiko DSSKey cho sshtunnel
if not hasattr(paramiko, 'DSSKey'):
    try:
        from paramiko.dsskey import DSSKey as _DSSKey
        paramiko.DSSKey = _DSSKey
    except Exception:
        pass

# ===== Truy vấn SQL =====
SQL_QUERY_TRAINEE_INFO = """
SELECT
    t.id AS trainee_id,
    t.ho_va_ten,
    t.ma_dk,
    t.ngay_sinh,
    c.ma_khoa_hoc,
    c.ma_hang_dao_tao,
    b.name AS branch_name
FROM
    trainees t
JOIN
    courses c ON t.course_id = c.id
JOIN
    branches b ON c.branch_id = b.id
WHERE
    t.ma_dk = %(ma_dk)s;
"""

SQL_QUERY_STATS = """
SELECT
    c.subject_id AS subject_id,
    c.name AS course_name,
    SUM(s.total_time) AS total_time_per_course
FROM
    trainees t
JOIN
    lms_trainee_stats s ON t.id = s.trainee_id
JOIN
    "lms_courses" c ON s.course_id = c.id
WHERE
    t.ma_dk = %(ma_dk)s
GROUP BY
    c.id, c.name
ORDER BY
    c.name;
"""

# ===== API =====
def fetch_trainee_data(trainee_id):
    url = f"{API_URL_BASE}{trainee_id}"
    headers = {
        "Authorization": f"Bearer {JWT_TOKEN}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Lỗi API GET {url}: {e}")
        return None
    except json.JSONDecodeError:
        print(f"Lỗi giải mã JSON từ API {url}")
        return None

# ===== TÍNH % HOÀN THÀNH =====
def extract_completion_stats(trainee_data):
    lms_courses = trainee_data.get('stats', {}).get('lms_courses', [])

    completion_data = []
    for course in lms_courses:
        course_name = course.get('name')
        progress = course.get('learning_stats', {}).get('progress', 0)

        completion_data.append({
            'Môn Học': course_name,
            'lms_completion_percent': float(progress)
        })

    return pd.DataFrame(completion_data)

# ===== DOWNLOAD ẢNH =====
def download_image_to_stream(url):
    if not url:
        return None
    headers = {"Authorization": f"Bearer {JWT_TOKEN}"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return io.BytesIO(response.content)
    except Exception as e:
        print(f"Lỗi tải ảnh từ {url}: {e}")
        return None

# ===== AUTO FIT COLUMN =====
def autofit_columns(writer, df, sheet_name, start_row, start_col=0):
    worksheet = writer.sheets[sheet_name]

    for col_num, col_name in enumerate(df.columns):
        max_len = max(
            len(str(col_name)),
            df[col_name].astype(str).str.len().max() if not df.empty else 0
        )
        width = max_len + 2

        if col_num + start_col > 0:
            worksheet.set_column(col_num + start_col, col_num + start_col, width)

# ===== XỬ LÝ 1 HỌC VIÊN =====
def process_single_trainee_report(ma_dk_input, conn, output_dir):
    ma_dk_input_clean = ma_dk_input.strip()
    print(f"\n--- Xử lý MA_DK: {ma_dk_input_clean} ---")

    sql_params = {'ma_dk': ma_dk_input_clean}
    image_stream = None
    ho_va_ten_clean = "Unknown"

    try:
        df_info = pd.read_sql(SQL_QUERY_TRAINEE_INFO, conn, params=sql_params)

        if df_info.empty:
            print(f"❌ Không tìm thấy học viên {ma_dk_input_clean}")
            return

        info = df_info.iloc[0]
        trainee_id = info['trainee_id']
        ho_va_ten = info['ho_va_ten']
        hang_dao_tao = info['ma_hang_dao_tao']

        ho_va_ten_clean = "".join(c for c in ho_va_ten if c.isalnum() or c in (' ', '_')).rstrip()
        output_filename = os.path.join(output_dir, f"{ma_dk_input_clean}_{ho_va_ten_clean}.xlsx")
        ngay_sinh_str = info['ngay_sinh'].strftime('%d/%m/%Y') if pd.notnull(info['ngay_sinh']) else ''

        trainee_data = fetch_trainee_data(trainee_id)
        df_completion = pd.DataFrame(columns=['Môn Học', 'lms_completion_percent'])

        if trainee_data:
            portrait_url = trainee_data.get('anh_chan_dung')
            df_completion = extract_completion_stats(trainee_data)

            if portrait_url:
                image_stream = download_image_to_stream(portrait_url)

        df_stats = pd.read_sql(SQL_QUERY_STATS, conn, params=sql_params)

        if not df_stats.empty:
            df_stats = df_stats.rename(columns={
                'course_name': 'Môn Học',
                'total_time_per_course': 'DB_time'
            })

            if not df_completion.empty:
                df_stats = pd.merge(df_stats, df_completion, on='Môn Học', how='left')
            else:
                df_stats['lms_completion_percent'] = 0.0

            def calc_time(row):
                subject_id = row['subject_id']
                progress = row['lms_completion_percent']
                required_time = THEORY_REQUIREMENTS.get(subject_id, {}).get(hang_dao_tao, 0)
                return round((progress / 100) * required_time, 2)

            df_stats['Thời Gian Học Thực Tế (Giờ)'] = df_stats.apply(calc_time, axis=1)
            df_stats['% Hoàn Thành'] = df_stats['lms_completion_percent'].apply(lambda x: f"{x:.2f}%")

            df_stats = df_stats.drop(columns=['lms_completion_percent', 'DB_time'], errors='ignore')

            df_stats.insert(0, 'STT', range(1, len(df_stats) + 1))
            df_stats = df_stats[['STT', 'Môn Học', 'Thời Gian Học Thực Tế (Giờ)', '% Hoàn Thành']]

        # ===== GHI EXCEL =====
        sheet_name = 'BaoCaoThucHanh'

        with pd.ExcelWriter(output_filename, engine='xlsxwriter') as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet(sheet_name)

            base_font = 'Times New Roman'
            title_format = workbook.add_format({
                'bold': True, 'font_name': base_font, 'font_size': 13,
                'align': 'center', 'valign': 'vcenter'
            })
            header_format = workbook.add_format({
                'bold': True, 'font_name': base_font, 'font_size': 12,
                'align': 'center', 'valign': 'vcenter', 'border': 1
            })
            data_format = workbook.add_format({'font_name': base_font, 'font_size': 13})
            center_border = workbook.add_format({
                'font_name': base_font, 'font_size': 13,
                'align': 'center', 'border': 1
            })
            bold_border = workbook.add_format({
                'bold': True, 'font_name': base_font, 'font_size': 13, 'border': 1
            })
            normal_border = workbook.add_format({'font_name': base_font,'font_size': 13,'border': 1,'align': 'center','valign': 'vcenter'})

            worksheet.merge_range('A2:H2', 'BÁO CÁO QUÁ TRÌNH ĐÀO TẠO LÝ THUYẾT ONLINE CỦA HỌC VIÊN', title_format)

            start = 4
            worksheet.write(start, 0, 'Họ và tên', bold_border)
            worksheet.write(start, 1, ho_va_ten, data_format)

            worksheet.write(start + 1, 0, 'Mã học viên', bold_border)
            worksheet.write(start + 1, 1, info['ma_dk'], data_format)

            worksheet.write(start + 2, 0, 'Ngày sinh', bold_border)
            worksheet.write(start + 2, 1, ngay_sinh_str, data_format)

            worksheet.write(start + 3, 0, 'Mã khóa học', bold_border)
            worksheet.write(start + 3, 1, info['ma_khoa_hoc'], data_format)

            worksheet.write(start + 4, 0, 'Hạng đào tạo', bold_border)
            worksheet.write(start + 4, 1, info['ma_hang_dao_tao'], data_format)

            worksheet.write(start + 5, 0, 'Cơ sở đào tạo', bold_border)
            worksheet.write(start + 5, 1, info['branch_name'], data_format)

            if image_stream:
                worksheet.insert_image(start - 1, 3, 'portrait.jpg',
                                       {'image_data': image_stream, 'x_scale': 0.3, 'y_scale': 0.3})

            start_stats = start + 9

            if not df_stats.empty:
                for col, val in enumerate(df_stats.columns):
                    worksheet.write(start_stats, col, val, header_format)

                for r in range(len(df_stats)):
                    worksheet.write(start_stats + 1 + r, 0, df_stats.iloc[r]['STT'], center_border)
                    # worksheet.write(start_stats + 1 + r, 1, df_stats.iloc[r]['subject_id'], center_border)
                    worksheet.write(start_stats + 1 + r, 1, df_stats.iloc[r]['Môn Học'], data_format)
                    worksheet.write(start_stats + 1 + r, 2, df_stats.iloc[r]['Thời Gian Học Thực Tế (Giờ)'], center_border)
                    worksheet.write(start_stats + 1 + r, 3, df_stats.iloc[r]['% Hoàn Thành'], center_border)

                autofit_columns(writer, df_stats, sheet_name, start_stats)

                total_time = df_stats['Thời Gian Học Thực Tế (Giờ)'].sum()
                total_row = start_stats + len(df_stats) + 1

                worksheet.merge_range(total_row, 0, total_row, 2, 'TỔNG', bold_border)
                worksheet.write(total_row, 3, round(total_time, 2), bold_border)
                # worksheet.write(total_row, 4, '', bold_border)

                worksheet.write(total_row + 3, 2, 'CƠ SỞ ĐÀO TẠO', normal_border)

        print(f"✅ Hoàn tất file: {output_filename}")

    except Exception as e:
        print(f"❌ Lỗi khi xử lý {ma_dk_input_clean}: {e}")

def main():
    input_folder = "listtruyvan"

    if not os.path.exists(input_folder):
        print("❌ Thư mục listtruyvan không tồn tại")
        return

    # Liệt kê tất cả file Excel trong thư mục
    excel_files = [f for f in os.listdir(input_folder) if f.lower().endswith(('.xlsx', '.xls'))]

    if not excel_files:
        print("❌ Không tìm thấy file Excel trong thư mục listtruyvan")
        return

    print("\n================= BẮT ĐẦU XỬ LÝ =================")

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

            # ===== XỬ LÝ TỪNG FILE =====
            for file_name in excel_files:
                file_path = os.path.join(input_folder, file_name)

                try:
                    df_list = pd.read_excel(file_path, header=None)

                    # Lấy MA_DK từ A8 → A17 (dòng 7 → 16 vì Python đếm từ 0)
                    ma_dk_list = (
                        df_list.iloc[7:17, 0]     # cột A
                        .dropna()
                        .astype(str)
                        .str.strip()
                        .tolist()
                    )

                    if not ma_dk_list:
                        print(f"⚠️ File {file_name} không có mã đăng ký ở A8–A17")
                        continue

                except Exception as e:
                    print(f"❌ Lỗi đọc file {file_name}: {e}")
                    continue

                # Tạo folder output riêng
                base_name = os.path.splitext(file_name)[0]   # b002 -> lấy tên
                output_directory = f"ketquatruyvan_{base_name}"

                if not os.path.exists(output_directory):
                    os.makedirs(output_directory)
                    print(f"Đã tạo thư mục {output_directory}")

                print(f"\n➡️ Đang xử lý file: {file_name} → thư mục {output_directory}")

                # Xử lý từng MA_DK trong file này
                for ma_dk in ma_dk_list:
                    process_single_trainee_report(ma_dk, conn, output_directory)

            conn.close()
            print("\n✅ HOÀN TẤT TOÀN BỘ.")

    except Exception as e:
        print(f"❌ Lỗi chung (SSH/DB): {e}")

if __name__ == "__main__":
    main()
