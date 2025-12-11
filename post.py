import httpx

URL = "http://localhost:8002/lms/trainee-sessions"
JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOjUyNDQxMywidHlwZSI6MCwic3YiOiJhRFB2bi4xIiwiZXhwIjoxNzY2NTUxMzc0fQ.FKaggR1QQTE9mO0U2y78wbVnPF0BE_S2M2mOdQLGq48"

DATA = {
    "confirmed": "false",
    "item_id": 16786  # bỏ khoảng trắng thừa
}

HEADERS = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "X-Branch-Id": "19"  # thêm header branch
}

TOTAL = 1898740

with httpx.Client() as client:
    for i in range(TOTAL):
        try:
            resp = client.post(URL, json=DATA, headers=HEADERS)
            print(f"{i+1}: {resp.status_code}")
        except Exception as e:
            print(f"{i+1}: Lỗi - {e}")
