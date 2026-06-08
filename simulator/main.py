import os
import requests
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor

AUTH_URL = os.getenv("AUTH_URL", "http://localhost:8001")
CONTENT_URL = os.getenv("CONTENT_URL", "http://localhost:8002")
STREAM_URL = os.getenv("STREAM_URL", "http://localhost:8003")

AGE_GROUPS = ["10대", "20-30대", "40대이상"]
GENRES = ["액션", "드라마", "애니", "코미디"]
TIME_SLOTS = ["오전", "오후", "저녁"]

NUM_USERS = 1000

# 닐슨코리아 + 왓챠피디아 2023
AGE_GENRE_BASE = {
    "10대":     {"애니": 0.35, "액션": 0.28, "코미디": 0.25, "드라마": 0.12},
    "20-30대":  {"드라마": 0.38, "코미디": 0.28, "액션": 0.20, "애니": 0.14},
    "40대이상": {"드라마": 0.52, "코미디": 0.22, "액션": 0.18, "애니": 0.08}
}

PREFERRED_BOOST = 2.0  # 선호 장르 가중치 배율

# 방통위 방송매체이용행태조사 2023
TIME_SLOT_BEHAVIOR = {
    "오전": {
        "watch_duration": (0.1, 0.3),   # 15~20분
        "think_time": (0.05, 0.15),     # 요청 간격
        "resume_prob": 0.2,             # 이어보기 확률
        "multi_content": (1, 1)         # 시청 편수
    },
    "오후": {
        "watch_duration": (0.25, 0.5),  # 30~40분
        "think_time": (0.1, 0.25),
        "resume_prob": 0.5,
        "multi_content": (1, 2)
    },
    "저녁": {
        "watch_duration": (0.6, 1.0),   # 80~120분
        "think_time": (0.2, 0.5),
        "resume_prob": 0.8,
        "multi_content": (2, 3)         # 몰아보기
    }
}

def create_user_patterns():
    users = []
    uid = 0
    per_pattern = NUM_USERS // 36
    for age in AGE_GROUPS:
        for genre in GENRES:
            for slot in TIME_SLOTS:
                for i in range(per_pattern):
                    users.append({
                        "user_id": f"user_{uid:04d}",
                        "password": "password123",
                        "age_group": age,
                        "preferred_genre": genre,
                        "watch_time_slot": slot,
                        "watched_ids": set()
                    })
                    uid += 1
    while uid < NUM_USERS:
        users.append({
            "user_id": f"user_{uid:04d}",
            "password": "password123",
            "age_group": random.choice(AGE_GROUPS),
            "preferred_genre": random.choice(GENRES),
            "watch_time_slot": random.choice(TIME_SLOTS),
            "watched_ids": set()
        })
        uid += 1
    return users

def pick_content(contents, age_group, preferred_genre, watched_ids):
    base_weights = AGE_GENRE_BASE[age_group]
    weights = []
    for c in contents:
        genre = c.get("genre", "")
        w = base_weights.get(genre, 0.1)
        if genre == preferred_genre:
            w *= PREFERRED_BOOST
        w *= max(c.get("popularity", 1.0) / 100.0, 0.01)
        if c["content_id"] in watched_ids:
            w *= 0.05  # 재시청 억제
        weights.append(max(w, 0.001))
    return random.choices(contents, weights=weights, k=1)[0]

def watch_content(content_id, headers, behavior, watched_ids):
    requests.get(f"{CONTENT_URL}/content/detail/{content_id}",
                 headers=headers, timeout=5)
    res = requests.post(f"{STREAM_URL}/stream/start/{content_id}",
                        headers=headers, timeout=10)
    if res.status_code != 200:
        return
    prev_ratio = res.json().get("watch_ratio", 0.0)
    # 이어보기 여부 결정
    if prev_ratio > 0.0 and random.random() < behavior["resume_prob"]:
        start_ratio = prev_ratio
    else:
        start_ratio = 0.0
    time.sleep(random.uniform(*behavior["think_time"]))
    watch_added = random.uniform(*behavior["watch_duration"])
    final_ratio = min(round(start_ratio + watch_added, 2), 1.0)
    requests.post(f"{STREAM_URL}/stream/end/{content_id}",
                  json={"watch_ratio": final_ratio},
                  headers=headers, timeout=5)
    watched_ids.add(content_id)

def simulate_user(user):
    try:
        behavior = TIME_SLOT_BEHAVIOR[user["watch_time_slot"]]
        res = requests.post(f"{AUTH_URL}/auth/login", json={
            "user_id": user["user_id"],
            "password": user["password"]
        }, timeout=10)
        if res.status_code != 200:
            return
        token = res.json().get("access_token")
        headers = {
            "Authorization": f"Bearer {token}",
            "X-User-Id": user["user_id"]
        }
        res = requests.get(f"{CONTENT_URL}/content/list",
                           headers=headers, timeout=10)
        if res.status_code != 200:
            return
        contents = res.json().get("contents", [])
        if not contents:
            return
        num_watch = random.randint(*behavior["multi_content"])
        for _ in range(num_watch):
            content = pick_content(
                contents,
                user["age_group"],
                user["preferred_genre"],
                user["watched_ids"]
            )
            watch_content(content["content_id"], headers, behavior, user["watched_ids"])
            time.sleep(random.uniform(*behavior["think_time"]))
        requests.post(f"{AUTH_URL}/auth/logout", headers=headers, timeout=5)
    except Exception as e:
        print(f"유저 {user['user_id']} 오류: {e}")

def dos_attack():
    # 유효 토큰 + IP 폭증 → Spark DoS 탐지
    print("DoS 공격 시작")
    try:
        res = requests.post(f"{AUTH_URL}/auth/login", json={
            "user_id": "user_0000",
            "password": "password123"
        }, timeout=10)
        if res.status_code != 200:
            print("DoS 공격자 로그인 실패")
            return
        token = res.json().get("access_token")
        headers = {"Authorization": f"Bearer {token}", "X-User-Id": "user_0000"}
    except Exception as e:
        print(f"DoS 로그인 오류: {e}")
        return

    stop_event = threading.Event()

    def attack():
        while not stop_event.is_set():
            try:
                requests.post(f"{STREAM_URL}/stream/start/1057265",
                              headers=headers, timeout=2)
            except:
                pass

    threads = []
    for _ in range(50):  # 50 스레드 동시
        t = threading.Thread(target=attack, daemon=True)
        t.start()
        threads.append(t)

    time.sleep(30)  # 30초 공격
    stop_event.set()
    print("DoS 공격 종료")

def auth_bypass():
    # 토큰 없이 streaming 직접 접근 → has_valid_token=false → Spark 탐지
    print("인증 우회 공격 시작")
    for i in range(300):
        try:
            requests.post(f"{STREAM_URL}/stream/start/{i % 40 + 1}", timeout=2)
        except:
            pass
        time.sleep(0.01)
    print("인증 우회 공격 종료")

def run():
    users = create_user_patterns()
    print(f"정상 유저 {len(users)}명 시뮬레이션 시작")

    # 60초 후 공격자 시작 (정상 유저와 동시에)
    def delayed_attack():
        time.sleep(60)
        print("공격자 실행 시작 (정상 유저 실행 중)")
        t1 = threading.Thread(target=dos_attack)
        t2 = threading.Thread(target=auth_bypass)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

    attack_thread = threading.Thread(target=delayed_attack)
    attack_thread.start()

    with ThreadPoolExecutor(max_workers=20) as executor:
        executor.map(simulate_user, users)

    attack_thread.join()
    print("시뮬레이션 완료")