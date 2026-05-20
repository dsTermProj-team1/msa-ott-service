from database import SessionLocal
from models import User
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

db = SessionLocal()

users = [
    {"user_id": "user_001", "password": "pass123", "age_group": "20-30대", "preferred_genre": "액션", "watch_time_slot": "저녁"},
    {"user_id": "user_002", "password": "pass123", "age_group": "10대", "preferred_genre": "애니", "watch_time_slot": "오후"},
    {"user_id": "user_003", "password": "pass123", "age_group": "40대이상", "preferred_genre": "드라마", "watch_time_slot": "오전"},
]

for u in users:
    existing = db.query(User).filter(User.user_id == u["user_id"]).first()
    if not existing:
        db.add(User(
            user_id=u["user_id"],
            password=pwd_context.hash(u["password"]),
            age_group=u["age_group"],
            preferred_genre=u["preferred_genre"],
            watch_time_slot=u["watch_time_slot"]
        ))

db.commit()
db.close()
print("Seed 완료")
