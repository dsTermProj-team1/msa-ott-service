import os
import random
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from passlib.context import CryptContext

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://ottuser:ottpass@localhost/ottdb")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

AGE_GROUPS = ["10대", "20-30대", "40대이상"]
GENRES = ["액션", "드라마", "애니", "코미디"]
TIME_SLOTS = ["오전", "오후", "저녁"]

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True)
    password = Column(String)
    age_group = Column(String)
    preferred_genre = Column(String)
    watch_time_slot = Column(String)

class WatchProgress(Base):
    __tablename__ = "watch_progress"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.user_id"))
    content_id = Column(Integer)
    progress = Column(Float, default=0.0)

def create_users():
    users = []
    uid = 0
    per_pattern = 1000 // 36
    for age in AGE_GROUPS:
        for genre in GENRES:
            for slot in TIME_SLOTS:
                for i in range(per_pattern):
                    users.append(User(
                        user_id=f"user_{uid:04d}",
                        password=pwd_context.hash("password123"),
                        age_group=age,
                        preferred_genre=genre,
                        watch_time_slot=slot
                    ))
                    uid += 1
    while uid < 1000:
        users.append(User(
            user_id=f"user_{uid:04d}",
            password=pwd_context.hash("password123"),
            age_group=random.choice(AGE_GROUPS),
            preferred_genre=random.choice(GENRES),
            watch_time_slot=random.choice(TIME_SLOTS)
        ))
        uid += 1
    return users

def run():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        existing = db.query(User).count()
        if existing > 0:
            print(f"이미 유저 {existing}명 존재. 스킵.")
            return
        users = create_users()
        db.bulk_save_objects(users)
        db.commit()
        print(f"유저 {len(users)}명 삽입 완료")
    except Exception as e:
        db.rollback()
        print(f"오류: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    run()