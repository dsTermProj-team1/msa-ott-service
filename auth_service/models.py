from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True)
    password = Column(String)
    age_group = Column(String)        # 10대 / 20-30대 / 40대이상
    preferred_genre = Column(String)  # 액션 / 드라마 / 애니 / 코미디
    watch_time_slot = Column(String)  # 오전 / 오후 / 저녁

    watch_progress = relationship("WatchProgress", back_populates="user")


class WatchProgress(Base):
    __tablename__ = "watch_progress"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.user_id"))
    content_id = Column(Integer)
    progress = Column(Float, default=0.0)  # 0.0 ~ 1.0 (시청 진행률)

    user = relationship("User", back_populates="watch_progress")