from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, unique=True, index=True)
    password = Column(String)
    age_group = Column(String)
    preferred_genre = Column(String)
    watch_time_slot = Column(String)

    watch_progress = relationship("WatchProgress", back_populates="user")


class WatchProgress(Base):
    __tablename__ = "watch_progress"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, ForeignKey("users.user_id"))
    content_id = Column(Integer)
    progress = Column(Float, default=0.0)

    user = relationship("User", back_populates="watch_progress")


class Content(Base):
    __tablename__ = "contents"

    id = Column(Integer, primary_key=True, index=True)
    content_id = Column(Integer, unique=True, index=True)
    title = Column(String)
    genre = Column(String)
    overview = Column(String, nullable=True)
    poster_path = Column(String, nullable=True)
    popularity = Column(Float, default=0.0)