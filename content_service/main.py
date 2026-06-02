from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db, engine, SessionLocal
from kafka import KafkaProducer
from datetime import datetime, timezone
from typing import Optional
import models
import requests
import json
import time
import os

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "03722d3a197a1488464e0e532a377427")
TMDB_BASE_URL = os.getenv("TMDB_BASE_URL", "https://api.themoviedb.org/3")

GENRE_MAP = {
    "액션": 28,
    "드라마": 18,
    "애니": 16,
    "코미디": 35
}

try:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "msa-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all"
    )
except Exception as e:
    print(f"Kafka 연결 실패: {e}")
    producer = None


def send_log(
    request: Request,
    user_id: str,
    status: int,
    start_time: float,
    has_valid_token: bool,
    content_id: Optional[int] = None,
    genre: Optional[str] = None
):
    if producer is None:
        return
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "content",
        "ip": request.client.host if request.client else None,
        "method": request.method,
        "uri": str(request.url.path),
        "status": status,
        "has_valid_token": has_valid_token,
        "user_id": user_id,
        "content_id": content_id,
        "genre": genre,
        "watch_ratio": None,
        "response_time": int((time.time() - start_time) * 1000)
    }
    try:
        producer.send("msa-logs", log)
        producer.flush()
    except Exception as e:
        print(f"로그 전송 실패: {e}")


def fetch_and_save_contents(db: Session):
    for genre_name, genre_id in GENRE_MAP.items():
        existing = db.query(models.Content).filter(models.Content.genre == genre_name).count()
        if existing >= 10:
            continue
        try:
            response = requests.get(
                f"{TMDB_BASE_URL}/discover/movie",
                params={
                    "api_key": TMDB_API_KEY,
                    "with_genres": genre_id,
                    "language": "ko-KR",
                    "sort_by": "popularity.desc",
                    "page": 1
                },
                timeout=5
            )
            response.raise_for_status()
        except Exception as e:
            print(f"TMDB 호출 실패 ({genre_name}): {e}")
            continue
        movies = response.json().get("results", [])[:10]
        for movie in movies:
            exists = db.query(models.Content).filter(models.Content.content_id == movie["id"]).first()
            if not exists:
                db.add(models.Content(
                    content_id=movie["id"],
                    title=movie["title"],
                    genre=genre_name,
                    overview=movie.get("overview"),
                    poster_path=movie.get("poster_path"),
                    popularity=movie.get("popularity", 0.0)
                ))
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            print(f"DB 저장 실패 ({genre_name}): {e}")
    print("컨텐츠 DB 저장 완료")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = SessionLocal()
    try:
        models.Base.metadata.create_all(bind=engine)
        fetch_and_save_contents(db)
    finally:
        db.close()
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/content/list")
def content_list(
    request: Request,
    genre: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    start_time = time.time()
    user_id = request.headers.get("X-User-Id", "anonymous")
    has_valid_token = user_id != "anonymous"

    if genre and genre not in GENRE_MAP:
        raise HTTPException(status_code=400, detail="Invalid genre")

    query = db.query(models.Content)
    if genre:
        query = query.filter(models.Content.genre == genre)

    contents = query.offset(offset).limit(limit).all()
    result = [
        {
            "content_id": c.content_id,
            "title": c.title,
            "genre": c.genre,
            "poster_path": c.poster_path,
            "popularity": c.popularity
        }
        for c in contents
    ]
    send_log(request, user_id, 200, start_time, has_valid_token, genre=genre)
    return {"contents": result}


@app.get("/content/detail/{content_id}")
def content_detail(
    request: Request,
    content_id: int,
    db: Session = Depends(get_db)
):
    start_time = time.time()
    user_id = request.headers.get("X-User-Id", "anonymous")
    has_valid_token = user_id != "anonymous"

    content = db.query(models.Content).filter(models.Content.content_id == content_id).first()
    if not content:
        send_log(request, user_id, 404, start_time, has_valid_token, content_id=content_id)
        raise HTTPException(status_code=404, detail="Not found")

    send_log(request, user_id, 200, start_time, has_valid_token, content_id=content_id, genre=content.genre)
    return {
        "content_id": content.content_id,
        "title": content.title,
        "genre": content.genre,
        "overview": content.overview,
        "poster_path": content.poster_path,
        "popularity": content.popularity
    }


@app.get("/health")
def health():
    return {"status": "ok"}