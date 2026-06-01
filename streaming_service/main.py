from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from database import get_db, engine
from kafka import KafkaProducer
from datetime import datetime, timezone
from pydantic import BaseModel, Field
from typing import Optional
import models
import json
import time
import os

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "mysecretkey")
ALGORITHM = "HS256"

try:
    producer = KafkaProducer(
        bootstrap_servers="msa-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        acks="all"
    )
except Exception as e:
    print(f"Kafka 연결 실패: {e}")
    producer = None

security = HTTPBearer(auto_error=False)


class StreamEndRequest(BaseModel):
    watch_ratio: float = Field(..., ge=0.0, le=1.0)


def verify_token(token: str) -> Optional[str]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None


def send_log(
    request: Request,
    user_id: str,
    status: int,
    start_time: float,
    has_valid_token: bool,
    content_id: Optional[int] = None,
    genre: Optional[str] = None,
    watch_ratio: Optional[float] = None
):
    if producer is None:
        return
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "streaming",
        "ip": request.client.host if request.client else None,
        "method": request.method,
        "uri": str(request.url.path),
        "status": status,
        "has_valid_token": has_valid_token,
        "user_id": user_id,
        "content_id": content_id,
        "genre": genre,
        "watch_ratio": watch_ratio,
        "response_time": int((time.time() - start_time) * 1000)
    }
    try:
        producer.send("msa-logs", log)
        producer.flush()
    except Exception as e:
        print(f"로그 전송 실패: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    models.Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(lifespan=lifespan)


@app.post("/stream/start/{content_id}")
def stream_start(
    request: Request,
    content_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    start_time = time.time()

    token = credentials.credentials if credentials else None
    user_id = verify_token(token) if token else None
    has_valid_token = user_id is not None

    if not has_valid_token:
        send_log(request, "unknown", 401, start_time, False, content_id=content_id)
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    content = db.query(models.Content).filter(models.Content.content_id == content_id).first()
    if not content:
        send_log(request, user_id, 404, start_time, True, content_id=content_id)
        raise HTTPException(status_code=404, detail="Content not found")

    progress = db.query(models.WatchProgress).filter(
        models.WatchProgress.user_id == user_id,
        models.WatchProgress.content_id == content_id
    ).first()

    watch_ratio = progress.progress if progress else 0.0

    send_log(request, user_id, 200, start_time, True,
             content_id=content_id, genre=content.genre, watch_ratio=watch_ratio)

    return {
        "content_id": content_id,
        "title": content.title,
        "genre": content.genre,
        "watch_ratio": watch_ratio
    }


@app.post("/stream/end/{content_id}")
def stream_end(
    request: Request,
    content_id: int,
    body: StreamEndRequest,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    start_time = time.time()

    token = credentials.credentials if credentials else None
    user_id = verify_token(token) if token else None
    has_valid_token = user_id is not None

    if not has_valid_token:
        send_log(request, "unknown", 401, start_time, False, content_id=content_id)
        raise HTTPException(status_code=401, detail="Invalid or missing token")

    content = db.query(models.Content).filter(models.Content.content_id == content_id).first()
    if not content:
        send_log(request, user_id, 404, start_time, True, content_id=content_id)
        raise HTTPException(status_code=404, detail="Content not found")

    progress = db.query(models.WatchProgress).filter(
        models.WatchProgress.user_id == user_id,
        models.WatchProgress.content_id == content_id
    ).first()

    if progress:
        progress.progress = body.watch_ratio
    else:
        db.add(models.WatchProgress(
            user_id=user_id,
            content_id=content_id,
            progress=body.watch_ratio
        ))
        
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        send_log(request, user_id, 500, start_time, True,
                 content_id=content_id, genre=content.genre, watch_ratio=body.watch_ratio)
        raise HTTPException(status_code=500, detail="DB error")

    send_log(request, user_id, 200, start_time, True,
             content_id=content_id, genre=content.genre, watch_ratio=body.watch_ratio)

    return {"message": "progress saved", "watch_ratio": body.watch_ratio}


@app.get("/health")
def health():
    return {"status": "ok"}