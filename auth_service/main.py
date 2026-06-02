import os

from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from passlib.context import CryptContext
from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from database import get_db, engine
from kafka import KafkaProducer
import models
import json
import time

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

try:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "msa-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
except Exception as e:
    print(f"Kafka 연결 실패: {e}")
    producer = None

class LoginRequest(BaseModel):
    user_id: str
    password: str

def create_token(user_id: str):
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    return jwt.encode({"sub": user_id, "exp": expire}, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        return None

def send_log(request: Request, user_id: str, status: int, start_time: float, has_valid_token: bool):
    if producer is None:
        return
    log = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": "auth",
        "ip": request.client.host,
        "method": request.method,
        "uri": str(request.url.path),
        "status": status,
        "has_valid_token": has_valid_token,
        "user_id": user_id,
        "content_id": None,
        "genre": None,
        "watch_ratio": None,
        "response_time": int((time.time() - start_time) * 1000)
    }
    try:
        producer.send("msa-logs", log)
        producer.flush()
    except Exception as e:
        print(f"로그 전송 실패: {e}")

@app.post("/auth/login")
def login(request: Request, body: LoginRequest, db: Session = Depends(get_db)):
    start_time = time.time()
    user = db.query(models.User).filter(models.User.user_id == body.user_id).first()
    if not user or not pwd_context.verify(body.password, user.password):
        send_log(request, body.user_id, 401, start_time, False)
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_token(user.user_id)
    send_log(request, user.user_id, 200, start_time, True)
    return {"access_token": token, "token_type": "bearer"}

@app.post("/auth/logout")
def logout(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    start_time = time.time()
    user_id = verify_token(credentials.credentials)
    if not user_id:
        send_log(request, "unknown", 401, start_time, False)
        raise HTTPException(status_code=401, detail="Invalid token")
    send_log(request, user_id, 200, start_time, True)
    return {"message": "logged out"}

@app.get("/health")
def health():
    return {"status": "ok"}