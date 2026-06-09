"""
dos-aggregator (2단계) — 단일 컨슈머. 전역 합산·판정 전담.

  - msa-ip-counts(부분합)만 읽어 pod별 최신값 upsert → 전역 합산 (로그 backlog와 격리)
  - 이벤트시간 watermark로 윈도우 닫고 판정, 2분 GC 보험, 윈도우 단위 만료
  - [차단 발행] DoS 판정한 IP를 msa-blocked-ips 로 내보내 speed-consumer가 skip하게 함
    (발표자료 "DoS 차단 vs HPA 트리거 충돌 방지"). 공격이 계속되면 매 윈도우 재발행해 TTL 갱신.
"""
import os
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "msa-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
PARTIAL_TOPIC = os.getenv("PARTIAL_TOPIC", "msa-ip-counts")
BLOCKED_TOPIC = os.getenv("BLOCKED_TOPIC", "msa-blocked-ips")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "speed-agg-group")
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

DOS_MULTIPLIER = float(os.getenv("DOS_MULTIPLIER", "5.0"))
BASELINE_ROUNDS = int(os.getenv("BASELINE_BATCHES", "2"))
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE_SEC", "60"))
WATERMARK = int(os.getenv("WATERMARK_SEC", "30"))
GC_TIMEOUT = int(os.getenv("GC_TIMEOUT_SEC", "120"))
EVAL_INTERVAL = int(os.getenv("EVAL_SEC", "5"))
BLOCK_TTL = int(os.getenv("BLOCK_TTL_SEC", "300"))          # speed-consumer가 적용할 차단 유지 시간

es = Elasticsearch(ES_HOST)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

windows = defaultdict(lambda: defaultdict(dict))   # window -> ip -> pod -> 최신 누적 카운트
last_update = {}
max_window_seen = 0
baseline = {"avg": None, "rounds": 0}
closed_windows = set()      # 이미 평가(닫힘)한 윈도우 — 재수신해도 재평가/재발행 안 함(멱등)


def ingest(rec):
    global max_window_seen
    w = int(rec["window"]); ip = rec["ip"]; pod = rec["pod"]; c = int(rec["count"])
    # speed-consumer는 내결함성을 위해 닫힌 윈도우 스냅샷도 로컬 GC 전까지 계속 재전송한다.
    # 이미 닫은 윈도우면 무시 → 중복 탐지/차단 재발행 방지.
    if w in closed_windows:
        return
    windows[w][ip][pod] = c
    last_update[w] = time.time()
    if w > max_window_seen:
        max_window_seen = w


def block_ip(ip):
    """차단 IP를 speed-consumer 들에게 broadcast (skip 대상)."""
    try:
        producer.send(BLOCKED_TOPIC, key=ip, value={
            "ip": ip,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "ttl_sec": BLOCK_TTL,
        })
        producer.flush()
    except Exception as e:
        print(f"차단 발행 실패: {e}")


def evaluate(w):
    ipmap = windows.get(w, {})
    req = {ip: sum(pods.values()) for ip, pods in ipmap.items()}   # pod 합산 = 전역 카운트
    windows.pop(w, None)
    last_update.pop(w, None)
    closed_windows.add(w)            # 닫힘 기록 → 이후 재수신돼도 ingest에서 무시(멱등)
    if not req:
        return

    current_avg = sum(req.values()) / len(req)

    if baseline["rounds"] < BASELINE_ROUNDS:
        if baseline["avg"] is None:
            baseline["avg"] = current_avg
        else:
            n = baseline["rounds"]
            baseline["avg"] = (baseline["avg"] * n + current_avg) / (n + 1)
        baseline["rounds"] += 1
        print(f"[기준값 수집 중] 라운드 {baseline['rounds']}/{BASELINE_ROUNDS} 평균: {baseline['avg']:.1f}")
        return

    threshold = baseline["avg"] * DOS_MULTIPLIER
    for ip, c in req.items():
        if c > threshold:
            doc = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "ip": ip,
                "req_count": c,
                "baseline_avg": baseline["avg"],
                "threshold": threshold,
                "multiplier": DOS_MULTIPLIER,
                "attack_type": "dos",
                "detected_at": datetime.now(timezone.utc).isoformat(),
            }
            try:
                es.index(index="msa-detections", body=doc)
                print(f"[DoS 탐지] IP: {ip} 요청수(전역): {c} 임계값: {threshold:.1f} (평균 {baseline['avg']:.1f}의 {DOS_MULTIPLIER}배)")
            except Exception as e:
                print(f"ES 저장 실패: {e}")
            block_ip(ip)        # ← speed-consumer가 이후 이 IP 로그를 skip하도록 차단 발행


def maybe_close():
    now = time.time()
    for w in sorted(list(windows.keys())):
        closeable = (max_window_seen - w) >= WATERMARK
        stale = (now - last_update.get(w, now)) >= GC_TIMEOUT
        if closeable or stale:
            evaluate(w)
    # closed_windows 메모리 한도: 충분히 지난 윈도우는 기록에서 제거
    # (그만큼 늦은 재전송은 안 오고, 혹시 와도 한 윈도우 재생성은 무해).
    floor = max_window_seen - GC_TIMEOUT - WINDOW_SIZE
    for w in [w for w in closed_windows if w < floor]:
        closed_windows.discard(w)


def main():
    consumer = KafkaConsumer(
        PARTIAL_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=1000,
    )
    print(f"[dos-aggregator] group={GROUP_ID} src={PARTIAL_TOPIC} block={BLOCKED_TOPIC} 시작")

    last_eval = time.time()
    while True:
        batch = consumer.poll(timeout_ms=1000)
        for _tp, msgs in batch.items():
            for m in msgs:
                try:
                    ingest(m.value)
                except Exception:
                    continue

        if time.time() - last_eval >= EVAL_INTERVAL:
            maybe_close()
            last_eval = time.time()


if __name__ == "__main__":
    main()
