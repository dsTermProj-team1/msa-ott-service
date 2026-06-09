"""
speed-consumer (1단계) — KEDA가 lag으로 스케일하는 컨슈머.

역할:
  - msa-logs 를 읽어(수동 커밋 → KEDA lag 측정, 같은 group pod끼리 파티션 분배)
  - 인증 우회는 단건 판정이라 여기서 바로 ES(msa-detections)에 적재
  - DoS는 자기 파티션 몫의 (ip, 윈도우)별 누적 카운트를 만들어
    누적 스냅샷을 msa-ip-counts 토픽으로 flush (전역 합산은 2단계 aggregator)
  - [차단 로직] aggregator가 DoS 판정해 msa-blocked-ips 로 내려보낸 IP는
    분석을 건너뛰고 곧바로 commit → 공격 로그로 lag이 쌓여 HPA가 스케일업하는 충돌 방지
    (발표자료 "DoS 차단 vs HPA 트리거 충돌 방지" 슬라이드 구현)
"""
import os
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "msa-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
SRC_TOPIC = os.getenv("KAFKA_TOPIC", "msa-logs")
PARTIAL_TOPIC = os.getenv("PARTIAL_TOPIC", "msa-ip-counts")
BLOCKED_TOPIC = os.getenv("BLOCKED_TOPIC", "msa-blocked-ips")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "speed-layer-group")   # KEDA가 보는 그룹
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

WINDOW_SIZE = int(os.getenv("WINDOW_SIZE_SEC", "60"))
FLUSH_INTERVAL = int(os.getenv("FLUSH_SEC", "5"))
WATERMARK = int(os.getenv("WATERMARK_SEC", "30"))
BLOCK_TTL = int(os.getenv("BLOCK_TTL_SEC", "300"))            # 로컬 차단 유지 시간(기본값)
POD_ID = os.getenv("HOSTNAME", "speed-0")

es = Elasticsearch(ES_HOST)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

local_counts = defaultdict(int)     # (window, ip) -> 이 pod 누적 카운트
max_local_window = 0
blocked = {}                        # ip -> 차단 만료 epoch


def parse_ts(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def win_of(ts):
    return int(ts // WINDOW_SIZE) * WINDOW_SIZE


def detect_auth_bypass(rec):
    if rec.get("has_valid_token") is False and "/stream/" in (rec.get("uri") or ""):
        doc = {
            "timestamp": rec.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            "service": rec.get("service"),
            "ip": rec.get("ip"),
            "uri": rec.get("uri"),
            "user_id": rec.get("user_id"),
            "attack_type": "auth_bypass",
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            es.index(index="msa-detections", body=doc)
            print(f"[인증 우회 탐지] IP: {rec.get('ip')} URI: {rec.get('uri')}")
        except Exception as e:
            print(f"ES 저장 실패: {e}")


def handle(rec):
    global max_local_window
    ip = rec.get("ip")
    if not ip:
        return

    # ── 차단 IP: 무거운 분석 건너뛰고 즉시 반환(루프에서 곧바로 commit) ──
    exp = blocked.get(ip)
    if exp is not None and time.time() < exp:
        return

    detect_auth_bypass(rec)
    ts = parse_ts(rec.get("timestamp")) or time.time()
    w = win_of(ts)
    local_counts[(w, ip)] += 1
    if w > max_local_window:
        max_local_window = w


def apply_blocks(block_consumer):
    """aggregator가 내려보낸 차단 IP를 로컬 차단셋에 반영(논블로킹)."""
    bb = block_consumer.poll(timeout_ms=0)
    for _tp, msgs in bb.items():
        for m in msgs:
            v = m.value
            ip = v.get("ip")
            if not ip:
                continue
            ttl = int(v.get("ttl_sec", BLOCK_TTL))
            blocked[ip] = time.time() + ttl
            print(f"[차단 적용] IP: {ip} ttl={ttl}s")


def flush():
    now = time.time()
    keep_floor = max_local_window - WINDOW_SIZE - WATERMARK
    to_drop = []
    for (w, ip), c in list(local_counts.items()):
        producer.send(
            PARTIAL_TOPIC,
            key=f"{w}:{ip}",
            value={"window": w, "ip": ip, "pod": POD_ID, "count": c},
        )
        if w < keep_floor:
            to_drop.append((w, ip))
    producer.flush()
    for k in to_drop:
        del local_counts[k]
    # 만료된 차단 정리
    for ip in [ip for ip, e in blocked.items() if e < now]:
        del blocked[ip]


def main():
    consumer = KafkaConsumer(
        SRC_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        value_deserializer=lambda m: m.decode("utf-8"),
        max_poll_records=500,
    )
    # 차단 목록은 broadcast: pod마다 고유 group으로 전체 메시지 수신(earliest)
    block_consumer = KafkaConsumer(
        BLOCKED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=f"speed-block-{POD_ID}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    print(f"[speed-consumer] pod={POD_ID} group={GROUP_ID} src={SRC_TOPIC} -> {PARTIAL_TOPIC} (block={BLOCKED_TOPIC}) 시작")

    last_flush = time.time()
    while True:
        apply_blocks(block_consumer)        # 차단 목록 먼저 갱신

        batch = consumer.poll(timeout_ms=1000)
        for _tp, msgs in batch.items():
            for m in msgs:
                try:
                    handle(json.loads(m.value))
                except Exception:
                    continue
        consumer.commit()                   # 처리분(차단 IP 포함) offset 커밋

        if time.time() - last_flush >= FLUSH_INTERVAL:
            flush()
            last_flush = time.time()


if __name__ == "__main__":
    main()
