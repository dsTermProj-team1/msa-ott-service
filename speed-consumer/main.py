"""
speed-consumer (1단계) — KEDA가 lag으로 스케일하는 컨슈머

역할:
  - msa-logs 를 읽어(수동 커밋 → KEDA lag 측정 가능, 같은 group pod끼리 파티션 분배)
  - 인증 우회는 단건 판정이라 여기서 바로 ES(msa-detections)에 적재
  - DoS는 '자기 파티션 몫'의 (ip, 윈도우)별 누적 카운트를 만들어,
    주기적으로 '누적 스냅샷'을 msa-ip-counts 토픽으로 flush (전역 합산은 2단계가 함)

왜 누적 스냅샷?:
  - 메시지 한 건이 유실/지연돼도 다음 스냅샷이 최신 누적값을 다시 들고 와 자동 복구
  - key = "{window}:{ip}" → 같은 (window,ip)는 같은 파티션 → 2단계가 순서대로/일관되게 수신
  - 모든 ip를 빠짐없이 보냄(per-pod 값이 작아도) → 분산된 부분합이 합쳐져야 하므로 절대 필터 안 함
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
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "speed-layer-group")   # KEDA가 보는 그룹
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

WINDOW_SIZE = int(os.getenv("WINDOW_SIZE_SEC", "60"))   # 1분 윈도우(tumbling)
FLUSH_INTERVAL = int(os.getenv("FLUSH_SEC", "5"))       # 부분합 스냅샷 전송 주기(낮을수록 탐지 빠름)
WATERMARK = int(os.getenv("WATERMARK_SEC", "30"))       # 지각 허용(2단계와 맞춤)
POD_ID = os.getenv("HOSTNAME", "speed-0")

es = Elasticsearch(ES_HOST)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
)

# local_counts[(window, ip)] = 이 pod가 그 윈도우에서 본 누적 요청 수
local_counts = defaultdict(int)
max_local_window = 0


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
    # spark: (has_valid_token == False) & uri.contains("/stream/") — 단건 판정, 집계 불필요
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
    detect_auth_bypass(rec)
    ts = parse_ts(rec.get("timestamp")) or time.time()
    w = win_of(ts)
    local_counts[(w, ip)] += 1
    if w > max_local_window:
        max_local_window = w


def flush():
    # 살아있는 윈도우(현재~지각허용 범위)의 (ip) 누적 스냅샷을 전부 전송.
    # 충분히 지난 윈도우는 마지막으로 한 번 더 보낸 뒤 로컬에서 정리(메모리 한도).
    keep_floor = max_local_window - WINDOW_SIZE - WATERMARK
    to_drop = []
    for (w, ip), c in list(local_counts.items()):
        producer.send(
            PARTIAL_TOPIC,
            key=f"{w}:{ip}",                       # 같은 (window,ip) → 같은 파티션
            value={"window": w, "ip": ip, "pod": POD_ID, "count": c},
        )
        if w < keep_floor:
            to_drop.append((w, ip))
    producer.flush()
    for k in to_drop:
        del local_counts[k]


def main():
    consumer = KafkaConsumer(
        SRC_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=False,       # 수동 커밋 → KEDA lag 측정 가능
        auto_offset_reset="latest",
        value_deserializer=lambda m: m.decode("utf-8"),
        max_poll_records=500,
    )
    print(f"[speed-consumer] pod={POD_ID} group={GROUP_ID} src={SRC_TOPIC} -> {PARTIAL_TOPIC} 시작")

    last_flush = time.time()
    while True:
        batch = consumer.poll(timeout_ms=1000)
        for _tp, msgs in batch.items():
            for m in msgs:
                try:
                    handle(json.loads(m.value))
                except Exception:
                    continue
        consumer.commit()   # 처리분 offset 커밋 (at-least-once)

        if time.time() - last_flush >= FLUSH_INTERVAL:
            flush()
            last_flush = time.time()


if __name__ == "__main__":
    main()
