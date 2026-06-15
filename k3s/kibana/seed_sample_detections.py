#!/usr/bin/env python3
"""
데모용 msa-detections 샘플 탐지 문서 생성기 (speed layer 미실행 시).

실제 파이프라인(speed-consumer / dos-aggregator)이 적재하는 문서 스키마와 동일하게,
최근 1시간에 걸쳐 auth_bypass / dos 탐지 문서를 생성해 stdout 으로 ES _bulk 본문을 출력한다.

사용:
  python3 seed_sample_detections.py > /tmp/detect_bulk.ndjson
  ES=$(kubectl get svc -n monitoring elasticsearch -o jsonpath='{.spec.clusterIP}')
  kubectl run seed-$RANDOM --rm -i --restart=Never --image=curlimages/curl:8.8.0 \
    --overrides='{"spec":{"nodeName":"ubuntu-vm"}}' -- \
    -s -XPOST "http://$ES:9200/_bulk" -H 'Content-Type: application/x-ndjson' \
    --data-binary @- < /tmp/detect_bulk.ndjson
"""
import json
import random
from datetime import datetime, timedelta, timezone

random.seed(7)
now = datetime.now(timezone.utc)
INDEX = "msa-detections"

SERVICES = ["auth", "content", "streaming"]
URIS = ["/stream/movie-101", "/stream/series-22/ep-3", "/stream/live-7", "/stream/movie-88"]
ATTACKER_IPS = ["203.0.113.7", "203.0.113.42", "198.51.100.9", "192.0.2.123", "198.51.100.77"]
NORMAL_BASELINE_AVG = 120.0
DOS_MULTIPLIER = 5.0
THRESHOLD = NORMAL_BASELINE_AVG * DOS_MULTIPLIER  # 600.0


def iso(dt):
    return dt.isoformat()


def emit(doc):
    print(json.dumps({"index": {"_index": INDEX}}))
    print(json.dumps(doc, ensure_ascii=False))


# auth_bypass: 단건 판정 (speed-consumer) — 최근 60분에 걸쳐 다수
for i in range(45):
    t = now - timedelta(minutes=random.uniform(0, 60), seconds=random.uniform(0, 59))
    emit({
        "timestamp": iso(t),
        "service": random.choice(SERVICES),
        "ip": random.choice(ATTACKER_IPS),
        "uri": random.choice(URIS),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "attack_type": "auth_bypass",
        "detected_at": iso(t),
    })

# dos: 전역 집계 판정 (dos-aggregator) — 일부 공격 IP가 임계값 초과
for i in range(14):
    t = now - timedelta(minutes=random.uniform(0, 60), seconds=random.uniform(0, 59))
    ip = random.choice(ATTACKER_IPS[:3])
    req_count = random.randint(int(THRESHOLD) + 50, int(THRESHOLD) + 1200)
    emit({
        "timestamp": iso(t),
        "ip": ip,
        "req_count": req_count,
        "baseline_avg": NORMAL_BASELINE_AVG,
        "threshold": THRESHOLD,
        "multiplier": DOS_MULTIPLIER,
        "attack_type": "dos",
        "detected_at": iso(t),
    })
