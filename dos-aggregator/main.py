"""
dos-aggregator (2단계) — 단일 컨슈머. 전역 합산·판정 전담.
  - 원본 로그(msa-logs)가 아니라 '부분합'(msa-ip-counts)만 읽음
    -> 로그 backlog가 아무리 쌓여도 지연 X (지연 격리)
  - pod A/B/C가 같은 (window,ip)에 대해 따로 보낸 누적 스냅샷을 'pod별 최신값'으로 보관 후 합산
    -> A만 보거나 마지막 것만 보지 않음. 순서/도착 시점 무관(upsert)
    -> 예: pod1=300, pod2=300, pod3=400 → 전역 1000 으로 합산해 임계 판정
  - 만료는 pod별이 아니라 '윈도우' 단위:
      1차) 이벤트시간 watermark로 윈도우가 닫히면 그 윈도우 전체 삭제(주 만료)
      2차) 어떤 이유로 안 닫힌 윈도우는 GC_TIMEOUT(2분) 지나면 평가 후 강제 삭제(보험)
    -> 살아있는 윈도우 동안엔 죽은 pod의 마지막 값도 유지(그 윈도우의 정탐 보존),
      윈도우가 닫히면 같이 비워짐
  - backlog로 부분합이 늦게 와도, 윈도우 닫힘을 'Wall Time'가 아니라 '이벤트시간 watermark'로 판단
    -> 늦은 부분합도 윈도우가 안 닫혔으면 합산에 포함(undercount 방지)
"""
import os
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "msa-kafka-kafka-bootstrap.kafka.svc.cluster.local:9092")
PARTIAL_TOPIC = os.getenv("PARTIAL_TOPIC", "msa-ip-counts")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "speed-agg-group")
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")

DOS_MULTIPLIER = float(os.getenv("DOS_MULTIPLIER", "5.0"))      # 평균의 N배 (spark와 동일)
BASELINE_ROUNDS = int(os.getenv("BASELINE_BATCHES", "2"))       # 기준값 수집 라운드(닫힌 윈도우) 수
WINDOW_SIZE = int(os.getenv("WINDOW_SIZE_SEC", "60"))
WATERMARK = int(os.getenv("WATERMARK_SEC", "30"))               # 지각 허용 (윈도우 닫기 지연)
GC_TIMEOUT = int(os.getenv("GC_TIMEOUT_SEC", "120"))            # 2분: 안 닫힌 윈도우 강제 정리
EVAL_INTERVAL = int(os.getenv("EVAL_SEC", "5"))                 # 윈도우 닫힘 점검 주기

es = Elasticsearch(ES_HOST)

# windows[window][ip][pod] = 그 pod가 보낸 최신 누적 카운트
windows = defaultdict(lambda: defaultdict(dict))
last_update = {}            # window -> 마지막 갱신 Wall Time 시각 (2차 GC용)
max_window_seen = 0         # 수신한 부분합 중 가장 최신 윈도우 (이벤트시간 watermark 기준)
baseline = {"avg": None, "rounds": 0}


def ingest(rec):
    global max_window_seen
    w = int(rec["window"])
    ip = rec["ip"]
    pod = rec["pod"]
    c = int(rec["count"])
    windows[w][ip][pod] = c          # pod별 최신값 upsert (더하지 않고 덮어씀)
    last_update[w] = time.time()
    if w > max_window_seen:
        max_window_seen = w


def evaluate(w):
    """윈도우 w를 닫고 전역 합산·판정 후 삭제."""
    ipmap = windows.get(w, {})
    req = {ip: sum(pods.values()) for ip, pods in ipmap.items()}   # pod 합산 = 전역 카운트
    windows.pop(w, None)
    last_update.pop(w, None)
    if not req:
        return

    current_avg = sum(req.values()) / len(req)

    # 초반 N개 닫힌 윈도우로 정상 기준값 수집 (이 동안은 판정 안 함)
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


def maybe_close():
    now = time.time()
    for w in sorted(list(windows.keys())):
        # 1차: 이벤트시간 watermark — 더 최신 윈도우가 watermark 이상 진행하면 닫힘
        closeable = (max_window_seen - w) >= WATERMARK
        # 2차: Wall Time GC — 오래 갱신 없는(안 닫힌) 윈도우 강제 정리(스트림 정지/누수 보험)
        stale = (now - last_update.get(w, now)) >= GC_TIMEOUT
        if closeable or stale:
            evaluate(w)


def main():
    consumer = KafkaConsumer(
        PARTIAL_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=True,        # 부분합은 재수신해도 upsert라 멱등 → 자동 커밋 OK
        auto_offset_reset="latest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=1000,
    )
    print(f"[dos-aggregator] group={GROUP_ID} src={PARTIAL_TOPIC} 시작")

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
