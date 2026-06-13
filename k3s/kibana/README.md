# Kibana 대시보드 (코드로 관리)

log-pipeline 집계 결과를 시각화하는 Kibana Saved Objects를 NDJSON으로 박아둔 것.
클러스터를 갈아엎어도 import 한 번으로 데이터 뷰·시각화·대시보드가 복원된다.

## 포함 객체 (`msa-monitoring-dashboard.ndjson`)

- **데이터 뷰**
  - `msa-batch` (시간 필드 `window_start`) — 1차 집계(시간당 서비스별 요청/에러)
  - `msa-baseline` (시간 필드 `ref_date`) — 2차 baseline(구간별 권장 Pod 수)
- **시각화**
  - 요약 지표 (metric): 총 요청수 / 4xx / 5xx / 평균 에러율
  - 서비스별 시간당 요청수 (line, `service.keyword` 분할)
  - 시간대별 4xx/5xx 에러 (bar stacked, `count_4xx`/`count_5xx` 합계)
  - 서비스별 에러율 추세 (line, `error_rate` 평균, `service.keyword` 분할)
  - 구간별 권장 Pod 수 (grouped bar, `ref_date`별 `window.keyword` 시간순, max replicas, Y 0~6)
  - 구간별 권장 Pod 수 표 (Lens datatable, 날짜=행 / 구간=열 전치, max replicas)
- **대시보드**: `MSA 배치 모니터링` (위 6개 패널, 기본 기간 `now-7d/d ~ now` = 최근 1주일)

> `service`/`window` 필드는 ES에서 `text`+`.keyword` 멀티필드로 매핑되므로
> terms 집계는 반드시 `*.keyword` 를 사용한다. 또한 legacy 시각화는
> `searchSourceJSON` 안에 `indexRefName` 이 있어야 데이터 뷰와 연결된다.

## Import

Kibana NodePort는 `30731`. (아무 노드 IP로나 접속)

```bash
# UI: Stack Management → Saved Objects → Import → 파일 선택

# 또는 API
curl -s "http://<node-ip>:30731/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" \
  --form file=@k3s/kibana/msa-monitoring-dashboard.ndjson
```

import 후 Dashboard → `MSA 배치 모니터링` 에서 확인.

## Export (대시보드 수정 후 갱신)

```bash
curl -s "http://<node-ip>:30731/api/saved_objects/_export" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"objects":[{"type":"dashboard","id":"dash-msa-monitoring"}],"includeReferencesDeep":true}' \
  > k3s/kibana/msa-monitoring-dashboard.ndjson
```

---

# 실시간 탐지 대시보드 (speed layer)

speed layer가 적재하는 실시간 보안 탐지를 시각화한다. 생성기는
`build_detections_dashboard.py`, 산출물은 `msa-detections-dashboard.ndjson`.

## 데이터 소스

- `msa-detections` (시간 필드 `detected_at`) — speed-consumer/dos-aggregator 탐지
  - `attack_type="auth_bypass"`: `service`, `ip`, `uri`, `user_id`
  - `attack_type="dos"`: `ip`, `req_count`, `baseline_avg`, `threshold`, `multiplier`
- `msa-pod-status` (시간 필드 `timestamp`) — **`pod-status-watcher` Deployment가 적재(실측·이벤트 드리븐)**
  - speed-consumer Deployment를 k8s watch API로 구독 → KEDA 스케일 순간 즉시 적재(+ ~20초 하트비트)
  - `current_replicas`(=`readyReplicas`), `desired_replicas`
  - `cron_min`/`window`: ScaledObject(`speed-consumer-scaler`)의 현재 KST cron 구간에서 조회(단일 출처)
  - `below_min`
- `msa-throughput` (시간 필드 `timestamp`) — **`log-throughput-reporter` Deployment가 적재(실측)**
  - msa-logs를 독립 group으로 소비해 5초마다 서비스별 처리량을 보고
  - `service`, `count`, `total`, `rate_per_sec`

## 포함 객체 (`msa-detections-dashboard.ndjson`)

- **KPI(metric)**: 전체 탐지 / DoS / 인증 우회 / 차단 IP 수(DoS, `ip.keyword` cardinality)
- **실시간 로그 처리량**: 현재 처리율(`rate_per_sec` 최신값) ·
  서비스별 처리량 area(`count` 합, `service.keyword` 분할 stacked)
- **현재 Pod 수 vs cron 최솟값**: 현재 Pod 수(`current_replicas` 최신값) ·
  cron 최솟값(`cron_min` 최신값) · 시간축 비교 line(step-after)
- **탐지 추세**: `detected_at` date_histogram, `attack_type.keyword` 분할 stacked bar
- **공격 IP Top 15**: `ip.keyword` terms + 탐지 건수 / 최대 req_count / 임계값 (단독 행)
- **인증 우회 상세**: 저장 검색(Discover) `attack_type="auth_bypass"`,
  컬럼 `service`/`ip`/`uri`/`user_id` (단독 행)
- **대시보드** `MSA 실시간 보안 탐지`: 기본 기간 `now-1h ~ now`,
  자동 새로고침 15초(`refreshInterval pause:false`)

## 빌드 / Import / Export

```bash
# 생성
python3 k3s/kibana/build_detections_dashboard.py > /tmp/detections-import.ndjson
# import
curl -s "http://<node-ip>:30731/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: true" --form file=@/tmp/detections-import.ndjson
# export(깊은 참조 포함) → repo 갱신
curl -s "http://<node-ip>:30731/api/saved_objects/_export" \
  -H "kbn-xsrf: true" -H "Content-Type: application/json" \
  -d '{"objects":[{"type":"dashboard","id":"dash-msa-detections"}],"includeReferencesDeep":true}' \
  > k3s/kibana/msa-detections-dashboard.ndjson
```

## 파이프라인 / 데모 데이터

- `현재 Pod 수`·`cron 최솟값`은 **실측**(watch 워처 `../pod-status-watcher.yaml`, KEDA ScaledObject 연동).
  speed-consumer/dos-aggregator/throughput-reporter/traffic-sim은 데스크탑 노드
  (`ubuntu-virtual-machine`)에 고정 배포. KEDA는 ghcr 접근 가능한 control-plane 노드에 고정.
- 실시간 흐름은 `../traffic-sim.yaml`이 msa-logs에 상시 트래픽을 발행해 살린다.
- `msa-detections` 탐지 문서는 트래픽이 없을 때 데모용으로 `seed_sample_detections.py`로도
  채울 수 있다(최근 1시간 분포). 실파이프라인이 트래픽을 받으면 같은 인덱스에 실제 탐지가 쌓인다.
