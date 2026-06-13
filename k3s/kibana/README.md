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
