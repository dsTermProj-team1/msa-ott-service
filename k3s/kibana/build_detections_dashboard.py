#!/usr/bin/env python3
"""
실시간(speed layer) 보안 탐지 대시보드 Saved Objects(NDJSON) 생성기.

데이터 소스: ES 인덱스 msa-detections (speed-consumer=auth_bypass, dos-aggregator=dos).
시간 필드: detected_at. 동적 매핑이므로 terms/cardinality 집계는 *.keyword 사용.

사용:
  python3 build_detections_dashboard.py > /tmp/detections-import.ndjson
  curl -s "http://localhost:30731/api/saved_objects/_import?overwrite=true" \
       -H "kbn-xsrf: true" --form file=@/tmp/detections-import.ndjson
  # export(깊은 참조 포함) 후 repo 의 msa-detections-dashboard.ndjson 으로 커밋
"""
import json

CORE = "8.13.0"
DV_ID = "dv-msa-detections"
DV_POD_ID = "dv-msa-pod-status"
INDEX_REF = "kibanaSavedObjectMeta.searchSourceJSON.index"

objs = []


def search_source(query=""):
    return json.dumps({
        "query": {"query": query, "language": "kuery"},
        "filter": [],
        "indexRefName": INDEX_REF,
    })


def viz(vid, title, vis_state, query="", dv=DV_ID):
    objs.append({
        "attributes": {
            "title": title,
            "description": "",
            "uiStateJSON": "{}",
            "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_source(query)},
            "visState": json.dumps(vis_state, ensure_ascii=False),
        },
        "coreMigrationVersion": CORE,
        "id": vid,
        "managed": False,
        "references": [{"id": dv, "name": INDEX_REF, "type": "index-pattern"}],
        "type": "visualization",
        "typeMigrationVersion": "8.5.0",
    })


# ── 데이터 뷰 ────────────────────────────────────────────────
objs.append({
    "attributes": {"timeFieldName": "detected_at", "title": "msa-detections"},
    "coreMigrationVersion": CORE,
    "id": DV_ID,
    "managed": False,
    "references": [],
    "type": "index-pattern",
    "typeMigrationVersion": "8.0.0",
})

objs.append({
    "attributes": {"timeFieldName": "timestamp", "title": "msa-pod-status"},
    "coreMigrationVersion": CORE,
    "id": DV_POD_ID,
    "managed": False,
    "references": [],
    "type": "index-pattern",
    "typeMigrationVersion": "8.0.0",
})


# ── KPI 메트릭 (각 패널마다 KQL 필터) ─────────────────────────
def metric_state(title, agg):
    return {
        "title": title,
        "type": "metric",
        "params": {
            "addTooltip": True, "addLegend": False, "type": "metric",
            "metric": {
                "percentageMode": False, "useRanges": False,
                "colorSchema": "Green to Red", "metricColorMode": "None",
                "colorsRange": [{"from": 0, "to": 100000}],
                "labels": {"show": True}, "invertColors": False,
                "style": {"bgFill": "#000", "bgColor": False, "labelColor": False,
                          "subText": "", "fontSize": 36},
            },
        },
        "aggs": [agg],
    }


viz("viz-detect-kpi-total", "전체 탐지 (최근)",
    metric_state("전체 탐지", {"id": "1", "enabled": True, "type": "count",
                              "schema": "metric", "params": {"customLabel": "전체 탐지"}}))

viz("viz-detect-kpi-dos", "DoS 탐지",
    metric_state("DoS 탐지", {"id": "1", "enabled": True, "type": "count",
                             "schema": "metric", "params": {"customLabel": "DoS 탐지"}}),
    query='attack_type.keyword:"dos"')

viz("viz-detect-kpi-auth", "인증 우회 탐지",
    metric_state("인증 우회", {"id": "1", "enabled": True, "type": "count",
                            "schema": "metric", "params": {"customLabel": "인증 우회"}}),
    query='attack_type.keyword:"auth_bypass"')

viz("viz-detect-kpi-blocked", "차단 IP 수 (DoS)",
    metric_state("차단 IP 수", {"id": "1", "enabled": True, "type": "cardinality",
                             "schema": "metric",
                             "params": {"field": "ip.keyword", "customLabel": "차단 IP 수"}}),
    query='attack_type.keyword:"dos"')


# ── 탐지 추세 (시간축 stacked bar, attack_type 분할) ──────────
viz("viz-detect-trend", "탐지 추세 (유형별)", {
    "title": "탐지 추세 (유형별)",
    "type": "histogram",
    "params": {
        "type": "histogram",
        "grid": {"categoryLines": False},
        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                          "show": True, "scale": {"type": "linear"},
                          "labels": {"show": True, "filter": True, "truncate": 100},
                          "title": {"text": "시각"}}],
        "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                       "position": "left", "show": True,
                       "scale": {"type": "linear", "mode": "normal"},
                       "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                       "title": {"text": "탐지 건수"}}],
        "seriesParams": [{"show": True, "type": "histogram", "mode": "stacked",
                          "data": {"label": "탐지 건수", "id": "1"},
                          "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
                          "lineWidth": 2, "showCircles": True}],
        "addTooltip": True, "addLegend": True, "legendPosition": "right",
        "times": [], "addTimeMarker": False, "labels": {},
        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full",
                          "color": "#E7664C"},
    },
    "aggs": [
        {"id": "1", "enabled": True, "type": "count", "schema": "metric",
         "params": {"customLabel": "탐지 건수"}},
        {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
         "params": {"field": "detected_at", "useNormalizedEsInterval": True,
                    "interval": "auto", "drop_partials": False, "min_doc_count": 1,
                    "extended_bounds": {}}},
        {"id": "3", "enabled": True, "type": "terms", "schema": "group",
         "params": {"field": "attack_type.keyword", "orderBy": "1", "order": "desc",
                    "size": 5, "otherBucket": False, "missingBucket": False}},
    ],
})


# ── 현재 Pod 수 / cron 최솟값 (speed-consumer, msa-pod-status) ─
def latest_metric(field, label):
    return {"id": "1", "enabled": True, "type": "top_hits", "schema": "metric",
            "params": {"field": field, "aggregate": "concat", "size": 1,
                       "sortField": "timestamp", "sortOrder": "desc",
                       "customLabel": label}}


viz("viz-pod-current", "현재 speed-consumer Pod 수",
    metric_state("현재 Pod 수", latest_metric("current_replicas", "현재 Pod 수")),
    dv=DV_POD_ID)

viz("viz-pod-cronmin", "cron 최솟값 (현재 구간)",
    metric_state("cron 최솟값", latest_metric("cron_min", "cron 최솟값")),
    dv=DV_POD_ID)

viz("viz-pod-trend", "Pod 수 vs cron 최솟값", {
    "title": "Pod 수 vs cron 최솟값",
    "type": "line",
    "params": {
        "type": "line",
        "grid": {"categoryLines": False},
        "categoryAxes": [{"id": "CategoryAxis-1", "type": "category", "position": "bottom",
                          "show": True, "scale": {"type": "linear"},
                          "labels": {"show": True, "filter": True, "truncate": 100},
                          "title": {"text": "시각"}}],
        "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1", "type": "value",
                       "position": "left", "show": True,
                       "scale": {"type": "linear", "mode": "normal"},
                       "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                       "title": {"text": "Pod 수"}}],
        "seriesParams": [
            {"show": True, "type": "line", "mode": "normal",
             "data": {"label": "현재 Pod 수", "id": "1"},
             "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
             "interpolate": "step-after", "lineWidth": 2, "showCircles": True},
            {"show": True, "type": "line", "mode": "normal",
             "data": {"label": "cron 최솟값", "id": "3"},
             "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
             "interpolate": "step-after", "lineWidth": 2, "showCircles": False},
        ],
        "addTooltip": True, "addLegend": True, "legendPosition": "right",
        "times": [], "addTimeMarker": False, "labels": {},
        "thresholdLine": {"show": False, "value": 10, "width": 1, "style": "full",
                          "color": "#E7664C"},
    },
    "aggs": [
        {"id": "1", "enabled": True, "type": "max", "schema": "metric",
         "params": {"field": "current_replicas", "customLabel": "현재 Pod 수"}},
        {"id": "3", "enabled": True, "type": "max", "schema": "metric",
         "params": {"field": "cron_min", "customLabel": "cron 최솟값"}},
        {"id": "2", "enabled": True, "type": "date_histogram", "schema": "segment",
         "params": {"field": "timestamp", "useNormalizedEsInterval": True,
                    "interval": "auto", "drop_partials": False, "min_doc_count": 1,
                    "extended_bounds": {}}},
    ],
}, dv=DV_POD_ID)


# ── 공격 IP Top N 테이블 ─────────────────────────────────────
viz("viz-detect-topip", "공격 IP Top 15", {
    "title": "공격 IP Top 15",
    "type": "table",
    "params": {
        "perPage": 10, "showPartialRows": False, "showMetricsAtAllLevels": False,
        "showTotal": False, "totalFunc": "sum", "percentageCol": "",
    },
    "aggs": [
        {"id": "1", "enabled": True, "type": "count", "schema": "metric",
         "params": {"customLabel": "탐지 건수"}},
        {"id": "2", "enabled": True, "type": "max", "schema": "metric",
         "params": {"field": "req_count", "customLabel": "최대 요청수(DoS)"}},
        {"id": "4", "enabled": True, "type": "max", "schema": "metric",
         "params": {"field": "threshold", "customLabel": "임계값"}},
        {"id": "3", "enabled": True, "type": "terms", "schema": "bucket",
         "params": {"field": "ip.keyword", "orderBy": "1", "order": "desc",
                    "size": 15, "otherBucket": False, "missingBucket": False}},
    ],
})


# ── 인증 우회 상세 (Discover 저장 검색) ───────────────────────
objs.append({
    "attributes": {
        "title": "인증 우회 상세 (최근)",
        "description": "",
        "hits": 0,
        "columns": ["service", "ip", "uri", "user_id"],
        "sort": [["detected_at", "desc"]],
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": search_source('attack_type.keyword:"auth_bypass"')
        },
    },
    "coreMigrationVersion": CORE,
    "id": "search-detect-auth",
    "managed": False,
    "references": [{"id": DV_ID, "name": INDEX_REF, "type": "index-pattern"}],
    "type": "search",
    "typeMigrationVersion": "8.0.0",
})


# ── 대시보드 ─────────────────────────────────────────────────
panels = [
    # KPI 행
    ("viz-detect-kpi-total",   "visualization", "1", 0,  0, 12, 8),
    ("viz-detect-kpi-dos",     "visualization", "2", 12, 0, 12, 8),
    ("viz-detect-kpi-auth",    "visualization", "3", 24, 0, 12, 8),
    ("viz-detect-kpi-blocked", "visualization", "4", 36, 0, 12, 8),
    # Pod 수 행 (현재 / cron 최솟값 / 시간축 비교)
    ("viz-pod-current",        "visualization", "8", 0,  8, 12, 10),
    ("viz-pod-cronmin",        "visualization", "9", 12, 8, 12, 10),
    ("viz-pod-trend",          "visualization", "10", 24, 8, 24, 10),
    # 탐지 추세
    ("viz-detect-trend",       "visualization", "5", 0, 18, 48, 14),
    # 공격 IP Top 15 (단독 행)
    ("viz-detect-topip",       "visualization", "6", 0, 32, 48, 16),
    # 인증 우회 상세 (단독 행)
    ("search-detect-auth",     "search",        "7", 0, 48, 48, 16),
]
panels_json, refs = [], []
for oid, otype, pi, x, y, w, h in panels:
    rn = f"panel_{pi}"
    panels_json.append({
        "version": CORE, "type": otype,
        "gridData": {"x": x, "y": y, "w": w, "h": h, "i": pi},
        "panelIndex": pi, "embeddableConfig": {}, "panelRefName": rn,
    })
    refs.append({"id": oid, "name": rn, "type": otype})

objs.append({
    "attributes": {
        "title": "MSA 실시간 보안 탐지",
        "description": "speed layer 실시간 탐지(msa-detections) — auth_bypass / dos",
        "hits": 0,
        "version": 1,
        "timeRestore": True,
        "timeFrom": "now-1h",
        "timeTo": "now",
        "refreshInterval": {"pause": False, "value": 15000},
        "optionsJSON": json.dumps({"useMargins": True, "syncColors": False,
                                   "syncCursor": True, "syncTooltips": False,
                                   "hidePanelTitles": False}),
        "panelsJSON": json.dumps(panels_json, ensure_ascii=False),
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps({"query": {"query": "", "language": "kuery"},
                                            "filter": []})
        },
    },
    "coreMigrationVersion": CORE,
    "id": "dash-msa-detections",
    "managed": False,
    "references": refs,
    "type": "dashboard",
    "typeMigrationVersion": "8.9.0",
})


for o in objs:
    print(json.dumps(o, ensure_ascii=False))
