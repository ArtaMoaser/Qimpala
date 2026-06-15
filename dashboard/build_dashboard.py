#!/usr/bin/env python3
"""
Generate an Elasticsearch / Kibana 7.x saved-objects export for the three
Qimpala indices. Run it to (re)produce ``qimpala-kibana-dashboard.ndjson``,
then import that file in Kibana via:

    Stack Management -> Saved Objects -> Import

It creates:
  * 3 index patterns (impala-query-summary / -metrics / -analysis)
  * ~11 visualizations
  * 1 dashboard "Impala Query Monitoring" tying them together

The summary & analysis index patterns use ``start_time`` as their time
field, so every chart counts/buckets queries by when they actually ran -
not by when the collector scraped them. Because the collector writes one
document per query (``_id = query_id``), a query count is simply a document
count and never double-counts a long-running query scraped on many cycles.
"""

import json
import os

KIBANA_VERSION = "7.17.0"

IDX_SUMMARY = "impala-query-summary"
IDX_METRICS = "impala-query-metrics"
IDX_ANALYSIS = "impala-query-analysis"
IDX_HDFS = "hdfs-namenode-metrics"
IDX_KUDU = "kudu-master-metrics"

objects = []


# ----------------------------------------------------------------------
# index patterns
# ----------------------------------------------------------------------

def index_pattern(pattern_id, title, time_field):
    objects.append({
        "id": pattern_id,
        "type": "index-pattern",
        "version": "1",
        "migrationVersion": {"index-pattern": "7.11.0"},
        "references": [],
        "attributes": {
            "title": title,
            "timeFieldName": time_field,
        },
    })


index_pattern(IDX_SUMMARY, IDX_SUMMARY, "start_time")
index_pattern(IDX_METRICS, IDX_METRICS, "start_time")
index_pattern(IDX_ANALYSIS, IDX_ANALYSIS, "start_time")
index_pattern(IDX_HDFS, IDX_HDFS, "@timestamp")
index_pattern(IDX_KUDU, IDX_KUDU, "@timestamp")


# ----------------------------------------------------------------------
# visualization helper
# ----------------------------------------------------------------------

def search_source(query=""):
    return json.dumps({
        "query": {"language": "kuery", "query": query},
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index",
    })


def visualization(viz_id, title, index_pattern_id, vis_state, query=""):
    objects.append({
        "id": viz_id,
        "type": "visualization",
        "version": "1",
        "migrationVersion": {"visualization": "7.14.0"},
        "references": [{
            "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
            "type": "index-pattern",
            "id": index_pattern_id,
        }],
        "attributes": {
            "title": title,
            "visState": json.dumps(vis_state),
            "uiStateJSON": "{}",
            "description": "",
            "version": 1,
            "kibanaSavedObjectMeta": {"searchSourceJSON": search_source(query)},
        },
    })


def agg_count(agg_id="1"):
    return {"id": agg_id, "enabled": True, "type": "count",
            "schema": "metric", "params": {}}


def agg_metric(agg_id, kind, field, schema="metric"):
    return {"id": agg_id, "enabled": True, "type": kind, "schema": schema,
            "params": {"field": field}}


def agg_terms(agg_id, field, size=10, order_by="1", schema="segment"):
    return {"id": agg_id, "enabled": True, "type": "terms", "schema": schema,
            "params": {"field": field, "orderBy": order_by, "order": "desc",
                       "size": size, "otherBucket": False,
                       "missingBucket": False}}


def agg_date_histogram(agg_id, field="start_time", schema="segment"):
    return {"id": agg_id, "enabled": True, "type": "date_histogram",
            "schema": schema,
            "params": {"field": field, "useNormalizedEsInterval": True,
                       "interval": "auto", "drop_partials": False,
                       "min_doc_count": 1, "extended_bounds": {}}}


def timeseries_line(viz_id, title, index_pattern_id, metrics, y_title,
                    split_by="host", time_field="@timestamp", chart="line"):
    """A line/area chart of one or more metrics over @timestamp, optionally
    split into one series per host. ``metrics`` is a list of (kind, field)."""
    aggs, series = [], []
    for i, (kind, field) in enumerate(metrics, start=1):
        aggs.append(agg_metric(str(i), kind, field))
        series.append({"show": True, "type": chart, "mode": "normal",
                       "data": {"label": "%s %s" % (kind, field), "id": str(i)},
                       "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
                       "showCircles": False})
    aggs.append(agg_date_histogram(str(len(metrics) + 1), field=time_field))
    if split_by:
        aggs.append(agg_terms(str(len(metrics) + 2), split_by, size=10,
                              order_by="1", schema="group"))
    visualization(viz_id, title, index_pattern_id, {
        "title": title, "type": chart,
        "aggs": aggs,
        "params": {"type": chart, "grid": {"categoryLines": False},
                   "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                     "position": "bottom", "show": True,
                                     "scale": {"type": "linear"},
                                     "labels": {"show": True, "truncate": 100},
                                     "title": {}}],
                   "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                                  "type": "value", "position": "left",
                                  "show": True,
                                  "scale": {"type": "linear", "mode": "normal"},
                                  "labels": {"show": True, "rotate": 0,
                                             "filter": False, "truncate": 100},
                                  "title": {"text": y_title}}],
                   "seriesParams": series, "addTooltip": True, "addLegend": True,
                   "legendPosition": "right", "times": [], "addTimeMarker": False}},
    )


# ----------------------------------------------------------------------
# visualizations
# ----------------------------------------------------------------------

# 1. total distinct queries (one doc per query, so a plain count is exact)
visualization("qimpala-total-queries", "Total Queries", IDX_SUMMARY, {
    "title": "Total Queries", "type": "metric",
    "aggs": [agg_count("1")],
    "params": {"metric": {
        "percentageMode": False, "useRanges": False, "colorSchema": "Green to Red",
        "metricColorMode": "None", "labels": {"show": True},
        "style": {"fontSize": 60}, "metrics": []}},
})

# 2. queries over time (when they actually ran)
visualization("qimpala-queries-over-time", "Queries Over Time", IDX_SUMMARY, {
    "title": "Queries Over Time", "type": "histogram",
    "aggs": [agg_count("1"), agg_date_histogram("2")],
    "params": {"type": "histogram", "grid": {"categoryLines": False},
               "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                 "position": "bottom", "show": True,
                                 "scale": {"type": "linear"},
                                 "labels": {"show": True, "truncate": 100},
                                 "title": {}}],
               "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                              "type": "value", "position": "left", "show": True,
                              "scale": {"type": "linear", "mode": "normal"},
                              "labels": {"show": True, "rotate": 0,
                                         "filter": False, "truncate": 100},
                              "title": {"text": "Query count"}}],
               "seriesParams": [{"show": True, "type": "histogram",
                                 "mode": "stacked", "data": {"label": "Count",
                                 "id": "1"}, "valueAxis": "ValueAxis-1",
                                 "drawLinesBetweenPoints": True,
                                 "showCircles": True}],
               "addTooltip": True, "addLegend": True, "legendPosition": "right",
               "times": [], "addTimeMarker": False},
})

# 3. queries by state
visualization("qimpala-by-state", "Queries by State", IDX_SUMMARY, {
    "title": "Queries by State", "type": "pie",
    "aggs": [agg_count("1"), agg_terms("2", "query_state", size=10)],
    "params": {"type": "pie", "addTooltip": True, "addLegend": True,
               "legendPosition": "right", "isDonut": True,
               "labels": {"show": False, "values": True, "last_level": True,
                          "truncate": 100}},
})

# 4. top users by query count
visualization("qimpala-by-user", "Top Users by Query Count", IDX_SUMMARY, {
    "title": "Top Users by Query Count", "type": "horizontal_bar",
    "aggs": [agg_count("1"), agg_terms("2", "user", size=15)],
    "params": {"type": "horizontal_bar",
               "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                 "position": "left", "show": True,
                                 "scale": {"type": "linear"},
                                 "labels": {"show": True, "truncate": 100},
                                 "title": {}}],
               "valueAxes": [{"id": "ValueAxis-1", "name": "BottomAxis-1",
                              "type": "value", "position": "bottom",
                              "show": True,
                              "scale": {"type": "linear", "mode": "normal"},
                              "labels": {"show": True, "rotate": 75,
                                         "filter": True, "truncate": 100},
                              "title": {"text": "Query count"}}],
               "seriesParams": [{"show": True, "type": "histogram",
                                 "mode": "normal", "data": {"label": "Count",
                                 "id": "1"}, "valueAxis": "ValueAxis-1"}],
               "addTooltip": True, "addLegend": True, "legendPosition": "right"},
})

# 5. queries by request pool
visualization("qimpala-by-pool", "Queries by Request Pool", IDX_SUMMARY, {
    "title": "Queries by Request Pool", "type": "pie",
    "aggs": [agg_count("1"), agg_terms("2", "request_pool", size=10)],
    "params": {"type": "pie", "addTooltip": True, "addLegend": True,
               "legendPosition": "right", "isDonut": True,
               "labels": {"show": False, "values": True, "last_level": True,
                          "truncate": 100}},
})

# 6. queries by type
visualization("qimpala-by-type", "Queries by Type", IDX_SUMMARY, {
    "title": "Queries by Type", "type": "pie",
    "aggs": [agg_count("1"), agg_terms("2", "query_type", size=10)],
    "params": {"type": "pie", "addTooltip": True, "addLegend": True,
               "legendPosition": "right", "isDonut": True,
               "labels": {"show": False, "values": True, "last_level": True,
                          "truncate": 100}},
})

# 7. avg & max duration over time
visualization("qimpala-duration-over-time", "Query Duration Over Time (ms)",
              IDX_SUMMARY, {
    "title": "Query Duration Over Time (ms)", "type": "line",
    "aggs": [agg_metric("1", "avg", "duration_ms"),
             agg_metric("3", "max", "duration_ms"),
             agg_date_histogram("2")],
    "params": {"type": "line", "grid": {"categoryLines": False},
               "categoryAxes": [{"id": "CategoryAxis-1", "type": "category",
                                 "position": "bottom", "show": True,
                                 "scale": {"type": "linear"},
                                 "labels": {"show": True, "truncate": 100},
                                 "title": {}}],
               "valueAxes": [{"id": "ValueAxis-1", "name": "LeftAxis-1",
                              "type": "value", "position": "left", "show": True,
                              "scale": {"type": "linear", "mode": "normal"},
                              "labels": {"show": True, "rotate": 0,
                                         "filter": False, "truncate": 100},
                              "title": {"text": "Duration (ms)"}}],
               "seriesParams": [
                   {"show": True, "type": "line", "mode": "normal",
                    "data": {"label": "Average duration_ms", "id": "1"},
                    "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
                    "showCircles": True},
                   {"show": True, "type": "line", "mode": "normal",
                    "data": {"label": "Max duration_ms", "id": "3"},
                    "valueAxis": "ValueAxis-1", "drawLinesBetweenPoints": True,
                    "showCircles": True}],
               "addTooltip": True, "addLegend": True, "legendPosition": "right",
               "times": [], "addTimeMarker": False},
})

# 8. slowest queries table
visualization("qimpala-slowest-queries", "Slowest Queries", IDX_SUMMARY, {
    "title": "Slowest Queries", "type": "table",
    "aggs": [
        agg_metric("1", "max", "duration_ms"),
        agg_terms("2", "query_id", size=25, order_by="1"),
        agg_terms("3", "user", size=1, order_by="1", schema="bucket"),
        agg_terms("4", "request_pool", size=1, order_by="1", schema="bucket"),
    ],
    "params": {"perPage": 15, "showPartialRows": False,
               "showMetricsAtAllLevels": False, "showTotal": False,
               "totalFunc": "sum", "percentageCol": ""},
})

# 9. root cause breakdown (analysis index)
visualization("qimpala-root-cause", "Suspected Root Cause", IDX_ANALYSIS, {
    "title": "Suspected Root Cause", "type": "pie",
    "aggs": [agg_count("1"), agg_terms("2", "suspected_root_cause", size=15)],
    "params": {"type": "pie", "addTooltip": True, "addLegend": True,
               "legendPosition": "right", "isDonut": True,
               "labels": {"show": False, "values": True, "last_level": True,
                          "truncate": 100}},
})

# 10. severity breakdown (analysis index)
visualization("qimpala-severity", "Severity Breakdown", IDX_ANALYSIS, {
    "title": "Severity Breakdown", "type": "pie",
    "aggs": [agg_count("1"), agg_terms("2", "severity", size=10)],
    "params": {"type": "pie", "addTooltip": True, "addLegend": True,
               "legendPosition": "right", "isDonut": True,
               "labels": {"show": False, "values": True, "last_level": True,
                          "truncate": 100}},
})

# 11. root cause x severity table
visualization("qimpala-findings-table", "Root Cause by Severity",
              IDX_ANALYSIS, {
    "title": "Root Cause by Severity", "type": "table",
    "aggs": [
        agg_count("1"),
        agg_terms("2", "suspected_root_cause", size=20, order_by="1"),
        agg_terms("3", "severity", size=5, order_by="1", schema="bucket"),
    ],
    "params": {"perPage": 15, "showPartialRows": False,
               "showMetricsAtAllLevels": False, "showTotal": True,
               "totalFunc": "sum", "percentageCol": ""},
})


# ---- HDFS NameNode health (time series) ------------------------------

# 12. capacity used %
timeseries_line("qimpala-hdfs-capacity", "HDFS Capacity Used (%)", IDX_HDFS,
                [("max", "capacity_used_pct")], "Capacity used (%)")

# 13. block health (missing / corrupt / under-replicated) - max per bucket
timeseries_line("qimpala-hdfs-block-health",
                "HDFS Block Health (missing / corrupt / under-replicated)",
                IDX_HDFS,
                [("max", "missing_blocks"), ("max", "corrupt_blocks"),
                 ("max", "under_replicated_blocks")],
                "Block count", split_by=None)

# 14. datanode liveness
timeseries_line("qimpala-hdfs-datanodes",
                "HDFS DataNodes (live vs dead)", IDX_HDFS,
                [("max", "num_live_datanodes"), ("max", "num_dead_datanodes")],
                "DataNodes", split_by=None)

# 15. NameNode RPC latency
timeseries_line("qimpala-hdfs-rpc",
                "HDFS NameNode RPC Latency (ms) & Call Queue", IDX_HDFS,
                [("max", "rpc_queue_time_avg_ms"),
                 ("max", "rpc_processing_time_avg_ms"),
                 ("max", "call_queue_length")],
                "ms / queue length")

# 16. NameNode JVM heap & GC
timeseries_line("qimpala-hdfs-jvm",
                "HDFS NameNode Heap Used (%)", IDX_HDFS,
                [("max", "jvm_heap_used_pct")], "Heap used (%)")

# 17. current HA roles / status table
visualization("qimpala-hdfs-status", "HDFS NameNode Status", IDX_HDFS, {
    "title": "HDFS NameNode Status", "type": "table",
    "aggs": [
        agg_metric("1", "max", "capacity_used_pct"),
        agg_metric("4", "max", "num_dead_datanodes"),
        agg_metric("5", "max", "missing_blocks"),
        agg_terms("2", "host", size=10, order_by="1"),
        agg_terms("3", "ha_state", size=3, order_by="1", schema="bucket"),
    ],
    "params": {"perPage": 10, "showPartialRows": False,
               "showMetricsAtAllLevels": False, "showTotal": False,
               "totalFunc": "sum", "percentageCol": ""},
})


# ---- Kudu master health (time series) --------------------------------

# 18. RPC incoming queue time (p99 / mean) - scan/admission pressure signal
timeseries_line("qimpala-kudu-rpc-queue",
                "Kudu Master RPC Incoming Queue Time (us)", IDX_KUDU,
                [("max", "rpc_incoming_queue_time_p99_us"),
                 ("avg", "rpc_incoming_queue_time_mean_us")],
                "microseconds")

# 19. RPC queue overflow (dropped/rejected RPCs)
timeseries_line("qimpala-kudu-overflow",
                "Kudu Master RPC Queue Overflow", IDX_KUDU,
                [("max", "rpc_queue_overflow")], "overflow count")

# 20. error / warning log messages
timeseries_line("qimpala-kudu-logs",
                "Kudu Master Error & Warning Log Messages", IDX_KUDU,
                [("max", "glog_error_messages"),
                 ("max", "glog_warning_messages")], "message count")

# 21. block cache hit ratio
timeseries_line("qimpala-kudu-cache",
                "Kudu Master Block Cache Hit Ratio", IDX_KUDU,
                [("avg", "block_cache_hit_ratio")], "hit ratio")

# 22. threads running
timeseries_line("qimpala-kudu-threads",
                "Kudu Master Threads Running", IDX_KUDU,
                [("max", "threads_running")], "threads")

# 23. current master status table (leader, errors, queue)
visualization("qimpala-kudu-status", "Kudu Master Status", IDX_KUDU, {
    "title": "Kudu Master Status", "type": "table",
    "aggs": [
        agg_metric("1", "max", "rpc_incoming_queue_time_p99_us"),
        agg_metric("4", "max", "glog_error_messages"),
        agg_metric("5", "max", "rpc_queue_overflow"),
        agg_terms("2", "host", size=10, order_by="1"),
        agg_terms("3", "is_leader", size=2, order_by="1", schema="bucket"),
    ],
    "params": {"perPage": 10, "showPartialRows": False,
               "showMetricsAtAllLevels": False, "showTotal": False,
               "totalFunc": "sum", "percentageCol": ""},
})


# ----------------------------------------------------------------------
# dashboard
# ----------------------------------------------------------------------
# Kibana 7 grid is 48 columns wide. (x, y, w, h) per panel.
LAYOUT = [
    # --- query overview ---
    ("qimpala-total-queries",      0,  0, 12,  8),
    ("qimpala-root-cause",        12,  0, 18,  8),
    ("qimpala-severity",          30,  0, 18,  8),
    ("qimpala-queries-over-time",  0,  8, 48, 10),
    ("qimpala-duration-over-time", 0, 18, 24, 10),
    ("qimpala-by-state",          24, 18, 12, 10),
    ("qimpala-by-pool",           36, 18, 12, 10),
    ("qimpala-by-user",            0, 28, 24, 12),
    ("qimpala-slowest-queries",   24, 28, 24, 12),
    ("qimpala-by-type",            0, 40, 16, 10),
    ("qimpala-findings-table",    16, 40, 32, 10),
    # --- HDFS NameNode health ---
    ("qimpala-hdfs-status",        0, 50, 24, 10),
    ("qimpala-hdfs-capacity",     24, 50, 24, 10),
    ("qimpala-hdfs-datanodes",     0, 60, 16, 10),
    ("qimpala-hdfs-block-health", 16, 60, 16, 10),
    ("qimpala-hdfs-jvm",          32, 60, 16, 10),
    ("qimpala-hdfs-rpc",           0, 70, 48, 10),
    # --- Kudu master health ---
    ("qimpala-kudu-status",        0, 80, 24, 10),
    ("qimpala-kudu-rpc-queue",    24, 80, 24, 10),
    ("qimpala-kudu-overflow",      0, 90, 16, 10),
    ("qimpala-kudu-logs",         16, 90, 16, 10),
    ("qimpala-kudu-cache",        32, 90, 16, 10),
    ("qimpala-kudu-threads",       0,100, 24, 10),
]

panels, references = [], []
for i, (viz_id, x, y, w, h) in enumerate(LAYOUT, start=1):
    panel_ref = "panel_%d" % i
    panels.append({
        "version": KIBANA_VERSION, "type": "visualization",
        "gridData": {"x": x, "y": y, "w": w, "h": h, "i": str(i)},
        "panelIndex": str(i), "embeddableConfig": {}, "panelRefName": panel_ref,
    })
    references.append({"name": panel_ref, "type": "visualization", "id": viz_id})

objects.append({
    "id": "qimpala-dashboard",
    "type": "dashboard",
    "version": "1",
    "migrationVersion": {"dashboard": "7.14.0"},
    "references": references,
    "attributes": {
        "title": "Impala Query Monitoring",
        "hits": 0,
        "description": "Qimpala: query volume, durations, and rule-engine "
                       "root-cause analysis across Impala coordinators.",
        "panelsJSON": json.dumps(panels),
        "optionsJSON": json.dumps({"useMargins": True, "hidePanelTitles": False}),
        "version": 1,
        "timeRestore": True,
        "timeTo": "now",
        "timeFrom": "now-7d",
        "refreshInterval": {"pause": False, "value": 60000},
        "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(
            {"query": {"language": "kuery", "query": ""}, "filter": []})},
    },
})


# ----------------------------------------------------------------------
# write NDJSON (one object per line + export footer, like a real export)
# ----------------------------------------------------------------------

def main():
    out_path = os.path.join(os.path.dirname(__file__),
                            "qimpala-kibana-dashboard.ndjson")
    with open(out_path, "w", encoding="utf-8") as f:
        for obj in objects:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        f.write(json.dumps({"exportedCount": len(objects),
                            "missingRefCount": 0,
                            "missingReferences": []}) + "\n")
    print("wrote %d saved objects to %s" % (len(objects), out_path))


if __name__ == "__main__":
    main()
