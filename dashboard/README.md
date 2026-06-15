# Qimpala Kibana Dashboard

`qimpala-kibana-dashboard.ndjson` is a Kibana 7.x saved-objects export that
builds an **Impala Query Monitoring** dashboard on top of the three indices
the collector writes.

## Import

1. Open Kibana → **Stack Management → Saved Objects → Import**.
2. Select `qimpala-kibana-dashboard.ndjson`.
3. Choose *"Automatically overwrite conflicts"* if you are re-importing.
4. Open **Dashboard → Impala Query Monitoring**.

It creates five index patterns (`impala-query-summary`, `impala-query-metrics`,
`impala-query-analysis`, `hdfs-namenode-metrics`, `kudu-master-metrics`),
23 visualizations, and the dashboard linking them.

## What's on it

### Query monitoring
| Panel | Source | Shows |
|-------|--------|-------|
| Total Queries | summary | exact distinct query count |
| Queries Over Time | summary | volume bucketed by real start time |
| Query Duration Over Time | summary | avg & max `duration_ms` |
| Queries by State / Pool / Type | summary | breakdowns |
| Top Users by Query Count | summary | busiest users |
| Slowest Queries | summary | top 25 by max duration, with user/pool |
| Suspected Root Cause | analysis | rule-engine verdict distribution |
| Severity Breakdown | analysis | ok / low / medium / high / critical |
| Root Cause by Severity | analysis | cross-tab table |

### HDFS NameNode health (time series)
| Panel | Shows |
|-------|-------|
| HDFS NameNode Status | per-node HA role, capacity %, dead DN, missing blocks |
| HDFS Capacity Used (%) | capacity trend per node |
| HDFS DataNodes (live vs dead) | datanode liveness |
| HDFS Block Health | missing / corrupt / under-replicated blocks |
| HDFS Heap Used (%) | NameNode JVM heap pressure |
| HDFS RPC Latency & Call Queue | RPC queue/processing time and call-queue length |

### Kudu master health (time series)
| Panel | Shows |
|-------|-------|
| Kudu Master Status | leader flag, p99 RPC queue time, errors, overflow |
| Kudu Master RPC Incoming Queue Time | p99/mean queue wait (scan-pressure signal) |
| Kudu Master RPC Queue Overflow | rejected/dropped RPCs |
| Kudu Master Error & Warning Logs | glog error/warning counts |
| Kudu Master Block Cache Hit Ratio | cache effectiveness |
| Kudu Master Threads Running | thread pressure |

The HDFS/Kudu indices are **time series** (one document per node per poll,
`@timestamp` = scrape time), so their panels use `@timestamp` as the time
field and trend cluster health alongside query activity. When an Impala query
shows an `HDFS_SCAN_BOTTLENECK` or `KUDU_SCAN_BOTTLENECK`, line these panels up
against the query's start time to see whether the cluster itself was degraded.

## Accurate counts

The collector indexes **one document per query** (`_id = query_id`), so a
long-running query scraped on several poll cycles is upserted in place rather
than duplicated — a document count equals a query count.

The index patterns use **`start_time`** (the query's real start) as their time
field, not the scrape time, so the "Queries Over Time" histogram reflects when
queries actually ran.

## Regenerating

Edit `build_dashboard.py` and run:

```bash
python3 dashboard/build_dashboard.py
```
