# Qimpala Kibana Dashboard

`qimpala-kibana-dashboard.ndjson` is a Kibana 7.x saved-objects export that
builds an **Impala Query Monitoring** dashboard on top of the three indices
the collector writes.

## Import

1. Open Kibana → **Stack Management → Saved Objects → Import**.
2. Select `qimpala-kibana-dashboard.ndjson`.
3. Choose *"Automatically overwrite conflicts"* if you are re-importing.
4. Open **Dashboard → Impala Query Monitoring**.

It creates three index patterns (`impala-query-summary`, `impala-query-metrics`,
`impala-query-analysis`), 11 visualizations, and the dashboard linking them.

## What's on it

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
