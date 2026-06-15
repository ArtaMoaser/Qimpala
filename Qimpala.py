#!/usr/bin/env python3
"""
Qimpala - Impala Query Monitor & Profile Analyzer
==================================================

Polls Impala coordinators, discovers queries, fetches their full query
profile and ships three kinds of documents to Elasticsearch:

  1. impala-query-summary   -> one doc per query, session / admission metadata
  2. impala-query-metrics   -> one doc per query, flattened performance counters
  3. impala-query-analysis  -> one doc per query, deterministic rule-engine verdict

Flow:

    /queries?json
        -> query_id
            -> /query_profile?query_id=<id>&json
                -> parse summary / timeline / exec summary / counters
                    -> rule engine
                        -> bulk index into Elasticsearch

Dependencies: requests only (Elasticsearch is reached through its REST API,
compatible with Elasticsearch 7.x). No databases, no Kafka, no Redis.
"""

import json
import logging
import re
import time

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import requests

# ==================================================
# CONFIG
# ==================================================

POLL_INTERVAL = 10                     # seconds between polling cycles

IMPALA_SERVERS = [
    "impala01.company.local",
    "impala02.company.local",
    "impala03.company.local",
]

IMPALA_WEBUI_PORT = 25000

# --- HDFS NameNodes (JMX) ---------------------------------------------
# Two NameNodes in an HA pair; both are polled and each doc records whether
# it was the active or standby node at scrape time.
HDFS_NAMENODES = [
    "namenode01.company.local",
    "namenode02.company.local",
]
HDFS_JMX_PORT = 50070                  # http://<nn>:50070/jmx

# --- Kudu masters (JSON metrics) --------------------------------------
KUDU_MASTERS = [
    "kudumaster01.company.local",
    "kudumaster02.company.local",
    "kudumaster03.company.local",
]
KUDU_METRICS_PORT = 8051               # http://<master>:8051/metrics

# Cluster-health endpoints are cheap and change slowly, so poll them less
# often than queries. These are time-series snapshots (one doc per node per
# poll), unlike query docs which upsert by query_id.
CLUSTER_POLL_INTERVAL = 30             # seconds between HDFS/Kudu scrapes

REQUEST_TIMEOUT = 5                    # /queries?json timeout
PROFILE_TIMEOUT = 15                   # /query_profile timeout (profiles can be large)
PROFILE_WORKERS = 4                    # parallel profile fetches per server

ELASTICSEARCH_URL = "http://elasticsearch01.company.local:9200"
ELASTICSEARCH_USER = None              # set both for HTTP basic auth
ELASTICSEARCH_PASSWORD = None
ELASTICSEARCH_TIMEOUT = 10             # seconds per REST call
ELASTICSEARCH_VERIFY_TLS = True        # set False for self-signed https

INDEX_SUMMARY = "impala-query-summary"
INDEX_METRICS = "impala-query-metrics"
INDEX_ANALYSIS = "impala-query-analysis"
INDEX_HDFS = "hdfs-namenode-metrics"   # time-series cluster health
INDEX_KUDU = "kudu-master-metrics"     # time-series cluster health

OUTPUT_LOG = "impala_queries.json"     # fallback / audit trail (newline-delimited JSON)

CACHE_TTL = 86400                      # forget processed queries after 24h

# Only re-fetch the profile when the query reaches a terminal state, plus
# once while running so long queries surface early. Profiles of finished
# queries are complete; profiles of running queries are partial snapshots.
TERMINAL_STATES = {"FINISHED", "EXCEPTION", "CANCELLED", "CANCELED", "RETRIED"}

# ==================================================
# LOGGING
# ==================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

log = logging.getLogger("qimpala")

# ==================================================
# CACHE  (query_id+state -> last processed timestamp)
# ==================================================

seen_queries = {}

# ==================================================
# METRIC CATALOG
# ==================================================
# Every counter we try to pull out of the profile text. Each metric may
# occur many times (once per fragment instance / operator); we aggregate
# across occurrences. Missing metrics become null in the output document.

METRIC_NAMES = [
    # Row batch / wait times
    "RowBatchQueueGetWaitTime", "RowBatchQueuePutWaitTime", "TotalGetBatchTime",
    "FirstBatchWaitTime", "DataWaitTime", "InactiveTotalTime", "TotalStorageWaitTime",

    # Scanner threads
    "AverageScannerThreadConcurrency", "PeakScannerThreadConcurrency",
    "NumScannerThreadsStarted", "NumScannerThreadMemUnavailable",
    "NumScannerThreadReservationsDenied", "ScannerThreadsTotalWallClockTime",
    "ScannerThreadsUserTime", "ScannerThreadsSysTime",
    "ScannerThreadsInvoluntaryContextSwitches", "ScannerThreadsVoluntaryContextSwitches",
    "ScannerThreadWorklessLoops",

    # Memory
    "PeakMemoryUsage", "PeakReservation", "PeakUsedReservation",
    "PerHostPeakMemUsage", "RowBatchQueuePeakMemoryUsage",
    "ColumnarScannerActualReservation", "ColumnarScannerIdealReservation",
    "InitialRangeActualReservation", "InitialRangeIdealReservation",

    # HDFS I/O
    "BytesRead", "TotalReadThroughput", "TotalRawHdfsReadTime",
    "TotalRawHdfsOpenFileTime", "ScannerIoWaitTime",

    # Kudu
    "KuduClientTime", "KuduScannerTotalDurationTime", "KuduScannerQueueDurationTime",
    "KuduScannerCpuUserTime", "KuduScannerCpuSysTime", "TotalKuduScanRoundTrips",
    "KuduRemoteScanTokens", "KuduScannerCfileCacheHitBytes", "KuduScannerCfileCacheMissBytes",

    # Network / RPC
    "TotalNetworkSendTime", "TotalNetworkReceiveTime", "RpcNetworkTime",
    "RpcRecvrTime", "RpcFailure", "RpcRetry", "SerializeBatchTime",
    "DeserializeRowBatchTime", "TotalRPCsDeferred", "TotalHasDeferredRPCsTime",

    # Rows / bytes movement
    "RowsSent", "RowsReceived", "RowsReturned", "TotalBytesSent",
    "TotalBytesReceived", "BytesDequeued", "BytesReceived", "DispatchTime",

    # Joins / partitions
    "ProbeRows", "ProbeTime", "BuildRows", "HashTablesBuildTime",
    "NumHashTableBuildsSkipped", "NumRepartitions", "SpilledPartitions",
    "PartitionsCreated", "LargestPartitionPercent", "MaxPartitionLevel",

    # Lifecycle timers
    "PrepareTime", "OpenTime", "ExecTime", "ExecTreePrepareTime",
    "ExecTreeOpenTime", "ExecTreeExecTime",

    # First/last batch events
    "FirstBatchProduced", "FirstBatchRequested", "FirstBatchReturned", "LastBatchReturned",

    # Parquet
    "ParquetCompressedBytesReadPerColumn", "ParquetUncompressedBytesReadPerColumn",
    "ParquetCompressedPageSize", "FooterProcessingTime", "PageIndexProcessingTime",
    "NumStatsFilteredPages", "NumStatsFilteredRowGroups",

    # Spill
    "ScratchBytesWritten", "ScratchBytesRead", "ScratchReads", "ScratchWrites",
]

# ==================================================
# VALUE PARSERS
# ==================================================

_TIME_UNIT_MS = {
    "d": 86400000.0, "h": 3600000.0, "m": 60000.0,
    "s": 1000.0, "ms": 1.0, "us": 0.001, "ns": 0.000001,
}

_BYTE_UNIT = {
    "B": 1, "KB": 1024, "MB": 1024 ** 2,
    "GB": 1024 ** 3, "TB": 1024 ** 4, "PB": 1024 ** 5,
}

_DURATION_RE = re.compile(r"(\d+(?:\.\d+)?)(d|h|ms|m|s|us|ns)")


def parse_duration_ms(text):
    """Parse an Impala duration ('4s139ms', '8h19m', '363.323ms', '0.000ns')
    into milliseconds. Returns None if the string is not a duration."""
    if text is None:
        return None
    text = str(text).strip()
    if not text:
        return None
    pos, total = 0, 0.0
    matched = False
    for m in _DURATION_RE.finditer(text):
        if m.start() != pos:
            return None  # garbage between components -> not a pure duration
        total += float(m.group(1)) * _TIME_UNIT_MS[m.group(2)]
        pos = m.end()
        matched = True
    if not matched or pos != len(text):
        return None
    return total


# Impala prints timestamps as '2026-06-08 19:08:15.456143000' (space
# separator, up to 9 fractional digits, no timezone). The cluster runs with
# TIMEZONE=Etc/UTC, so we treat these as UTC and emit ISO-8601 with
# millisecond precision, which Elasticsearch 7 maps cleanly to `date`.
_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.(\d+))?")


def parse_impala_timestamp(text):
    """'2026-06-08 19:08:15.456143000' -> '2026-06-08T19:08:15.456Z'.
    Returns None when the string is empty or not a timestamp. Never raises."""
    if not text:
        return None
    m = _TS_RE.match(str(text).strip())
    if not m:
        return None
    date_part, time_part, frac = m.group(1), m.group(2), m.group(3) or ""
    millis = (frac + "000")[:3]  # pad/truncate fractional seconds to ms
    return "%sT%s.%sZ" % (date_part, time_part, millis)


def parse_value(text):
    """Parse a single profile counter value into a number.

    Handles, in order:
      '7.79 MB (8170570)'  -> 8170570            (raw bytes in parens)
      '12 (12)'            -> 12                  (counter, raw in parens)
      '1s440ms'            -> 1440.0              (duration -> milliseconds)
      '123.45 /sec'        -> 123.45
      '0.00'               -> 0.0
    Returns None when nothing numeric can be extracted. Never raises.
    """
    if text is None:
        return None
    try:
        text = str(text).strip()
        if not text:
            return None

        # Raw value in trailing parentheses, e.g. '3.00 MB (3145728)' or '12 (12)'
        m = re.search(r"\(([\d,]+)\)\s*$", text)
        if m:
            return int(m.group(1).replace(",", ""))

        # Duration
        dur = parse_duration_ms(text)
        if dur is not None:
            return dur

        # Byte value without raw parens, e.g. '36.14 MB'
        m = re.match(r"^(-?\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB|PB)\b", text)
        if m:
            return float(m.group(1)) * _BYTE_UNIT[m.group(2)]

        # Plain number, possibly with a rate suffix ('123.4 /sec', '85.00%')
        m = re.match(r"^(-?\d+(?:\.\d+)?)", text)
        if m:
            v = float(m.group(1))
            return int(v) if v.is_integer() else v
    except Exception:
        pass
    return None


# ==================================================
# PROFILE PARSER
# ==================================================

class ProfileParser:
    """Extracts structured data from the text profile returned by
    /query_profile?query_id=<id>&json.

    The endpoint returns {'profile': '<huge indented text>', 'query_id': ...}.
    All extractors tolerate missing sections and return None / {} / []
    instead of raising.
    """

    # 'Key: value' lines we lift verbatim from the Summary block
    SUMMARY_FIELDS = {
        "Session ID": "session_id",
        "Session Type": "session_type",
        "Start Time": "start_time",
        "End Time": "end_time",
        "Query Type": "query_type",
        "Query State": "query_state",
        "Impala Query State": "impala_query_state",
        "Query Status": "query_status",
        "User": "user",
        "Connected User": "connected_user",
        "Delegated User": "delegated_user",
        "Network Address": "network_address",
        "Default Db": "default_db",
        "Coordinator": "coordinator",
        "Estimated Per-Host Mem": "estimated_per_host_mem",
        "Cluster Memory Admitted": "cluster_memory_admitted",
        "Admission result": "admission_result",
        "Request Pool": "request_pool",
        "Duration": "duration",
    }

    def __init__(self, profile_text):
        self.text = profile_text or ""

    # ---------- generic helpers ----------

    def extract_field(self, label):
        """Return the value of a '    Label: value' line, or None.
        Uses [ \\t] (not \\s) around the value so an empty field never
        bleeds into the following line."""
        m = re.search(r"^[ \t]*%s:[ \t]*(.*?)[ \t]*$" % re.escape(label), self.text, re.M)
        return m.group(1) if (m and m.group(1)) else None

    def extract_metric_by_name(self, name):
        """Return every occurrence of a counter line '- Name: value' as
        parsed numbers. Returns [] when the metric never appears."""
        values = []
        pattern = re.compile(r"^\s*-\s*%s(?:\(\*\))?:\s*(.+?)\s*$" % re.escape(name), re.M)
        for m in pattern.finditer(self.text):
            v = parse_value(m.group(1))
            if v is not None:
                values.append(v)
        return values

    def extract_sampled_metric(self, name):
        """Parse sampled stats like:
        '- Name: (Avg: 7.42 MB (7778039) ; Min: 520.00 B (520) ; Max: 60.52 MB (63459349) ; Number of samples: 120)'
        Returns dict {avg, min, max, samples} or None."""
        m = re.search(
            r"^\s*-\s*%s(?:\(\*\))?:\s*\(Avg:\s*(.+?)\s*;\s*Min:\s*(.+?)\s*;\s*Max:\s*(.+?)\s*;\s*Number of samples:\s*(\d+)\)" % re.escape(name),
            self.text, re.M)
        if not m:
            return None
        return {
            "avg": parse_value(m.group(1)),
            "min": parse_value(m.group(2)),
            "max": parse_value(m.group(3)),
            "samples": int(m.group(4)),
        }

    # ---------- section extractors ----------

    def extract_summary(self):
        """Lift the Summary block fields into a flat dict."""
        out = {}
        for label, key in self.SUMMARY_FIELDS.items():
            out[key] = self.extract_field(label)
        # SQL statement: capture up to end of line (Impala keeps it on one line)
        out["sql_statement"] = self.extract_field("Sql Statement")
        return out

    def _extract_event_list(self, header):
        """Parse an event list section such as Query Timeline / Query Compilation:

            Query Timeline: 8h19m
               - Query submitted: 7.396ms (7.396ms)
               - Planning finished: 388.361ms (380.964ms)

        Returns {'total_ms': float|None, 'events': {name: cumulative_ms}}.
        """
        result = {"total_ms": None, "events": {}}
        m = re.search(r"^([ \t]*)%s:[ \t]*(.*?)[ \t]*$" % re.escape(header), self.text, re.M)
        if not m:
            return result
        result["total_ms"] = parse_duration_ms(m.group(2))
        indent = len(m.group(1))
        # walk following lines while they are deeper-indented event bullets
        # (split()[1:] skips the tail of the header line itself)
        for line in self.text[m.end():].split("\n")[1:]:
            if not line.strip():
                break
            stripped_indent = len(line) - len(line.lstrip())
            if stripped_indent <= indent:
                break
            em = re.match(r"^\s*-\s*(.+?):\s*([\dA-Za-z.]+)\s*(?:\(([\dA-Za-z.]+)\))?\s*$", line)
            if em:
                cumulative = parse_duration_ms(em.group(2))
                if cumulative is not None:
                    result["events"][em.group(1)] = cumulative
            else:
                # tolerate sub-counters / unexpected lines inside the section
                continue
        return result

    def extract_timeline(self):
        """Query Timeline events -> {event_name: cumulative_ms}."""
        return self._extract_event_list("Query Timeline")

    def extract_compilation(self):
        """Query Compilation phases -> {phase_name: cumulative_ms}."""
        return self._extract_event_list("Query Compilation")

    def extract_exec_summary(self):
        """Parse the ExecSummary operator table into a list of dicts:
        [{operator, hosts, instances, avg_time_ms, max_time_ms, rows,
          est_rows, peak_mem_bytes, est_peak_mem_bytes, detail}, ...]
        """
        rows = []
        m = re.search(r"^[ \t]*ExecSummary:[ \t]*$", self.text, re.M)
        if not m:
            return rows
        lines = self.text[m.end():].split("\n")[1:]
        if not lines or not lines[0].startswith("Operator"):
            return rows
        header = lines[0]

        # The table is fixed-width with right-aligned numeric columns: each
        # value column ends where its header token ends. Compute slice
        # boundaries from the header so blank cells parse as None.
        columns = ["#Hosts", "#Inst", "Avg Time", "Max Time", "#Rows",
                   "Est. #Rows", "Peak Mem", "Est. Peak Mem"]
        bounds, prev_end = [], header.find("#Hosts")
        operator_end = prev_end
        for col in columns:
            pos = header.find(col)
            if pos < 0:
                return rows  # unknown layout: bail out rather than misparse
            end = pos + len(col)
            bounds.append((prev_end, end))
            prev_end = end
        detail_start = prev_end

        def cell(line, lo, hi):
            return line[lo:hi].strip() or None

        for line in lines[1:]:
            if not line.strip() or line.startswith("---"):
                continue
            op = re.sub(r"^[|\- ]+", "", line[:operator_end]).strip()
            # operator rows look like '14:TOP-N' or 'F11:EXCHANGE SENDER'
            if not re.match(r"^F?\d+:", op):
                break  # past the end of the table
            rows.append({
                "operator": op,
                "hosts": parse_value(cell(line, *bounds[0])),
                "instances": parse_value(cell(line, *bounds[1])),
                "avg_time_ms": parse_duration_ms(cell(line, *bounds[2])),
                "max_time_ms": parse_duration_ms(cell(line, *bounds[3])),
                "rows": _parse_short_count(cell(line, *bounds[4])),
                "est_rows": _parse_short_count(cell(line, *bounds[5])),
                "peak_mem_bytes": parse_value(cell(line, *bounds[6])),
                "est_peak_mem_bytes": parse_value(cell(line, *bounds[7])),
                "detail": (line[detail_start:].strip() or None),
            })
        return rows

    def extract_per_host_peak_memory(self):
        """Per Host Peak Memory Usage line -> {host: bytes}."""
        line = self.extract_field("Per Host Peak Memory Usage")
        out = {}
        if not line:
            return out
        for host, val in re.findall(r"([\w.\-]+:\d+)\(([^)]+)\)", line):
            out[host] = parse_value(val)
        return out


def _parse_short_count(text):
    """'3.00M' -> 3000000, '469.41K' -> 469410, '12' -> 12, None -> None."""
    if not text:
        return None
    m = re.match(r"^([\d.]+)([KMBG]?)$", text.strip())
    if not m:
        return None
    mult = {"": 1, "K": 1e3, "M": 1e6, "B": 1e9, "G": 1e9}[m.group(2)]
    return int(float(m.group(1)) * mult)


# ==================================================
# METRIC AGGREGATION
# ==================================================

# Sampled per-column metrics use the (Avg ; Min ; Max ; samples) format
SAMPLED_METRICS = {
    "ParquetCompressedBytesReadPerColumn",
    "ParquetUncompressedBytesReadPerColumn",
}


def build_metrics_doc(parser):
    """Flatten every cataloged metric into a single dict.

    Each metric occurs once per fragment instance, so we aggregate:
        <Name>       -> max across instances (worst case, drives diagnosis)
        <Name>_sum   -> sum across instances (cluster-wide totals)
        <Name>_avg   -> average across instances (skew baseline)
        <Name>_count -> number of occurrences
    Metrics absent from the profile become null (plain name only).
    """
    doc = {}
    for name in METRIC_NAMES:
        if name in SAMPLED_METRICS:
            stats = parser.extract_sampled_metric(name)
            if stats:
                doc[name] = stats["max"]
                doc[name + "_avg"] = stats["avg"]
                doc[name + "_min"] = stats["min"]
                doc[name + "_count"] = stats["samples"]
            else:
                doc[name] = None
            continue

        values = parser.extract_metric_by_name(name)
        if values:
            doc[name] = max(values)
            doc[name + "_sum"] = sum(values)
            doc[name + "_avg"] = sum(values) / len(values)
            doc[name + "_count"] = len(values)
        else:
            doc[name] = None
    return doc


# ==================================================
# RULE ENGINE
# ==================================================

class QueryAnalyzer:
    """Deterministic rule engine that turns metrics + timeline + exec summary
    into a root-cause verdict. Each rule appends a finding; the highest
    scoring finding becomes suspected_root_cause."""

    # severity thresholds expressed as a fraction of total query time
    DOMINANT = 0.40       # a wait that eats >=40% of the query is the story
    SIGNIFICANT = 0.20

    SKEW_RATIO = 3.0      # max/avg operator or instance time ratio
    MIN_SKEW_MS = 1000    # ignore skew on operators faster than this

    BYTES_READ_HUGE = 50 * 1024 ** 3      # 50 GB scanned -> flag regardless
    ADMISSION_DELAY_MS = 5000
    MEM_PRESSURE_RATIO = 0.85             # peak vs estimate
    LARGEST_PARTITION_PCT = 40
    REPARTITION_LIMIT = 2
    MIN_BUILD_ROWS = 100000   # ignore partition skew on tiny build sides

    def __init__(self, metrics, summary, timeline, exec_summary, duration_ms):
        self.m = metrics
        self.summary = summary
        self.timeline = timeline.get("events", {})
        self.exec_summary = exec_summary
        # total wall time the rules normalize against; never zero
        self.duration_ms = max(duration_ms or 0, 1.0)
        self.findings = []

    # ---------- helpers ----------

    def metric(self, name, default=0):
        v = self.m.get(name)
        return v if v is not None else default

    def add(self, cause, severity, finding, recommendation, score):
        self.findings.append({
            "cause": cause,
            "severity": severity,
            "finding": finding,
            "recommendation": recommendation,
            "score": score,
        })

    def severity_for(self, ratio):
        # waits are summed per instance and can exceed wall time; cap at 1
        ratio = min(ratio, 1.0)
        if ratio >= 0.6:
            return "critical"
        if ratio >= self.DOMINANT:
            return "high"
        if ratio >= self.SIGNIFICANT:
            return "medium"
        return "low"

    # ---------- rules ----------

    def check_hdfs_scan(self):
        read_ms = self.metric("TotalRawHdfsReadTime")
        io_wait = self.metric("ScannerIoWaitTime")
        bytes_read = self.metric("BytesRead_sum")
        worst = max(read_ms, io_wait)
        ratio = worst / self.duration_ms
        if ratio >= self.SIGNIFICANT or bytes_read >= self.BYTES_READ_HUGE:
            self.add(
                "HDFS_SCAN_BOTTLENECK",
                self.severity_for(max(ratio, 0.5 if bytes_read >= self.BYTES_READ_HUGE else 0)),
                "HDFS scan dominates: raw read time %.0fms, IO wait %.0fms, %.1f GB read "
                "(query wall time %.0fms)." % (read_ms, io_wait, bytes_read / 1024 ** 3, self.duration_ms),
                "Reduce scanned data: add partition pruning predicates, use min/max & "
                "dictionary filters (sort data on filter columns), increase replica "
                "locality, or raise NUM_SCANNER_THREADS / IO capacity.",
                ratio + (0.3 if bytes_read >= self.BYTES_READ_HUGE else 0),
            )

    def check_admission(self):
        submit = self.timeline.get("Submit for admission")
        admitted = self.timeline.get("Completed admission")
        queued = "queued" in (self.summary.get("admission_result") or "").lower()
        if submit is not None and admitted is not None:
            delay = admitted - submit
            if delay >= self.ADMISSION_DELAY_MS or queued:
                ratio = delay / self.duration_ms
                self.add(
                    "ADMISSION_CONTROL_DELAY",
                    self.severity_for(max(ratio, 0.25)),
                    "Query waited %.0fms in admission control (result: %s)."
                    % (delay, self.summary.get("admission_result")),
                    "Pool '%s' is saturated: review pool memory/queue limits, lower "
                    "MEM_LIMIT for over-estimating queries, or add executor capacity."
                    % self.summary.get("request_pool"),
                    ratio + 0.1,
                )

    def check_data_skew(self):
        for op in self.exec_summary:
            avg, mx = op.get("avg_time_ms"), op.get("max_time_ms")
            if not avg or not mx or mx < self.MIN_SKEW_MS:
                continue
            if mx / max(avg, 0.001) >= self.SKEW_RATIO and op.get("instances", 0) > 1:
                ratio = mx / self.duration_ms
                self.add(
                    "DATA_SKEW",
                    self.severity_for(ratio),
                    "Operator %s shows skew: max time %.0fms vs avg %.0fms across %d instances (%s)."
                    % (op["operator"], mx, avg, op["instances"], op.get("detail") or "-"),
                    "Check the distribution of the join/partition key feeding this "
                    "operator; consider salting hot keys, recomputing stats, or "
                    "repartitioning the source table.",
                    ratio,
                )

    def check_join_skew(self):
        largest = self.metric("LargestPartitionPercent")
        repart = self.metric("NumRepartitions_sum")
        build_rows = self.metric("BuildRows")
        # a 100% partition on a tiny build side is noise, not skew
        if build_rows < self.MIN_BUILD_ROWS and repart <= self.REPARTITION_LIMIT:
            return
        if largest >= self.LARGEST_PARTITION_PCT or repart > self.REPARTITION_LIMIT:
            self.add(
                "JOIN_SKEW",
                "high" if largest >= 70 or repart > 4 else "medium",
                "Hash join imbalance: largest partition holds %d%% of rows, "
                "%d repartition passes." % (largest, repart),
                "The build-side key distribution is heavily skewed. Verify column "
                "stats are fresh (COMPUTE STATS), filter NULL-heavy keys before the "
                "join, or rewrite the hot-key portion as a separate broadcast join.",
                largest / 100.0 + repart * 0.1,
            )

    def check_memory(self):
        peak = self.metric("PeakMemoryUsage")
        per_host_peak = self.metric("PerHostPeakMemUsage")
        estimate = parse_value(self.summary.get("estimated_per_host_mem"))
        spilled = self.metric("SpilledPartitions_sum")
        scratch = self.metric("ScratchBytesWritten_sum")
        if spilled > 0 or scratch > 0:
            self.add(
                "MEMORY_PRESSURE", "high",
                "Query spilled to disk: %d partitions spilled, %.1f MB scratch written."
                % (spilled, scratch / 1024 ** 2),
                "Raise the query MEM_LIMIT / pool memory, reduce build-side size "
                "(filter earlier, drop unused columns), or enable more executors.",
                0.6,
            )
        elif estimate and per_host_peak and per_host_peak / estimate >= self.MEM_PRESSURE_RATIO:
            self.add(
                "MEMORY_PRESSURE", "medium",
                "Per-host peak memory %.1f MB is %.0f%% of the %.1f MB estimate."
                % (per_host_peak / 1024 ** 2, 100.0 * per_host_peak / estimate, estimate / 1024 ** 2),
                "Query runs close to its memory budget and risks spilling under "
                "concurrency; consider raising MEM_LIMIT or trimming the plan.",
                0.3,
            )

    def check_scanner_starvation(self):
        mem_unavail = self.metric("NumScannerThreadMemUnavailable_sum")
        denied = self.metric("NumScannerThreadReservationsDenied_sum")
        if mem_unavail > 0 or denied > 0:
            self.add(
                "SCANNER_THREAD_STARVATION", "high",
                "Scanner threads starved: %d mem-unavailable events, %d reservation denials."
                % (mem_unavail, denied),
                "Scan nodes could not get memory for additional scanner threads. "
                "Increase MEM_LIMIT, reduce concurrent queries on the pool, or lower "
                "NUM_SCANNER_THREADS so each thread gets adequate reservation.",
                0.5 + min((mem_unavail + denied) / 100.0, 0.4),
            )

    def check_network(self):
        send = self.metric("TotalNetworkSendTime")
        recv = max(self.metric("TotalNetworkReceiveTime"), self.metric("RpcNetworkTime"))
        worst = max(send, recv)
        ratio = worst / self.duration_ms
        if ratio >= self.SIGNIFICANT:
            self.add(
                "NETWORK_BOTTLENECK",
                self.severity_for(ratio),
                "Exchange/network wait dominates: send %.0fms, receive %.0fms, "
                "%.1f MB sent total." % (send, recv, self.metric("TotalBytesSent_sum") / 1024 ** 2),
                "Large shuffles relative to network capacity. Reduce exchanged data "
                "(project fewer columns, aggregate before the exchange), prefer "
                "broadcast joins for small build sides, or check NIC saturation on "
                "the slow hosts.",
                ratio,
            )
        failures = self.metric("RpcFailure_sum")
        retries = self.metric("RpcRetry_sum")
        if failures > 0 or retries > 0:
            self.add(
                "NETWORK_BOTTLENECK", "medium",
                "RPC instability: %d failures, %d retries." % (failures, retries),
                "Investigate network flakiness or overloaded KRPC service threads "
                "between executors.",
                0.3,
            )

    def check_kudu(self):
        client = self.metric("KuduClientTime")
        queue = self.metric("KuduScannerQueueDurationTime")
        total = self.metric("KuduScannerTotalDurationTime")
        worst = max(client, queue, total)
        ratio = worst / self.duration_ms
        if ratio >= self.SIGNIFICANT:
            self.add(
                "KUDU_SCAN_BOTTLENECK",
                self.severity_for(ratio),
                "Kudu scan is slow: client time %.0fms, scanner queue %.0fms, "
                "total scanner %.0fms over %d round trips."
                % (client, queue, total, self.metric("TotalKuduScanRoundTrips_sum")),
                "Kudu tablet servers are the bottleneck: check tserver CPU / "
                "maintenance-manager backlog, rebalance hot tablets, raise "
                "kudu scanner batch size, or add tservers. High queue duration "
                "specifically means tservers cannot keep up with scan requests.",
                ratio,
            )

    def check_fragment_imbalance(self):
        """Backend imbalance detected via per-instance ExecTime spread."""
        avg, mx = self.m.get("ExecTime_avg"), self.m.get("ExecTime")
        cnt = self.metric("ExecTime_count")
        if avg and mx and cnt > 1 and mx >= self.MIN_SKEW_MS and mx / max(avg, 0.001) >= self.SKEW_RATIO:
            self.add(
                "DATA_SKEW",
                self.severity_for(mx / self.duration_ms),
                "Fragment imbalance: slowest instance ExecTime %.0fms vs %.0fms "
                "average across %d instances." % (mx, avg, cnt),
                "One backend does most of the work: check scan-range assignment "
                "locality, a slow/overloaded host, or key skew in the data.",
                mx / self.duration_ms * 0.9,  # slightly below operator-level skew
            )

    # ---------- driver ----------

    def analyze(self):
        for rule in (
            self.check_hdfs_scan, self.check_admission, self.check_data_skew,
            self.check_join_skew, self.check_memory, self.check_scanner_starvation,
            self.check_network, self.check_kudu, self.check_fragment_imbalance,
        ):
            try:
                rule()
            except Exception:
                log.exception("rule %s failed", rule.__name__)

        if not self.findings:
            return {
                "suspected_root_cause": "NONE",
                "severity": "ok",
                "findings": [],
                "recommendations": [],
            }

        self.findings.sort(key=lambda f: f["score"], reverse=True)
        top = self.findings[0]
        return {
            "suspected_root_cause": top["cause"],
            "severity": top["severity"],
            "findings": [f["finding"] for f in self.findings],
            "recommendations": [f["recommendation"] for f in self.findings],
            "all_causes": [
                {"cause": f["cause"], "severity": f["severity"], "score": round(f["score"], 3)}
                for f in self.findings
            ],
        }


# ==================================================
# ELASTICSEARCH SINK
# ==================================================

# keep mappings lean: dynamic numeric metrics map fine automatically,
# we only pin the fields we query/aggregate on.
# All date fields accept both ISO-8601 strings and epoch millis so the
# mapping never rejects a document.
_DATE = {"type": "date", "format": "strict_date_optional_time||epoch_millis"}

INDEX_DEFINITIONS = {
    INDEX_SUMMARY: {
        "mappings": {
            "properties": {
                "@timestamp": _DATE,        # when the collector wrote the doc
                "collected_at": _DATE,       # last time this query was scraped
                "start_time": _DATE,         # query start (real event time)
                "end_time": _DATE,           # query end
                "query_id": {"type": "keyword"},
                "host": {"type": "keyword"},
                "user": {"type": "keyword"},
                "connected_user": {"type": "keyword"},
                "delegated_user": {"type": "keyword"},
                "request_pool": {"type": "keyword"},
                "query_state": {"type": "keyword"},
                "impala_query_state": {"type": "keyword"},
                "query_status": {"type": "keyword"},
                "query_type": {"type": "keyword"},
                "session_type": {"type": "keyword"},
                "coordinator": {"type": "keyword"},
                "default_db": {"type": "keyword"},
                "network_address": {"type": "keyword"},
                "duration_ms": {"type": "double"},
                "query_compilation_total_ms": {"type": "double"},
                "query_text": {"type": "text"},
                "query_timeline": {"type": "object", "enabled": True},
                "query_compilation": {"type": "object", "enabled": True},
            }
        }
    },
    INDEX_METRICS: {
        "mappings": {
            # all counters are numeric; let ES infer doubles/longs dynamically
            "dynamic_templates": [
                {"metrics_as_double": {
                    "match_mapping_type": "long",
                    "mapping": {"type": "double"},
                }}
            ],
            "properties": {
                "@timestamp": _DATE,
                "collected_at": _DATE,
                "start_time": _DATE,
                "query_id": {"type": "keyword"},
                "host": {"type": "keyword"},
            },
        }
    },
    INDEX_ANALYSIS: {
        "mappings": {
            "properties": {
                "@timestamp": _DATE,
                "collected_at": _DATE,
                "start_time": _DATE,
                "query_id": {"type": "keyword"},
                "host": {"type": "keyword"},
                "suspected_root_cause": {"type": "keyword"},
                "severity": {"type": "keyword"},
                "findings": {"type": "text"},
                "recommendations": {"type": "text"},
                "duration_ms": {"type": "double"},
                "user": {"type": "keyword"},
                "request_pool": {"type": "keyword"},
                "query_state": {"type": "keyword"},
            }
        }
    },
    # --- HDFS NameNode health (time series) ---------------------------
    INDEX_HDFS: {
        "mappings": {
            "dynamic_templates": [
                {"jmx_longs_as_double": {
                    "path_match": "jmx.*",
                    "match_mapping_type": "long",
                    "mapping": {"type": "double"}}},
            ],
            "properties": {
                "@timestamp": _DATE,
                "host": {"type": "keyword"},
                "ha_state": {"type": "keyword"},      # active / standby
                "fs_state": {"type": "keyword"},
                "safemode": {"type": "keyword"},
                # capacity
                "capacity_total_bytes": {"type": "double"},
                "capacity_used_bytes": {"type": "double"},
                "capacity_remaining_bytes": {"type": "double"},
                "capacity_used_pct": {"type": "double"},
                "total_load": {"type": "double"},
                # blocks / files
                "blocks_total": {"type": "double"},
                "files_total": {"type": "double"},
                "missing_blocks": {"type": "double"},
                "corrupt_blocks": {"type": "double"},
                "under_replicated_blocks": {"type": "double"},
                "pending_replication_blocks": {"type": "double"},
                "pending_deletion_blocks": {"type": "double"},
                "excess_blocks": {"type": "double"},
                # datanodes
                "num_live_datanodes": {"type": "integer"},
                "num_dead_datanodes": {"type": "integer"},
                "num_stale_datanodes": {"type": "integer"},
                "num_decom_live_datanodes": {"type": "integer"},
                "num_decom_dead_datanodes": {"type": "integer"},
                "volume_failures_total": {"type": "integer"},
                # rpc
                "rpc_queue_time_avg_ms": {"type": "double"},
                "rpc_processing_time_avg_ms": {"type": "double"},
                "rpc_queue_time_num_ops": {"type": "double"},
                "rpc_processing_time_num_ops": {"type": "double"},
                "call_queue_length": {"type": "double"},
                "num_open_connections": {"type": "double"},
                # jvm
                "jvm_heap_used_mb": {"type": "double"},
                "jvm_heap_max_mb": {"type": "double"},
                "jvm_heap_used_pct": {"type": "double"},
                "jvm_gc_count": {"type": "double"},
                "jvm_gc_time_ms": {"type": "double"},
                "jvm_gc_num_warn_threshold_exceeded": {"type": "double"},
                "threads_blocked": {"type": "double"},
                "threads_waiting": {"type": "double"},
            },
        }
    },
    # --- Kudu master health (time series) ----------------------------
    INDEX_KUDU: {
        "mappings": {
            "dynamic_templates": [
                {"metrics_longs_as_double": {
                    "path_match": "metrics.*",
                    "match_mapping_type": "long",
                    "mapping": {"type": "double"}}},
            ],
            "properties": {
                "@timestamp": _DATE,
                "host": {"type": "keyword"},
                "entity_id": {"type": "keyword"},     # e.g. kudu.master
                "is_leader": {"type": "boolean"},
                # promoted, well-known server metrics
                "rpc_queue_overflow": {"type": "double"},
                "rpc_incoming_queue_time_mean_us": {"type": "double"},
                "rpc_incoming_queue_time_p99_us": {"type": "double"},
                "rpc_incoming_queue_time_max_us": {"type": "double"},
                "rpc_incoming_queue_time_count": {"type": "double"},
                "rpc_connections_accepted": {"type": "double"},
                "threads_running": {"type": "double"},
                "threads_started": {"type": "double"},
                "cpu_utime_ms": {"type": "double"},
                "cpu_stime_ms": {"type": "double"},
                "voluntary_context_switches": {"type": "double"},
                "involuntary_context_switches": {"type": "double"},
                "glog_error_messages": {"type": "double"},
                "glog_warning_messages": {"type": "double"},
                "block_cache_hits_caching": {"type": "double"},
                "block_cache_misses_caching": {"type": "double"},
                "block_cache_usage_bytes": {"type": "double"},
                "block_cache_hit_ratio": {"type": "double"},
                "data_dirs_failed": {"type": "double"},
                "data_dirs_full": {"type": "double"},
            },
        }
    },
}


class ElasticsearchSink:
    """Talks to Elasticsearch 7.x through its plain REST API using requests -
    no python client dependency. Ensures the three indices exist on startup
    and indexes documents one-by-one (PUT /<index>/_doc/<id>) or in batches
    (POST /_bulk). Falls back to the local audit log when ES is unreachable
    so no data is silently dropped."""

    def __init__(self):
        self.base_url = ELASTICSEARCH_URL.rstrip("/")
        self.session = requests.Session()
        self.session.headers["Content-Type"] = "application/json"
        if ELASTICSEARCH_USER:
            self.session.auth = (ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD)
        self.session.verify = ELASTICSEARCH_VERIFY_TLS
        self.available = False
        try:
            r = self.session.get(self.base_url, timeout=ELASTICSEARCH_TIMEOUT)
            r.raise_for_status()
            version = r.json().get("version", {}).get("number", "?")
            log.info("connected to Elasticsearch %s at %s", version, self.base_url)
            self.available = True
            self.create_indices()
        except Exception as e:
            log.error("Elasticsearch unavailable at %s: %s", self.base_url, e)

    def _request(self, method, path, body=None, ndjson=None):
        """One REST call; returns the parsed JSON response or None on error."""
        url = "%s/%s" % (self.base_url, path.lstrip("/"))
        kwargs = {"timeout": ELASTICSEARCH_TIMEOUT}
        if ndjson is not None:
            kwargs["data"] = ndjson
            kwargs["headers"] = {"Content-Type": "application/x-ndjson"}
        elif body is not None:
            kwargs["data"] = json.dumps(body, default=str)
        r = self.session.request(method, url, **kwargs)
        if r.status_code >= 300:
            raise RuntimeError("ES %s %s -> %d: %s"
                               % (method, path, r.status_code, r.text[:500]))
        return r.json() if r.text else None

    def create_indices(self):
        for index, body in INDEX_DEFINITIONS.items():
            try:
                # HEAD /<index> -> 200 if it exists, 404 otherwise
                head = self.session.head("%s/%s" % (self.base_url, index),
                                         timeout=ELASTICSEARCH_TIMEOUT)
                if head.status_code == 200:
                    continue
                self._request("PUT", index, body=body)
                log.info("created index %s", index)
            except Exception as e:
                log.error("index creation failed for %s: %s", index, e)

    def index(self, index, doc, doc_id=None):
        """Index one document.

        Query docs pass no doc_id and carry query_id -> PUT /_doc/<query_id>,
        which upserts so a query is never duplicated. Cluster time-series docs
        pass an explicit doc_id (e.g. '<host>_<epoch_ms>') so each snapshot is
        a distinct, append-only document. If neither is given, ES auto-assigns
        an id via POST /_doc."""
        write_log({"_index": index, **doc})  # audit trail always
        if not self.available:
            return
        ref = doc_id or doc.get("query_id")
        try:
            if ref is not None:
                self._request("PUT", "%s/_doc/%s" % (
                    index, requests.utils.quote(str(ref), safe="")), body=doc)
            else:
                self._request("POST", "%s/_doc" % index, body=doc)
        except Exception as e:
            log.error("ES index %s failed for %s: %s", index, ref, e)

    def bulk(self, actions):
        """POST /_bulk with NDJSON. actions = [(index, doc), ...]."""
        if not self.available:
            for _, doc in actions:
                write_log(doc)
            return
        lines = []
        for index, doc in actions:
            lines.append(json.dumps(
                {"index": {"_index": index, "_id": doc.get("query_id")}}))
            lines.append(json.dumps(doc, default=str))
        try:
            resp = self._request("POST", "_bulk", ndjson="\n".join(lines) + "\n")
            if resp and resp.get("errors"):
                failed = [i["index"] for i in resp.get("items", [])
                          if i.get("index", {}).get("status", 200) >= 300]
                log.error("ES bulk: %d item(s) failed, first: %s",
                          len(failed), failed[0] if failed else "?")
        except Exception as e:
            log.error("ES bulk failed: %s", e)


# ==================================================
# LOCAL AUDIT LOG
# ==================================================

def write_log(doc):
    try:
        with open(OUTPUT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(doc, ensure_ascii=False, default=str) + "\n")
    except Exception as e:
        log.error("audit log write failed: %s", e)


# ==================================================
# HTTP FETCHERS
# ==================================================

def fetch_queries(server):
    url = "http://%s:%d/queries?json" % (server, IMPALA_WEBUI_PORT)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_profile(server, query_id):
    """Fetch the full text profile; returns the profile string or None."""
    url = "http://%s:%d/query_profile" % (server, IMPALA_WEBUI_PORT)
    try:
        response = requests.get(
            url, params={"query_id": query_id, "json": ""}, timeout=PROFILE_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        return payload.get("profile") or payload.get("contents")
    except Exception as e:
        log.warning("%s -> profile fetch failed for %s: %s", server, query_id, e)
        return None


def extract_queries(payload):
    """The /queries page exposes several lists depending on the version;
    completed queries carry the richest profiles."""
    queries = []
    for key in ("in_flight_queries", "completed_queries", "queries", "active_queries"):
        queries.extend(payload.get(key) or [])
    return queries


def fetch_jmx(host):
    """Fetch the HDFS NameNode JMX page (JSON: {'beans': [...]}).
    Returns the list of beans or None on error."""
    url = "http://%s:%d/jmx" % (host, HDFS_JMX_PORT)
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json().get("beans", [])
    except Exception as e:
        log.warning("HDFS %s -> jmx fetch failed: %s", host, e)
        return None


def fetch_kudu_metrics(host):
    """Fetch the Kudu master metrics page (JSON: list of entities).
    Returns the list of entities or None on error."""
    url = "http://%s:%d/metrics" % (host, KUDU_METRICS_PORT)
    try:
        # compact=1 trims whitespace; schema is omitted by default
        response = requests.get(url, params={"compact": "1"}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        log.warning("Kudu %s -> metrics fetch failed: %s", host, e)
        return None


# ==================================================
# DOCUMENT BUILDERS
# ==================================================

def compute_duration_ms(summary, timeline, profile_parser):
    """Best-effort wall time: explicit Duration, else last timeline event,
    else the Execution Profile total."""
    d = parse_duration_ms(summary.get("duration"))
    if d:
        return d
    events = timeline.get("events") or {}
    if events:
        return max(events.values())
    m = re.search(r"Execution Profile [\da-f:]+\:\(Total: ([\dA-Za-z.]+)",
                  profile_parser.text)
    if m:
        return parse_duration_ms(m.group(1))
    return None


def build_documents(server, query_id, query_text, profile_text):
    """Parse one profile and return the three documents (summary, metrics,
    analysis). Never raises - parse failures yield partially-filled docs.

    Date handling: the query's real Start Time becomes the primary event
    time (`@timestamp`) so dashboards bucket queries by when they actually
    ran, not by when the collector happened to scrape them. `collected_at`
    records the scrape time separately. All three docs share `_id=query_id`
    (set by the sink), so a long-running query scraped on several cycles
    collapses to exactly one document - counting docs == counting queries."""
    now = datetime.now(timezone.utc).isoformat()
    parser = ProfileParser(profile_text)

    summary = parser.extract_summary()
    timeline = parser.extract_timeline()
    compilation = parser.extract_compilation()
    exec_summary = parser.extract_exec_summary()
    duration_ms = compute_duration_ms(summary, timeline, parser)

    start_time = parse_impala_timestamp(summary.get("start_time"))
    end_time = parse_impala_timestamp(summary.get("end_time"))

    base = {
        # event time = real query start when known, else scrape time
        "@timestamp": start_time or now,
        "collected_at": now,
        "start_time": start_time,
        "query_id": query_id,
        "host": server,
    }

    summary_fields = {k: v for k, v in summary.items()
                      if k not in ("sql_statement", "start_time", "end_time")}
    summary_doc = {
        **base,
        **summary_fields,
        "end_time": end_time,
        "query_text": query_text or summary.get("sql_statement"),
        "duration_ms": duration_ms,
        "query_compilation": compilation["events"],
        "query_compilation_total_ms": compilation["total_ms"],
        "query_timeline": timeline["events"],
        "per_host_peak_memory": parser.extract_per_host_peak_memory(),
    }

    metrics_doc = {**base, **build_metrics_doc(parser), "duration_ms": duration_ms}

    analysis = QueryAnalyzer(
        metrics_doc, summary, timeline, exec_summary, duration_ms).analyze()
    analysis_doc = {
        **base,
        **analysis,
        "duration_ms": duration_ms,
        "user": summary.get("user"),
        "request_pool": summary.get("request_pool"),
        "query_state": summary.get("impala_query_state") or summary.get("query_state"),
    }

    return summary_doc, metrics_doc, analysis_doc


# ==================================================
# HDFS NAMENODE (JMX) BUILDER
# ==================================================

def _index_beans(beans):
    """Index JMX beans by their short name. A bean name looks like
    'Hadoop:service=NameNode,name=FSNamesystem'; we key on the 'name=' part."""
    out = {}
    for bean in beans or []:
        full = bean.get("name", "")
        m = re.search(r"name=([^,]+)", full)
        if m:
            out[m.group(1)] = bean
    return out


def _num(bean, key):
    """Read a numeric field from a JMX bean, tolerating missing keys/types."""
    if not bean:
        return None
    v = bean.get(key)
    if isinstance(v, bool):
        return None
    return v if isinstance(v, (int, float)) else None


def build_hdfs_doc(host, beans):
    """Flatten the diagnostically useful HDFS NameNode JMX beans into one
    time-series document. Missing fields become null; never raises.

    Beans used (standard Hadoop NameNode JMX):
      FSNamesystem / FSNamesystemState  -> capacity, blocks, datanode health
      NameNodeStatus                    -> active / standby (HA role)
      NameNodeInfo                      -> safemode, percent used
      RpcActivityForPort*               -> RPC latency / call queue
      JvmMetrics                        -> heap, GC, threads
    """
    now = datetime.now(timezone.utc).isoformat()
    by = _index_beans(beans)
    fsn = by.get("FSNamesystem")
    fss = by.get("FSNamesystemState")
    nninfo = by.get("NameNodeInfo")
    nnstatus = by.get("NameNodeStatus")
    jvm = by.get("JvmMetrics")
    # RPC bean name carries the port (RpcActivityForPort8020); pick the first.
    rpc = next((b for n, b in by.items() if n.startswith("RpcActivityForPort")), None)

    cap_total = _num(fsn, "CapacityTotal")
    cap_used = _num(fsn, "CapacityUsed")
    cap_remaining = _num(fsn, "CapacityRemaining")
    heap_used = _num(jvm, "MemHeapUsedM")
    heap_max = _num(jvm, "MemHeapMaxM")

    doc = {
        "@timestamp": now,
        "host": host,
        "ha_state": (_str(nnstatus, "State") or "").lower() or None,
        "fs_state": _str(fss, "FSState"),
        "safemode": _str(nninfo, "Safemode") or "OFF",
        # capacity
        "capacity_total_bytes": cap_total,
        "capacity_used_bytes": cap_used,
        "capacity_remaining_bytes": cap_remaining,
        "capacity_used_pct": (100.0 * cap_used / cap_total)
                              if (cap_total and cap_used is not None) else None,
        "total_load": _num(fsn, "TotalLoad"),
        # blocks / files
        "blocks_total": _num(fsn, "BlocksTotal"),
        "files_total": _num(fsn, "FilesTotal"),
        "missing_blocks": _num(fsn, "MissingBlocks"),
        "corrupt_blocks": _num(fsn, "CorruptBlocks"),
        "under_replicated_blocks": _num(fsn, "UnderReplicatedBlocks"),
        "pending_replication_blocks": _num(fsn, "PendingReplicationBlocks"),
        "pending_deletion_blocks": _num(fsn, "PendingDeletionBlocks"),
        "excess_blocks": _num(fsn, "ExcessBlocks"),
        # datanodes (FSNamesystemState carries the live/dead counts)
        "num_live_datanodes": _num(fss, "NumLiveDataNodes"),
        "num_dead_datanodes": _num(fss, "NumDeadDataNodes"),
        "num_stale_datanodes": _num(fsn, "NumStaleDataNodes"),
        "num_decom_live_datanodes": _num(fss, "NumDecomLiveDataNodes"),
        "num_decom_dead_datanodes": _num(fss, "NumDecomDeadDataNodes"),
        "volume_failures_total": _num(fss, "VolumeFailuresTotal"),
        # rpc
        "rpc_queue_time_avg_ms": _num(rpc, "RpcQueueTimeAvgTime"),
        "rpc_processing_time_avg_ms": _num(rpc, "RpcProcessingTimeAvgTime"),
        "rpc_queue_time_num_ops": _num(rpc, "RpcQueueTimeNumOps"),
        "rpc_processing_time_num_ops": _num(rpc, "RpcProcessingTimeNumOps"),
        "call_queue_length": _num(rpc, "CallQueueLength"),
        "num_open_connections": _num(rpc, "NumOpenConnections"),
        # jvm
        "jvm_heap_used_mb": heap_used,
        "jvm_heap_max_mb": heap_max,
        "jvm_heap_used_pct": (100.0 * heap_used / heap_max)
                             if (heap_max and heap_used is not None) else None,
        "jvm_gc_count": _num(jvm, "GcCount"),
        "jvm_gc_time_ms": _num(jvm, "GcTimeMillis"),
        "jvm_gc_num_warn_threshold_exceeded": _num(jvm, "GcNumWarnThresholdExceeded"),
        "threads_blocked": _num(jvm, "ThreadsBlocked"),
        "threads_waiting": _num(jvm, "ThreadsWaiting"),
    }
    # keep the full numeric FSNamesystem + RPC + JVM fields under jmx.* so
    # nothing useful is lost and ad-hoc queries stay possible
    doc["jmx"] = {}
    for bean in (fsn, fss, rpc, jvm):
        for k, v in (bean or {}).items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                doc["jmx"][k] = v
    return doc


def _str(bean, key):
    if not bean:
        return None
    v = bean.get(key)
    return v if isinstance(v, str) and v != "" else None


# ==================================================
# KUDU MASTER (METRICS) BUILDER
# ==================================================

# Server metrics promoted to friendly top-level fields when present. Times
# from Kudu are microseconds for histograms and milliseconds for the cpu
# counters; we keep the native unit and name fields accordingly.
KUDU_PROMOTE = {
    "rpcs_queue_overflow": "rpc_queue_overflow",
    "rpc_connections_accepted": "rpc_connections_accepted",
    "threads_running": "threads_running",
    "threads_started": "threads_started",
    "cpu_utime": "cpu_utime_ms",
    "cpu_stime": "cpu_stime_ms",
    "voluntary_context_switches": "voluntary_context_switches",
    "involuntary_context_switches": "involuntary_context_switches",
    "glog_error_messages": "glog_error_messages",
    "glog_warning_messages": "glog_warning_messages",
    "block_cache_hits_caching": "block_cache_hits_caching",
    "block_cache_misses_caching": "block_cache_misses_caching",
    "block_cache_usage": "block_cache_usage_bytes",
    "data_dirs_failed": "data_dirs_failed",
    "data_dirs_full": "data_dirs_full",
}


def build_kudu_doc(host, entities):
    """Flatten the Kudu master 'server' entity metrics into one time-series
    document. Counters/gauges contribute their value; histograms contribute
    mean / p99 / max / count. A curated set is promoted to friendly fields;
    everything numeric is also kept under metrics.* . Never raises."""
    now = datetime.now(timezone.utc).isoformat()
    server = None
    for ent in entities or []:
        if ent.get("type") == "server":
            server = ent
            break

    doc = {"@timestamp": now, "host": host,
           "entity_id": (server or {}).get("id"), "metrics": {}}

    for metric in (server or {}).get("metrics", []) or []:
        name = metric.get("name")
        if not name:
            continue
        if "value" in metric:                       # counter or gauge
            val = metric.get("value")
            if isinstance(val, (int, float)) and not isinstance(val, bool):
                doc["metrics"][name] = val
                if name in KUDU_PROMOTE:
                    doc[KUDU_PROMOTE[name]] = val
        elif "total_count" in metric:               # histogram
            doc["metrics"][name + "_mean"] = metric.get("mean")
            doc["metrics"][name + "_p99"] = metric.get("percentile_99")
            doc["metrics"][name + "_max"] = metric.get("max")
            doc["metrics"][name + "_count"] = metric.get("total_count")
            if name == "rpc_incoming_queue_time":
                doc["rpc_incoming_queue_time_mean_us"] = metric.get("mean")
                doc["rpc_incoming_queue_time_p99_us"] = metric.get("percentile_99")
                doc["rpc_incoming_queue_time_max_us"] = metric.get("max")
                doc["rpc_incoming_queue_time_count"] = metric.get("total_count")

    # derived: block cache hit ratio
    hits = doc.get("block_cache_hits_caching")
    misses = doc.get("block_cache_misses_caching")
    if hits is not None and misses is not None and (hits + misses) > 0:
        doc["block_cache_hit_ratio"] = round(hits / (hits + misses), 4)

    # leadership: the master that has a positive raft leader-election term for
    # the catalog / sys.catalog tablet is the active leader. Best-effort: many
    # builds expose 'is_raft_leader' or a leader role gauge; fall back to None.
    leader = doc["metrics"].get("is_raft_leader")
    if leader is None:
        leader = doc["metrics"].get("leader_count")  # 1 on the leader master
    doc["is_leader"] = bool(leader) if leader is not None else None

    return doc


# ==================================================
# COLLECTOR
# ==================================================

class ImpalaCollector:
    def __init__(self, sink):
        self.sink = sink

    def process_query(self, server, query_id, query_text):
        profile = fetch_profile(server, query_id)
        if not profile:
            return
        try:
            summary_doc, metrics_doc, analysis_doc = build_documents(
                server, query_id, query_text, profile)
        except Exception:
            log.exception("profile parse failed for %s", query_id)
            return
        self.sink.index(INDEX_SUMMARY, summary_doc)
        self.sink.index(INDEX_METRICS, metrics_doc)
        self.sink.index(INDEX_ANALYSIS, analysis_doc)
        log.info("%s -> indexed %s (%s, %s)", server, query_id,
                 analysis_doc["suspected_root_cause"], analysis_doc["severity"])

    def process_server(self, server):
        try:
            payload = fetch_queries(server)
        except Exception as e:
            log.error("%s -> %s", server, e)
            write_log({
                "@timestamp": datetime.now(timezone.utc).isoformat(),
                "event_type": "error",
                "impala_server": server,
                "error": str(e),
            })
            return

        queries = extract_queries(payload)
        log.info("%s -> %d queries visible", server, len(queries))

        todo = []
        for q in queries:
            query_id = q.get("query_id") or q.get("queryId") or q.get("id")
            if not query_id:
                continue
            state = (q.get("state") or q.get("query_state") or "UNKNOWN").upper()
            # process each query once while running and once when terminal,
            # so finished queries get their complete profile.
            phase = "done" if state in TERMINAL_STATES else "running"
            cache_key = "%s_%s" % (query_id, phase)
            if cache_key in seen_queries:
                continue
            seen_queries[cache_key] = time.time()
            sql = q.get("stmt") or q.get("statement") or q.get("query") or ""
            todo.append((query_id, sql))

        if not todo:
            return
        with ThreadPoolExecutor(max_workers=PROFILE_WORKERS) as pool:
            for query_id, sql in todo:
                pool.submit(self.process_query, server, query_id, sql)

    # ---- cluster health (HDFS / Kudu) -------------------------------

    def collect_hdfs(self, host):
        beans = fetch_jmx(host)
        if beans is None:
            return
        try:
            doc = build_hdfs_doc(host, beans)
        except Exception:
            log.exception("HDFS jmx parse failed for %s", host)
            return
        # time-series id: one doc per node per poll
        doc_id = "%s_%d" % (host, int(time.time() * 1000))
        self.sink.index(INDEX_HDFS, doc, doc_id=doc_id)
        log.info("HDFS %s -> indexed (%s, live_dn=%s, missing_blocks=%s)",
                 host, doc.get("ha_state"), doc.get("num_live_datanodes"),
                 doc.get("missing_blocks"))

    def collect_kudu(self, host):
        entities = fetch_kudu_metrics(host)
        if entities is None:
            return
        try:
            doc = build_kudu_doc(host, entities)
        except Exception:
            log.exception("Kudu metrics parse failed for %s", host)
            return
        doc_id = "%s_%d" % (host, int(time.time() * 1000))
        self.sink.index(INDEX_KUDU, doc, doc_id=doc_id)
        log.info("Kudu %s -> indexed (leader=%s, rpc_overflow=%s, errors=%s)",
                 host, doc.get("is_leader"), doc.get("rpc_queue_overflow"),
                 doc.get("glog_error_messages"))

    def collect_cluster(self):
        """Scrape all HDFS NameNodes and Kudu masters once."""
        nodes = ([("hdfs", h) for h in HDFS_NAMENODES]
                 + [("kudu", h) for h in KUDU_MASTERS])
        if not nodes:
            return
        with ThreadPoolExecutor(max_workers=max(len(nodes), 1)) as pool:
            for kind, host in nodes:
                pool.submit(self.collect_hdfs if kind == "hdfs"
                            else self.collect_kudu, host)

    def run_once(self, with_cluster=False):
        with ThreadPoolExecutor(max_workers=len(IMPALA_SERVERS)) as executor:
            executor.map(self.process_server, IMPALA_SERVERS)
        if with_cluster:
            self.collect_cluster()
        cleanup_cache()


def cleanup_cache():
    now = time.time()
    for k in [k for k, v in seen_queries.items() if now - v > CACHE_TTL]:
        del seen_queries[k]


# ==================================================
# MAIN
# ==================================================

def main():
    log.info("Starting Qimpala monitor (impala=%d, hdfs=%d, kudu=%d, "
             "query interval=%ds, cluster interval=%ds)",
             len(IMPALA_SERVERS), len(HDFS_NAMENODES), len(KUDU_MASTERS),
             POLL_INTERVAL, CLUSTER_POLL_INTERVAL)
    sink = ElasticsearchSink()
    collector = ImpalaCollector(sink)
    last_cluster = 0.0
    while True:
        now = time.time()
        # cluster health is polled on its own (slower) cadence
        with_cluster = (now - last_cluster) >= CLUSTER_POLL_INTERVAL
        if with_cluster:
            last_cluster = now
        collector.run_once(with_cluster=with_cluster)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()


# ==================================================
# EXAMPLE DOCUMENTS
# ==================================================
#
# impala-query-summary
# --------------------
# {
#   "@timestamp": "2026-06-08T19:08:15.456Z",   <- real query start (event time)
#   "collected_at": "2026-06-08T19:08:20.123456+00:00",  <- when scraped
#   "query_id": "304a6c7ec2b1380e:6bfd4df200000000",
#   "host": "impala01.company.local",
#   "session_id": "0f430fd720fcbf88:ed78c5a2777ad78b",
#   "session_type": "HIVESERVER2",
#   "start_time": "2026-06-08T19:08:15.456Z",    <- ISO-8601, mapped as date
#   "end_time": null,
#   "query_type": "QUERY",
#   "query_state": "FINISHED",
#   "impala_query_state": "FINISHED",
#   "query_status": "OK",
#   "user": "dotboard_reporting",
#   "connected_user": "dotboard_reporting",
#   "delegated_user": null,
#   "network_address": "10.10.2.2:34442",
#   "default_db": "default",
#   "coordinator": "dbhddfpsbpp0103:27000",
#   "estimated_per_host_mem": "1463363778",
#   "cluster_memory_admitted": "10.90 GB",
#   "admission_result": "Admitted immediately",
#   "request_pool": "default-pool",
#   "duration_ms": 4623.0,
#   "query_text": "with tmp_user as (select ... ) select ... limit 50",
#   "query_compilation_total_ms": 277.019,
#   "query_compilation": {
#     "Metadata of all 4 tables cached": 69.87,
#     "Analysis finished": 173.625,
#     "Single node plan created": 239.705,
#     "Planning finished": 277.019
#   },
#   "query_timeline": {
#     "Query submitted": 7.396,
#     "Planning finished": 388.361,
#     "Submit for admission": 410.033,
#     "Completed admission": 438.778,
#     "Ready to start on 8 backends": 443.062,
#     "All 8 execution backends (73 fragment instances) started": 753.201,
#     "First dynamic filter received": 4393.0,
#     "Rows available": 4623.0
#   },
#   "per_host_peak_memory": {"dbhddfpsbpp0103:27000": 524288000}
# }
#
# impala-query-metrics  (truncated - one key set per cataloged metric)
# --------------------
# {
#   "@timestamp": "2026-06-08T19:08:20.123456+00:00",
#   "query_id": "304a6c7ec2b1380e:6bfd4df200000000",
#   "host": "impala01.company.local",
#   "duration_ms": 4623.0,
#   "BytesRead": 8170570,            <- max across fragment instances
#   "BytesRead_sum": 37893436,       <- cluster-wide total
#   "BytesRead_avg": 4736679.5,
#   "BytesRead_count": 8,
#   "TotalRawHdfsReadTime": 1440.0,  <- durations normalized to milliseconds
#   "TotalRawHdfsReadTime_sum": 3363.3,
#   "KuduClientTime": 214.849,
#   "PeakMemoryUsage": 496877568,
#   "LargestPartitionPercent": 49,
#   "NumScannerThreadMemUnavailable": 0,
#   "FirstBatchProduced": null,      <- metric absent from this profile
#   ...
# }
#
# impala-query-analysis
# ---------------------
# {
#   "@timestamp": "2026-06-08T19:08:20.123456+00:00",
#   "query_id": "304a6c7ec2b1380e:6bfd4df200000000",
#   "host": "impala01.company.local",
#   "duration_ms": 4623.0,
#   "user": "dotboard_reporting",
#   "request_pool": "default-pool",
#   "query_state": "FINISHED",
#   "suspected_root_cause": "KUDU_SCAN_BOTTLENECK",
#   "severity": "high",
#   "findings": [
#     "Kudu scan is slow: client time 1036ms, scanner queue 817ms, total scanner 1809ms over 64 round trips.",
#     "Operator 03:SCAN KUDU shows skew: max time 1809ms vs avg 1036ms across 8 instances (pod.thingprofile)."
#   ],
#   "recommendations": [
#     "Kudu tablet servers are the bottleneck: check tserver CPU / maintenance-manager backlog, ...",
#     "Check the distribution of the join/partition key feeding this operator; ..."
#   ],
#   "all_causes": [
#     {"cause": "KUDU_SCAN_BOTTLENECK", "severity": "high", "score": 0.391},
#     {"cause": "DATA_SKEW", "severity": "medium", "score": 0.391}
#   ]
# }
#
# hdfs-namenode-metrics  (time series: one doc per NameNode per poll)
# ----------------------
# {
#   "@timestamp": "2026-06-15T07:22:30.000Z",
#   "host": "namenode01.company.local",
#   "ha_state": "active",            <- active / standby (HA role)
#   "fs_state": "Operational",
#   "safemode": "OFF",
#   "capacity_total_bytes": 109951162777600,
#   "capacity_used_bytes": 76965813944320,
#   "capacity_remaining_bytes": 32985348833280,
#   "capacity_used_pct": 70.0,
#   "blocks_total": 5123456,
#   "files_total": 7891011,
#   "missing_blocks": 2,
#   "corrupt_blocks": 1,
#   "under_replicated_blocks": 134,
#   "num_live_datanodes": 8,
#   "num_dead_datanodes": 1,
#   "volume_failures_total": 3,
#   "rpc_queue_time_avg_ms": 1.8,
#   "rpc_processing_time_avg_ms": 3.2,
#   "call_queue_length": 7,
#   "num_open_connections": 210,
#   "jvm_heap_used_mb": 24576.0,
#   "jvm_heap_max_mb": 32768.0,
#   "jvm_heap_used_pct": 75.0,
#   "jvm_gc_count": 150234,
#   "jvm_gc_time_ms": 845231,
#   "jmx": { "...": "full numeric FSNamesystem/RPC/JVM fields kept verbatim" }
# }
#
# kudu-master-metrics  (time series: one doc per master per poll)
# -------------------
# {
#   "@timestamp": "2026-06-15T07:22:30.000Z",
#   "host": "kudumaster01.company.local",
#   "entity_id": "kudu.master",
#   "is_leader": true,
#   "rpc_queue_overflow": 0,
#   "rpc_connections_accepted": 15234,
#   "rpc_incoming_queue_time_mean_us": 42.7,
#   "rpc_incoming_queue_time_p99_us": 350,
#   "rpc_incoming_queue_time_max_us": 98000,
#   "rpc_incoming_queue_time_count": 50000,
#   "threads_running": 62,
#   "cpu_utime_ms": 982340,
#   "cpu_stime_ms": 341200,
#   "voluntary_context_switches": 88231,
#   "involuntary_context_switches": 12003,
#   "glog_error_messages": 0,
#   "glog_warning_messages": 37,
#   "block_cache_hits_caching": 900000,
#   "block_cache_misses_caching": 100000,
#   "block_cache_hit_ratio": 0.9,
#   "metrics": { "...": "every numeric server metric; histograms as _mean/_p99/_max/_count" }
# }
