import React, { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "../api";
import Pagination from "../components/Pagination";
import {
  Loader2,
  Activity,
  CheckCircle2,
  AlertTriangle,
  Info,
  Bug,
  Filter,
} from "lucide-react";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
} from "recharts";

const SOURCES = ["Flink", "HBase", "HDFS", "MC"];

const SEVERITY_FILTERS = [
  { key: "", label: "全部" },
  { key: "critical", label: "严重" },
  { key: "warning", label: "警告" },
  { key: "info", label: "信息" },
];

const METRIC_KEYS = ["throughput", "latency_p99", "cpu_usage", "memory_usage"];
const METRIC_LABELS = {
  throughput: "吞吐量",
  latency_p99: "P99 延迟",
  cpu_usage: "CPU",
  memory_usage: "内存",
};
const METRIC_COLORS = {
  throughput: "#2563eb",
  latency_p99: "#d97706",
  cpu_usage: "#e11d48",
  memory_usage: "#7c3aed",
};

function pivotMetricsForChart(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const byTime = new Map();
  for (const r of rows) {
    const t = r.recorded_at || "";
    if (!byTime.has(t)) {
      byTime.set(t, { time: t, label: formatTimeLabel(t) });
    }
    const pt = byTime.get(t);
    const name = r.metric_name;
    if (METRIC_KEYS.includes(name)) {
      pt[name] = r.metric_value;
    }
  }
  return Array.from(byTime.values()).sort(
    (a, b) => new Date(a.time) - new Date(b.time),
  );
}

function formatTimeLabel(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return `${String(d.getMonth() + 1).padStart(2, "0")}/${String(d.getDate()).padStart(2, "0")} ${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
}

function SeverityBadge({ severity }) {
  const s = (severity || "").toLowerCase();
  if (s === "critical") {
    return (
      <span className="inline-flex items-center gap-1 rounded-md bg-red-100 px-2 py-0.5 text-xs font-medium text-red-800 ring-1 ring-inset ring-red-200">
        <Bug className="h-3 w-3" />
        严重
      </span>
    );
  }
  if (s === "warning") {
    return (
      <span className="inline-flex items-center gap-1 rounded-md bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-900 ring-1 ring-inset ring-amber-200">
        <AlertTriangle className="h-3 w-3" />
        警告
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 rounded-md bg-blue-100 px-2 py-0.5 text-xs font-medium text-blue-800 ring-1 ring-inset ring-blue-200">
      <Info className="h-3 w-3" />
      信息
    </span>
  );
}

function truncate(str, n) {
  if (!str) return "";
  return str.length <= n ? str : `${str.slice(0, n)}…`;
}

export default function Monitor() {
  const [severity, setSeverity] = useState("");
  const [events, setEvents] = useState([]);
  const [eventsLoading, setEventsLoading] = useState(true);
  const [eventsError, setEventsError] = useState(null);
  const [evPage, setEvPage] = useState(1);
  const [evPageInfo, setEvPageInfo] = useState({ total: 0, total_pages: 1 });

  const [tab, setTab] = useState("Flink");
  const [metricsRaw, setMetricsRaw] = useState([]);
  const [metricsLoading, setMetricsLoading] = useState(true);
  const [metricsError, setMetricsError] = useState(null);

  const [anomalyLoading, setAnomalyLoading] = useState(false);
  const [anomalyResult, setAnomalyResult] = useState(null);
  const [anomalyError, setAnomalyError] = useState(null);

  const loadEvents = useCallback(async () => {
    setEventsLoading(true);
    setEventsError(null);
    try {
      const data = await api.monitor.events({ severity: severity || undefined, page: evPage, page_size: 10 });
      if (data && data.items) {
        setEvents(data.items);
        setEvPageInfo({ total: data.total, total_pages: data.total_pages });
      } else {
        setEvents(Array.isArray(data) ? data : []);
      }
    } catch (e) {
      setEventsError(e.message || "加载事件失败");
      setEvents([]);
    } finally {
      setEventsLoading(false);
    }
  }, [severity, evPage]);

  useEffect(() => {
    loadEvents();
  }, [loadEvents]);

  const loadMetrics = useCallback(async (source) => {
    setMetricsLoading(true);
    setMetricsError(null);
    setAnomalyResult(null);
    setAnomalyError(null);
    try {
      const data = await api.monitor.metrics(source);
      setMetricsRaw(Array.isArray(data) ? data : []);
    } catch (e) {
      setMetricsError(e.message || "加载指标失败");
      setMetricsRaw([]);
    } finally {
      setMetricsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadMetrics(tab);
  }, [tab, loadMetrics]);

  const chartData = useMemo(() => pivotMetricsForChart(metricsRaw), [metricsRaw]);

  const metricValuesForDetect = useMemo(
    () => metricsRaw.map((m) => m.metric_value).filter((v) => typeof v === "number"),
    [metricsRaw],
  );

  const handleResolve = async (id) => {
    try {
      await api.monitor.resolve(id);
      await loadEvents();
    } catch (e) {
      setEventsError(e.message || "标记失败");
    }
  };

  const runAnomaly = async () => {
    setAnomalyLoading(true);
    setAnomalyError(null);
    setAnomalyResult(null);
    try {
      const res = await api.monitor.detectAnomaly(metricValuesForDetect);
      setAnomalyResult(res);
    } catch (e) {
      setAnomalyError(e.message || "检测失败");
    } finally {
      setAnomalyLoading(false);
    }
  };

  return (
    <div className="space-y-8 text-slate-900 dark:text-slate-100">
      <div className="flex flex-wrap items-center gap-2">
          <span className="mr-1 inline-flex items-center gap-1 text-sm font-medium text-slate-600 dark:text-slate-300">
          <Filter className="h-4 w-4 text-slate-400" />
          严重程度
        </span>
        {SEVERITY_FILTERS.map(({ key, label }) => (
          <button
            key={key || "all"}
            type="button"
            onClick={() => {
              setSeverity(key);
              setEvPage(1);
            }}
            className={[
              "rounded-lg px-3 py-1.5 text-sm font-medium transition-colors",
              severity === key
                ? "bg-indigo-600 text-white shadow-sm"
                : "border border-slate-200 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700",
            ].join(" ")}
          >
            {label}
          </button>
        ))}
      </div>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-700 dark:bg-slate-900">
        <div className="mb-4 flex items-center gap-2">
          <Activity className="h-5 w-5 text-indigo-600" />
          <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">监控事件</h2>
        </div>
        {eventsLoading && (
          <div className="flex min-h-[120px] items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
          </div>
        )}
        {eventsError && !eventsLoading && (
          <p className="text-sm text-red-600">{eventsError}</p>
        )}
        {!eventsLoading && !eventsError && events.length === 0 && (
          <p className="text-sm text-slate-500">暂无事件</p>
        )}
        <ul className="space-y-3">
          {!eventsLoading &&
            events.map((ev) => (
              <li
                key={ev.id}
                className="flex flex-col gap-2 rounded-xl border border-slate-100 bg-slate-50/60 p-4 sm:flex-row sm:items-start sm:justify-between dark:border-slate-700 dark:bg-slate-800/40"
              >
                <div className="min-w-0 flex-1 space-y-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <SeverityBadge severity={ev.severity} />
                    <span className="rounded-md bg-white px-2 py-0.5 text-xs font-medium text-slate-600 ring-1 ring-slate-200 dark:bg-slate-700 dark:text-slate-200 dark:ring-slate-600">
                      {ev.source_system || "—"}
                    </span>
                    <span className="text-xs text-slate-400">{ev.created_at}</span>
                  </div>
                  <h3 className="font-semibold text-slate-900 dark:text-slate-100">{ev.title}</h3>
                  <p className="text-sm text-slate-600 dark:text-slate-300">{truncate(ev.detail, 200)}</p>
                </div>
                {!ev.resolved && (
                  <button
                    type="button"
                    onClick={() => handleResolve(ev.id)}
                    className="inline-flex shrink-0 items-center gap-1.5 rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-1.5 text-sm font-medium text-emerald-800 hover:bg-emerald-100"
                  >
                    <CheckCircle2 className="h-4 w-4" />
                    标记已解决
                  </button>
                )}
              </li>
            ))}
        </ul>
        {!eventsLoading && !eventsError && (
          <div className="mt-4 border-t border-slate-100 pt-4">
            <Pagination page={evPage} totalPages={evPageInfo.total_pages} total={evPageInfo.total} pageSize={10} onPageChange={setEvPage} />
          </div>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm dark:border-slate-700 dark:bg-slate-900">
        <h2 className="mb-4 text-lg font-semibold text-slate-900 dark:text-slate-100">系统指标</h2>
        <div className="mb-4 flex flex-wrap gap-2 border-b border-slate-100 pb-4 dark:border-slate-800">
          {SOURCES.map((s) => (
            <button
              key={s}
              type="button"
              onClick={() => setTab(s)}
              className={[
                "rounded-lg px-4 py-2 text-sm font-medium transition-colors",
                tab === s
                  ? "bg-slate-900 text-white"
                  : "text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-800",
              ].join(" ")}
            >
              {s}
            </button>
          ))}
        </div>

        {metricsLoading && (
          <div className="flex h-72 items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-indigo-600" />
          </div>
        )}
        {metricsError && !metricsLoading && (
          <p className="text-sm text-red-600">{metricsError}</p>
        )}
        {!metricsLoading && !metricsError && chartData.length === 0 && (
          <p className="py-12 text-center text-sm text-slate-500">暂无指标数据</p>
        )}
        {!metricsLoading && !metricsError && chartData.length > 0 && (
          <div className="h-80 w-full min-w-0">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" className="stroke-slate-200" />
                <XAxis dataKey="label" tick={{ fontSize: 11 }} className="text-slate-500" />
                <YAxis tick={{ fontSize: 11 }} className="text-slate-500" />
                <Tooltip
                  contentStyle={{
                    borderRadius: "8px",
                    border: "1px solid #e2e8f0",
                    fontSize: "12px",
                  }}
                />
                <Legend wrapperStyle={{ fontSize: "12px" }} />
                {METRIC_KEYS.map((key) => (
                  <Line
                    key={key}
                    type="monotone"
                    dataKey={key}
                    name={METRIC_LABELS[key]}
                    stroke={METRIC_COLORS[key]}
                    strokeWidth={2}
                    dot={false}
                    connectNulls
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </div>
        )}

        <div className="mt-6 flex flex-col gap-3 border-t border-slate-100 pt-4 sm:flex-row sm:items-center sm:justify-between dark:border-slate-800">
          <button
            type="button"
            onClick={runAnomaly}
            disabled={anomalyLoading || metricValuesForDetect.length === 0}
            className="inline-flex items-center justify-center gap-2 rounded-lg bg-amber-500 px-4 py-2 text-sm font-medium text-white hover:bg-amber-600 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {anomalyLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <AlertTriangle className="h-4 w-4" />
            )}
            异常检测
          </button>
          {metricValuesForDetect.length === 0 && !metricsLoading && (
            <p className="text-xs text-slate-500">当前无可用数值序列，无法检测</p>
          )}
        </div>
        {anomalyError && (
          <p className="mt-2 text-sm text-red-600">{anomalyError}</p>
        )}
        {anomalyResult && (
          <div className="mt-3 rounded-lg border border-amber-100 bg-amber-50/80 px-4 py-3 text-sm text-amber-950">
            <p className="font-medium">检测结果</p>
            <dl className="mt-2 grid gap-1 text-xs sm:grid-cols-2">
              <div>
                <dt className="text-amber-800/80">状态</dt>
                <dd className="font-mono">{anomalyResult.status}</dd>
              </div>
              {anomalyResult.mean != null && (
                <div>
                  <dt className="text-amber-800/80">均值</dt>
                  <dd className="font-mono">{anomalyResult.mean}</dd>
                </div>
              )}
              {anomalyResult.stdev != null && (
                <div>
                  <dt className="text-amber-800/80">标准差</dt>
                  <dd className="font-mono">{anomalyResult.stdev}</dd>
                </div>
              )}
              {Array.isArray(anomalyResult.anomaly_indexes) && (
                <div className="sm:col-span-2">
                  <dt className="text-amber-800/80">异常点索引</dt>
                  <dd className="font-mono">
                    {anomalyResult.anomaly_indexes.length
                      ? anomalyResult.anomaly_indexes.join(", ")
                      : "无"}
                  </dd>
                </div>
              )}
            </dl>
          </div>
        )}
      </section>
    </div>
  );
}
