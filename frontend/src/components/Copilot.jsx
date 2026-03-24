import { useState, useCallback, useRef, useEffect } from "react";
import {
  Bot,
  X,
  Zap,
  Send,
  Sparkles,
  ChevronUp,
  MessageSquare,
  Gauge,
  Shield,
  Terminal,
} from "lucide-react";

const API = "/api";

const SAMPLE_HBASE_BODY = {
  table_name: "demo_order_collect",
  namespace: "ctg363566671677_hh_fed_sub19_cjzh_cbss_hbase_lb19",
  pre_regions: 16,
};

const SAMPLE_VALIDATE_PARAMS = {
  hbasezk: "10.177.138.67,10.177.138.68,10.177.138.69",
  setParalizem: "100",
  sourceTopic: "tprds-dc-i-prods-new",
  password: "******",
};

const SAMPLE_ANOMALY_METRICS = [12.1, 11.8, 12.0, 11.9, 12.2, 48.7, 12.0, 11.7];

const FAQ_DEFAULT =
  "我可以帮助你了解采集流程的任何问题，试试输入关键词如：预分区、分隔符、BulkLoad、Flink配置、OOM";

const FAQ_BY_KEYWORD = [
  {
    test: (t) => t.includes("预分区"),
    text:
      "预分区数量由数据量与压缩方式决定：先将文件大小换算为 GB（size_gb），" +
      "region_size 在压缩为 gz 时取 3，否则取 4；副本数 replicas 固定为 4。" +
      "公式：pre_regions = max(10, ⌈size_gb × 1024 / region_size / replicas⌉)。这样可避免单 Region 过大导致热点与分裂风暴。",
  },
  {
    test: (t) => t.includes("分隔符"),
    text:
      "字段分隔符常选 0x01（SOH）或 0x0A（换行）。0x01 适合单行多字段、且字段内容不含该字节；" +
      "若字段内可能含控制字符，需评估转义策略。0x0A 多用于一行一条记录；与 BulkLoad 的 HFile 行格式、解析脚本要保持一致，避免与业务数据冲突。",
  },
  {
    test: (t) => t.includes("bulkload"),
    text:
      "BulkLoad 流程概要：1）将初始化或导出的数据落到 HDFS 约定目录并完成权限；2）" +
      "用脚本/工具生成 HFile 或直接准备已分好区的 HFile；3）执行 complete bulkload，将 HFile 载入目标表 Namespace:Table；" +
      "4）校验行数与抽样对比。全程需关注 Region 分布与磁盘、YARN 队列资源。",
  },
  {
    test: (t) => t.includes("flink") && (t.includes("配置") || t.includes("参数")),
    text:
      "Flink 侧关键参数包括：并行度（与 Kafka 分区、HBase 吞吐匹配）、Checkpoint 路径与间隔、" +
      "Kafka group.id / reset 策略、主键与字段列表（hbaseInfo / tableIndexName）、HBase ZK 与 namespace。" +
      "变更并行度或 Topic 后建议同步校验消费滞后与 Checkpoint 恢复策略。",
  },
  {
    test: (t) => t.includes("oom"),
    text:
      "BulkLoad / MapReduce 阶段 OOM 排查：减小单任务堆内存或降低并行度；检查单文件/Region 是否过大；" +
      "确认 Container 内存与 vcores 配额；避免一次性加载过多 HFile 列表到内存；查看 GC 日志与 container 退出码，必要时调大 mapreduce.map/reduce.memory.mb 或拆分输入路径分批加载。",
  },
];

function matchExploreReply(raw) {
  const t = raw.trim().toLowerCase();
  if (!t) return FAQ_DEFAULT;
  for (const { test, text } of FAQ_BY_KEYWORD) {
    if (test(t)) return text;
  }
  return FAQ_DEFAULT;
}

async function apiPost(path, body) {
  const res = await fetch(`${API}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body ?? {}),
  });
  let data;
  const ct = res.headers.get("content-type") || "";
  if (ct.includes("application/json")) {
    data = await res.json();
  } else {
    const text = await res.text();
    data = text;
  }
  if (!res.ok) {
    const detail = typeof data === "object" && data !== null ? data.detail : data;
    const msg = Array.isArray(detail)
      ? detail.map((d) => d.msg || d).join("; ")
      : detail || res.statusText;
    throw new Error(String(msg));
  }
  return data;
}

function ResultBlock({ data, error, loading }) {
  if (loading) {
    return (
      <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-500 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300">
        请求中…
      </div>
    );
  }
  if (error) {
    return (
      <div className="rounded-xl border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800">
        {error}
      </div>
    );
  }
  if (data == null) return null;

  if (typeof data !== "object") {
    return (
      <pre className="max-h-40 overflow-auto rounded-xl border border-slate-200 bg-slate-50 p-3 text-xs text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100">
        {String(data)}
      </pre>
    );
  }

  const flat =
    Object.values(data).every(
      (v) => v === null || typeof v !== "object" || Array.isArray(v),
    ) && !Array.isArray(data);

  if (flat) {
    return (
      <dl className="grid max-h-48 gap-2 overflow-auto rounded-xl border border-slate-200 bg-slate-50 p-3 text-sm dark:border-slate-700 dark:bg-slate-800">
        {Object.entries(data).map(([k, v]) => (
          <div key={k} className="grid grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)] gap-2 border-b border-slate-100 pb-2 last:border-0 last:pb-0 dark:border-slate-700">
            <dt className="font-medium text-slate-600 dark:text-slate-300">{k}</dt>
            <dd className="break-all font-mono text-xs text-slate-900 dark:text-slate-100">
              {typeof v === "object" ? JSON.stringify(v) : String(v)}
            </dd>
          </div>
        ))}
      </dl>
    );
  }

  return (
    <pre className="max-h-48 overflow-auto rounded-xl border border-slate-200 bg-slate-900/5 p-3 text-xs leading-relaxed text-slate-800 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100">
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}

export default function Copilot() {
  const [open, setOpen] = useState(false);
  const [tab, setTab] = useState("accelerate");
  const [tableName, setTableName] = useState("tf_oh_demo_order");
  const [actionLoading, setActionLoading] = useState(null);
  const [actionData, setActionData] = useState(null);
  const [actionError, setActionError] = useState(null);
  const [messages, setMessages] = useState(() => [
    { role: "bot", text: "你好，我是采集助手。探索模式下可输入关键词获取说明。" },
  ]);
  const [draft, setDraft] = useState("");
  const panelRef = useRef(null);

  useEffect(() => {
    if (!open) return;
    const onKey = (e) => {
      if (e.key === "Escape") setOpen(false);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open]);

  const runAction = useCallback(async (key, fn) => {
    setActionLoading(key);
    setActionError(null);
    setActionData(null);
    try {
      const out = await fn();
      setActionData(out);
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally {
      setActionLoading(null);
    }
  }, []);

  const quickActions = [
    {
      key: "hbase",
      label: "生成建表命令",
      desc: "样例表名调用 Skill",
      Icon: Terminal,
      onClick: () =>
        runAction("hbase", () =>
          apiPost("/skills/flow_orchestration/generate_hbase_cmd", SAMPLE_HBASE_BODY),
        ),
    },
    {
      key: "recommend",
      label: "AI 参数推荐",
      desc: "使用上方表名",
      Icon: Gauge,
      onClick: () => {
        const name = tableName.trim();
        if (!name) {
          setActionError("请先填写表名");
          setActionData(null);
          return;
        }
        return runAction("recommend", () =>
          apiPost("/params/recommend", { table_name: name }),
        );
      },
    },
    {
      key: "validate",
      label: "参数校验",
      desc: "示例 Flink/HBase 参数",
      Icon: Shield,
      onClick: () =>
        runAction("validate", () =>
          apiPost("/params/validate", { params: SAMPLE_VALIDATE_PARAMS }),
        ),
    },
    {
      key: "anomaly",
      label: "异常检测",
      desc: "示例指标序列",
      Icon: Zap,
      onClick: () =>
        runAction("anomaly", () =>
          apiPost("/monitor/anomaly-detect", { metrics: SAMPLE_ANOMALY_METRICS }),
        ),
    },
    {
      key: "health",
      label: "健康评分",
      desc: "综合维度得分",
      Icon: Bot,
      onClick: () => runAction("health", () => apiPost("/monitor/health-score", {})),
    },
  ];

  const sendExplore = useCallback(() => {
    const t = draft.trim();
    if (!t) return;
    setDraft("");
    setMessages((m) => [...m, { role: "user", text: t }]);
    const reply = matchExploreReply(t);
    setMessages((m) => [...m, { role: "bot", text: reply }]);
  }, [draft]);

  return (
    <>
      <button
        type="button"
        aria-expanded={open}
        aria-label={open ? "收起 AI 助手" : "打开 AI 助手"}
        onClick={() => setOpen((v) => !v)}
        className="fixed bottom-6 right-6 z-50 flex h-14 w-14 items-center justify-center rounded-full bg-indigo-600 text-white shadow-lg transition hover:bg-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-400 focus:ring-offset-2"
      >
        {open ? (
          <ChevronUp className="h-7 w-7" aria-hidden />
        ) : (
          <span className="relative flex h-10 w-10 items-center justify-center">
            <Sparkles className="absolute h-6 w-6 text-indigo-200 opacity-90" aria-hidden />
            <Bot className="relative h-7 w-7" aria-hidden />
          </span>
        )}
      </button>

      <div
        ref={panelRef}
        className={[
          "fixed bottom-0 right-6 z-40 flex w-96 max-h-[70vh] flex-col rounded-t-2xl border border-slate-200 bg-white shadow-2xl transition-transform duration-300 ease-out dark:border-slate-700 dark:bg-slate-900",
          open ? "translate-y-0" : "translate-y-full pointer-events-none",
        ].join(" ")}
        style={{ paddingBottom: "env(safe-area-inset-bottom, 0px)" }}
      >
        <div className="flex shrink-0 items-center justify-between border-b border-slate-100 px-4 py-3 dark:border-slate-800">
          <div className="flex items-center gap-2">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-indigo-50 text-indigo-600 dark:bg-indigo-950/40 dark:text-indigo-300">
              <Bot className="h-5 w-5" aria-hidden />
            </div>
            <div>
              <h2 className="text-sm font-semibold text-slate-900 dark:text-slate-100">AI Copilot</h2>
              <p className="text-xs text-slate-500 dark:text-slate-400">采集流程助手</p>
            </div>
          </div>
          <button
            type="button"
            onClick={() => setOpen(false)}
            className="rounded-lg p-1.5 text-slate-500 transition hover:bg-slate-100 hover:text-slate-800 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-slate-100"
            aria-label="关闭"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="flex shrink-0 border-b border-slate-100 px-2 dark:border-slate-800">
          <button
            type="button"
            onClick={() => setTab("accelerate")}
            className={[
              "flex flex-1 items-center justify-center gap-1.5 border-b-2 py-2.5 text-sm font-medium transition",
              tab === "accelerate"
                ? "border-indigo-600 text-indigo-600"
                : "border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200",
            ].join(" ")}
          >
            <Zap className="h-4 w-4" />
            加速模式
          </button>
          <button
            type="button"
            onClick={() => setTab("explore")}
            className={[
              "flex flex-1 items-center justify-center gap-1.5 border-b-2 py-2.5 text-sm font-medium transition",
              tab === "explore"
                ? "border-indigo-600 text-indigo-600"
                : "border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200",
            ].join(" ")}
          >
            <MessageSquare className="h-4 w-4" />
            探索模式
          </button>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto px-3 py-3">
          {tab === "accelerate" && (
            <div className="space-y-3">
              <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 dark:border-slate-700 dark:bg-slate-800/60">
                <label htmlFor="copilot-table" className="text-xs font-medium text-slate-600 dark:text-slate-300">
                  表名（用于 AI 参数推荐）
                </label>
                <input
                  id="copilot-table"
                  value={tableName}
                  onChange={(e) => setTableName(e.target.value)}
                  className="mt-1 w-full rounded-md border border-slate-200 bg-white px-2 py-1.5 text-sm text-slate-900 outline-none ring-indigo-500 focus:ring-2 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
                  placeholder="例如 tf_oh_xxx"
                />
              </div>

              <div className="grid grid-cols-2 gap-2">
                {quickActions.map(({ key, label, desc, Icon, onClick }) => (
                  <button
                    key={key}
                    type="button"
                    disabled={!!actionLoading}
                    onClick={onClick}
                    className="flex flex-col items-start gap-1 rounded-xl border border-slate-200 bg-white p-3 text-left shadow-sm transition hover:border-indigo-200 hover:shadow-md disabled:opacity-60 dark:border-slate-700 dark:bg-slate-800 dark:hover:border-indigo-700"
                  >
                    <Icon
                      className="h-5 w-5 text-indigo-600"
                      aria-hidden
                    />
                    <span className="text-sm font-medium text-slate-900 dark:text-slate-100">{label}</span>
                    <span className="text-xs text-slate-500 dark:text-slate-400">{desc}</span>
                  </button>
                ))}
              </div>

              <div>
                <p className="mb-1 text-xs font-medium text-slate-500 dark:text-slate-400">执行结果</p>
                <ResultBlock
                  data={actionData}
                  error={actionError}
                  loading={!!actionLoading}
                />
              </div>
            </div>
          )}

          {tab === "explore" && (
            <div className="flex h-full min-h-[220px] flex-col">
              <div className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
                {messages.map((msg, i) => (
                  <div
                    key={i}
                    className={msg.role === "user" ? "flex justify-end" : "flex justify-start"}
                  >
                    <div
                      className={[
                        "max-w-[90%] rounded-2xl px-3 py-2 text-sm leading-relaxed",
                        msg.role === "user"
                          ? "bg-indigo-600 text-white"
                          : "bg-slate-100 text-slate-800",
                      ].join(" ")}
                    >
                      {msg.text}
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-2 flex shrink-0 gap-2 border-t border-slate-100 pt-2 dark:border-slate-800">
                <input
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      sendExplore();
                    }
                  }}
                  className="min-w-0 flex-1 rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 outline-none ring-indigo-500 focus:ring-2 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-100"
                  placeholder="输入问题或关键词…"
                />
                <button
                  type="button"
                  onClick={sendExplore}
                  className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-indigo-600 text-white transition hover:bg-indigo-500"
                  aria-label="发送"
                >
                  <Send className="h-4 w-4" />
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
