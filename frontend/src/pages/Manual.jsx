import { useEffect, useMemo, useState } from "react";
import {
  AlertTriangle,
  BookOpen,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Cloud,
  Cpu,
  Database,
  HardDrive,
  Info,
  Layers,
  Loader2,
  Server,
  Shield,
  Terminal,
  Zap,
} from "lucide-react";

const SYSTEM_STYLES = {
  HBase: "bg-orange-100 text-orange-900 ring-orange-300/50 dark:bg-orange-950/50 dark:text-orange-200",
  HDFS: "bg-blue-100 text-blue-900 ring-blue-300/50 dark:bg-blue-950/50 dark:text-blue-200",
  Flink: "bg-cyan-100 text-cyan-900 ring-cyan-300/50 dark:bg-cyan-950/50 dark:text-cyan-200",
  BDI: "bg-violet-100 text-violet-900 ring-violet-300/50 dark:bg-violet-950/50 dark:text-violet-200",
  MC: "bg-emerald-100 text-emerald-900 ring-emerald-300/50 dark:bg-emerald-950/50 dark:text-emerald-200",
  Kafka: "bg-red-100 text-red-900 ring-red-300/50 dark:bg-red-950/50 dark:text-red-200",
};

const SYSTEM_ICONS = {
  HBase: Database,
  HDFS: HardDrive,
  Flink: Cpu,
  BDI: Layers,
  MC: Server,
  Kafka: Zap,
};

function normalizeManual(raw) {
  if (!raw || typeof raw !== "object") return { steps: [] };
  if (Array.isArray(raw)) return { steps: raw };
  if (Array.isArray(raw.steps)) return raw;
  if (Array.isArray(raw.data?.steps)) return { steps: raw.data.steps };
  return { steps: [] };
}

function normalizeBenchmark(raw) {
  if (!raw || typeof raw !== "object") return { rows: [] };
  if (Array.isArray(raw)) return { rows: raw };
  if (Array.isArray(raw.rows)) return raw;
  if (Array.isArray(raw.data?.rows)) return { rows: raw.data.rows };
  return { rows: [] };
}

function stepCircleGradient(index) {
  const n = index + 1;
  if (n <= 2) return "from-sky-500 via-blue-600 to-indigo-600 shadow-blue-500/25";
  if (n <= 4) return "from-violet-500 via-purple-600 to-fuchsia-600 shadow-purple-500/25";
  return "from-emerald-500 via-teal-600 to-green-600 shadow-emerald-500/25";
}

function automationBadgeClass(mode) {
  const m = String(mode || "").toLowerCase();
  if (m === "auto" || m === "自动" || m === "全自动")
    return "bg-emerald-100 text-emerald-900 ring-emerald-400/40 dark:bg-emerald-950/50 dark:text-emerald-200";
  if (m === "semi-auto" || m === "半自动" || m === "semi")
    return "bg-amber-100 text-amber-950 ring-amber-400/40 dark:bg-amber-950/40 dark:text-amber-100";
  if (m === "manual" || m === "人工" || m === "手动")
    return "bg-red-100 text-red-900 ring-red-400/40 dark:bg-red-950/50 dark:text-red-200";
  return "bg-slate-100 text-slate-700 ring-slate-400/30 dark:bg-slate-800 dark:text-slate-300";
}

function automationLabel(mode) {
  const m = String(mode || "").toLowerCase();
  if (m === "auto" || m === "自动" || m === "全自动") return "自动";
  if (m === "semi-auto" || m === "半自动" || m === "semi") return "半自动";
  if (m === "manual" || m === "人工" || m === "手动") return "人工";
  return mode || "—";
}

function gapBadgeClass(level) {
  const L = String(level || "").toUpperCase();
  if (L === "P0")
    return "bg-red-100 text-red-900 ring-red-500/30 dark:bg-red-950/50 dark:text-red-200";
  if (L === "P1")
    return "bg-amber-100 text-amber-950 ring-amber-500/30 dark:bg-amber-950/40 dark:text-amber-100";
  if (L === "P2")
    return "bg-blue-100 text-blue-900 ring-blue-500/30 dark:bg-blue-950/50 dark:text-blue-200";
  return "bg-slate-100 text-slate-700 ring-slate-400/30 dark:bg-slate-800 dark:text-slate-300";
}

function riskDotClass(level) {
  const s = String(level || "").toLowerCase();
  if (s === "high" || s === "高" || s === "critical")
    return "bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]";
  if (s === "medium" || s === "中" || s === "warning")
    return "bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.5)]";
  if (s === "low" || s === "低" || s === "info")
    return "bg-emerald-500 shadow-[0_0_6px_rgba(16,185,129,0.45)]";
  return "bg-slate-400";
}

function opAutomationKey(op, step) {
  const v = op?.automation ?? step?.automation ?? "";
  const m = String(v).toLowerCase();
  if (m === "auto" || m === "自动" || m === "全自动") return "auto";
  if (m === "semi-auto" || m === "半自动" || m === "semi") return "semi";
  if (m === "manual" || m === "人工" || m === "手动") return "manual";
  return "manual";
}

function isBenchmarkRowWeak(row) {
  const gap = String(row?.gapLevel ?? row?.gap ?? "").toUpperCase();
  if (row?.currentWeak === true || row?.weak === true) return true;
  if (row?.highlightWeakness === true) return true;
  if (gap === "P0" || gap === "P1") return true;
  return false;
}

function StatMiniCard({ icon: Icon, label, value, accent }) {
  return (
    <div className="group relative overflow-hidden rounded-2xl border border-slate-200/90 bg-white/90 p-5 shadow-sm backdrop-blur-sm transition-all duration-300 hover:border-indigo-200 hover:shadow-md dark:border-slate-700/90 dark:bg-slate-900/80 dark:hover:border-indigo-500/40">
      <div
        className={`pointer-events-none absolute -right-6 -top-6 h-24 w-24 rounded-full opacity-20 blur-2xl transition-all duration-300 group-hover:opacity-35 ${accent}`}
        aria-hidden
      />
      <div className="relative flex items-start gap-4">
        <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-xl bg-slate-900 text-white shadow-inner dark:bg-indigo-600">
          <Icon className="h-5 w-5" aria-hidden />
        </div>
        <div className="min-w-0">
          <p className="text-xs font-medium uppercase tracking-wider text-slate-500 dark:text-slate-400">
            {label}
          </p>
          <p className="mt-1 text-2xl font-semibold tabular-nums tracking-tight text-slate-900 dark:text-white">
            {value}
          </p>
        </div>
      </div>
    </div>
  );
}

export default function Manual() {
  const [manual, setManual] = useState(null);
  const [benchmark, setBenchmark] = useState(null);
  const [samples, setSamples] = useState(null);
  const [tab, setTab] = useState("flow");
  const [expanded, setExpanded] = useState(() => new Set(["step-1"]));
  const [opExpanded, setOpExpanded] = useState(() => new Set());

  useEffect(() => {
    fetch("/api/manual")
      .then((r) => r.json())
      .then(setManual)
      .catch(() => setManual({ steps: [] }));
    fetch("/api/manual/benchmark")
      .then((r) => r.json())
      .then(setBenchmark)
      .catch(() => setBenchmark({ rows: [] }));
    fetch("/api/manual/samples")
      .then((r) => r.json())
      .then(setSamples)
      .catch(() => setSamples({ scripts: [], jars: [] }));
  }, []);

  const { steps } = useMemo(() => normalizeManual(manual), [manual]);
  const { rows: benchmarkRows } = useMemo(
    () => normalizeBenchmark(benchmark),
    [benchmark],
  );

  const stats = useMemo(() => {
    const stepCount = steps.length || 0;
    let totalOps = 0;
    let autoOps = 0;
    let needHuman = 0;
    for (const step of steps) {
      const ops = Array.isArray(step?.operations) ? step.operations : [];
      for (const op of ops) {
        totalOps += 1;
        const k = opAutomationKey(op, step);
        if (k === "auto") autoOps += 1;
        else needHuman += 1;
      }
    }
    return {
      stepCount: stepCount || 6,
      totalOps: totalOps || 16,
      autoOps,
      needHuman,
    };
  }, [steps]);

  const loading = manual === null;

  const toggleStep = (stepKey) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(stepKey)) next.delete(stepKey);
      else next.add(stepKey);
      return next;
    });
  };

  const toggleOperation = (opKey) => {
    setOpExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(opKey)) next.delete(opKey);
      else next.add(opKey);
      return next;
    });
  };

  if (loading) {
    return (
      <div className="flex min-h-[420px] flex-col items-center justify-center gap-4">
        <Loader2
          className="h-10 w-10 animate-spin text-indigo-600 dark:text-indigo-400"
          aria-hidden
        />
        <p className="text-sm font-medium text-slate-500 dark:text-slate-400">
          正在加载操作手册…
        </p>
      </div>
    );
  }

  return (
    <div className="relative mx-auto max-w-6xl pb-16">
      <div
        className="pointer-events-none absolute inset-x-0 -top-6 h-48 rounded-b-[2.5rem] bg-gradient-to-b from-indigo-100/90 via-white/0 to-transparent dark:from-indigo-950/40 dark:via-transparent"
        aria-hidden
      />

      <header className="relative mb-8 flex flex-col gap-6 border-b border-slate-200/80 pb-8 dark:border-slate-700/80 lg:flex-row lg:items-end lg:justify-between">
        <div className="flex min-w-0 items-start gap-4">
          <div className="flex h-14 w-14 shrink-0 items-center justify-center rounded-2xl bg-gradient-to-br from-indigo-600 to-violet-700 text-white shadow-lg shadow-indigo-500/30">
            <BookOpen className="h-7 w-7" aria-hidden />
          </div>
          <div className="min-w-0">
            <h1 className="text-3xl font-bold tracking-tight text-slate-900 dark:text-white">
              操作手册
            </h1>
            <p className="mt-2 max-w-2xl text-sm leading-relaxed text-slate-600 dark:text-slate-400">
              智能订单采集全流程操作指南 — 6 大步骤 · 16 项操作
            </p>
            <div className="mt-4 flex flex-wrap items-center gap-2 text-xs text-slate-500 dark:text-slate-500">
              <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2.5 py-1 font-medium text-slate-700 ring-1 ring-slate-200/80 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-600/60">
                <Shield className="h-3.5 w-3.5" aria-hidden />
                企业级文档
              </span>
              <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2.5 py-1 font-medium text-slate-700 ring-1 ring-slate-200/80 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-600/60">
                <Zap className="h-3.5 w-3.5" aria-hidden />
                流程与对标一体化
              </span>
            </div>
          </div>
        </div>

        <div className="flex shrink-0 rounded-xl bg-slate-100/90 p-1 ring-1 ring-slate-200/80 dark:bg-slate-800/80 dark:ring-slate-700">
          <button
            type="button"
            onClick={() => setTab("flow")}
            className={[
              "rounded-lg px-4 py-2 text-sm font-semibold transition-all duration-300",
              tab === "flow"
                ? "bg-white text-indigo-700 shadow-sm ring-1 ring-slate-200/90 dark:bg-slate-900 dark:text-indigo-300 dark:ring-slate-600"
                : "text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white",
            ].join(" ")}
          >
            采集流程
          </button>
          <button
            type="button"
            onClick={() => setTab("benchmark")}
            className={[
              "rounded-lg px-4 py-2 text-sm font-semibold transition-all duration-300",
              tab === "benchmark"
                ? "bg-white text-indigo-700 shadow-sm ring-1 ring-slate-200/90 dark:bg-slate-900 dark:text-indigo-300 dark:ring-slate-600"
                : "text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white",
            ].join(" ")}
          >
            对标矩阵
          </button>
          <button
            type="button"
            onClick={() => setTab("samples")}
            className={[
              "rounded-lg px-4 py-2 text-sm font-semibold transition-all duration-300",
              tab === "samples"
                ? "bg-white text-indigo-700 shadow-sm ring-1 ring-slate-200/90 dark:bg-slate-900 dark:text-indigo-300 dark:ring-slate-600"
                : "text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white",
            ].join(" ")}
          >
            脚本/JAR样例
          </button>
        </div>
      </header>

      {tab === "flow" ? (
        <>
          <section className="mb-10 grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            <StatMiniCard
              icon={Layers}
              label="总步骤数"
              value={stats.stepCount}
              accent="bg-indigo-400"
            />
            <StatMiniCard
              icon={Terminal}
              label="总操作数"
              value={stats.totalOps}
              accent="bg-violet-400"
            />
            <StatMiniCard
              icon={CheckCircle2}
              label="自动化操作"
              value={stats.autoOps}
              accent="bg-emerald-400"
            />
            <StatMiniCard
              icon={AlertTriangle}
              label="需人工确认"
              value={stats.needHuman}
              accent="bg-amber-400"
            />
          </section>

          <div className="relative">
            <div
              className="absolute left-[1.35rem] top-4 bottom-4 hidden w-px bg-gradient-to-b from-blue-300/80 via-purple-300/60 to-emerald-300/80 md:block dark:from-blue-500/40 dark:via-purple-500/30 dark:to-emerald-500/40"
              aria-hidden
            />

            <ul className="relative flex flex-col gap-5">
              {steps.length === 0 ? (
                <li className="rounded-2xl border border-dashed border-slate-300 bg-slate-50/80 p-10 text-center dark:border-slate-600 dark:bg-slate-900/40">
                  <Info className="mx-auto mb-3 h-10 w-10 text-slate-400" aria-hidden />
                  <p className="text-sm font-medium text-slate-700 dark:text-slate-300">
                    尚未加载到流程步骤数据
                  </p>
                  <p className="mt-2 text-xs text-slate-500 dark:text-slate-500">
                    请确认后端已提供 <code className="rounded bg-slate-200 px-1 py-0.5 font-mono dark:bg-slate-800">/api/manual</code>{" "}
                    接口并返回 steps 列表。
                  </p>
                </li>
              ) : null}
              {steps.map((step, idx) => {
                const num = step.number ?? step.step ?? idx + 1;
                const stepKey = `step-${num}`;
                const open = expanded.has(stepKey);
                const name = step.name ?? step.title ?? `步骤 ${num}`;
                const category = step.category ?? step.stage ?? "";
                const systemsRaw = step.systems ?? step.system ?? [];
                const systems = Array.isArray(systemsRaw)
                  ? systemsRaw
                  : typeof systemsRaw === "string"
                    ? systemsRaw.split(/[,，]/).map((s) => s.trim()).filter(Boolean)
                    : [];
                const autoMode = step.automation ?? step.auto ?? "";
                const risk = step.risk ?? step.riskLevel ?? "";
                const desc = step.description ?? step.summary ?? "";
                const ops = Array.isArray(step.operations) ? step.operations : [];

                return (
                  <li key={step.id ?? idx} className="relative md:pl-14">
                    <div className="absolute left-0 top-5 hidden md:flex md:items-center md:justify-center">
                      <div
                        className={`flex h-11 w-11 items-center justify-center rounded-full bg-gradient-to-br text-sm font-bold text-white shadow-lg ring-4 ring-white dark:ring-slate-900 ${stepCircleGradient(idx)}`}
                      >
                        {num}
                      </div>
                    </div>

                    <article
                      className={`overflow-hidden rounded-2xl border bg-white/95 shadow-sm backdrop-blur-sm transition-all duration-300 dark:bg-slate-900/90 ${
                        open
                          ? "border-indigo-200/90 shadow-md shadow-indigo-500/10 dark:border-indigo-500/35"
                          : "border-slate-200/90 hover:border-slate-300 dark:border-slate-700 dark:hover:border-slate-600"
                      }`}
                    >
                      <button
                        type="button"
                        onClick={() => toggleStep(stepKey)}
                        className="flex w-full items-start gap-4 p-5 text-left transition-colors duration-300 hover:bg-slate-50/80 dark:hover:bg-slate-800/50"
                      >
                        <div
                          className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-gradient-to-br text-sm font-bold text-white shadow-md md:hidden ${stepCircleGradient(idx)}`}
                        >
                          {num}
                        </div>
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <h2 className="text-lg font-semibold text-slate-900 dark:text-white">
                              {name}
                            </h2>
                            {category ? (
                              <span className="inline-flex items-center rounded-md bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-700 ring-1 ring-slate-200/80 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-600">
                                {category}
                              </span>
                            ) : null}
                            <span
                              className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-semibold ring-1 ring-inset ${automationBadgeClass(autoMode)}`}
                            >
                              {automationLabel(autoMode)}
                            </span>
                            <span
                              className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-600 dark:text-slate-400"
                              title="风险指示"
                            >
                              <span
                                className={`inline-block h-2 w-2 rounded-full ${riskDotClass(risk)}`}
                              />
                              {risk ? String(risk) : "风险"}
                            </span>
                          </div>
                          <div className="mt-3 flex flex-wrap gap-1.5">
                            {systems.map((sys) => {
                              const S = String(sys).trim();
                              const cls =
                                SYSTEM_STYLES[S] ??
                                "bg-slate-100 text-slate-800 ring-slate-300/40 dark:bg-slate-800 dark:text-slate-200";
                              const SysIcon = SYSTEM_ICONS[S] ?? Cloud;
                              return (
                                <span
                                  key={S}
                                  className={`inline-flex items-center gap-1 rounded-md px-2 py-0.5 text-[11px] font-semibold ring-1 ring-inset ${cls}`}
                                >
                                  <SysIcon className="h-3 w-3 opacity-80" aria-hidden />
                                  {S}
                                </span>
                              );
                            })}
                          </div>
                          {desc ? (
                            <p className="mt-3 text-sm leading-relaxed text-slate-600 dark:text-slate-400">
                              {desc}
                            </p>
                          ) : null}
                        </div>
                        <div className="shrink-0 text-slate-400 dark:text-slate-500">
                          {open ? (
                            <ChevronUp className="h-5 w-5 transition-transform duration-300" />
                          ) : (
                            <ChevronDown className="h-5 w-5 transition-transform duration-300" />
                          )}
                        </div>
                      </button>

                      {open ? (
                        <div className="border-t border-slate-100 bg-slate-50/80 px-5 py-5 transition-all duration-300 dark:border-slate-800 dark:bg-slate-950/40">
                          <div className="mb-4 rounded-lg border border-indigo-200 bg-indigo-50 px-3 py-2 text-xs text-indigo-800 dark:border-indigo-700/50 dark:bg-indigo-950/30 dark:text-indigo-200">
                            自动化标注说明：<span className="font-semibold">自动</span> = 可系统自动执行；<span className="font-semibold">半自动</span> = 系统生成结果后人工确认；<span className="font-semibold">人工</span> = 必须人工操作。
                          </div>
                            {ops.length === 0 ? (
                              <p className="text-sm text-slate-500 dark:text-slate-500">
                                暂无子操作说明（请在后端配置 operations 列表）。
                              </p>
                            ) : (
                              <ul className="flex flex-col gap-4">
                                {ops.map((op, oi) => {
                                  const oid =
                                    op.id ??
                                    op.code ??
                                    `${num}.${oi + 1}`;
                                  const oname = op.name ?? op.title ?? "操作";
                                  const otype = op.type ?? op.kind ?? "";
                                  const opAuto = automationLabel(op.type ?? step.automation ?? "");
                                  const odesc =
                                    op.description ?? op.detail ?? "";
                                  const cmd = op.command ?? op.cmd ?? op.shell;
                                  const params = Array.isArray(op.params)
                                    ? op.params
                                    : Array.isArray(op.parameters)
                                      ? op.parameters
                                      : [];
                                  const tips = Array.isArray(op.tips)
                                    ? op.tips
                                    : op.tip
                                      ? [op.tip]
                                      : [];
                                  const orisk =
                                    op.riskLevel ?? op.risk ?? "";

                                  const opKey = `step-${num}-op-${oid}`;
                                  const opOpen = opExpanded.has(opKey);
                                  return (
                                    <li
                                      key={`${oid}-${oi}`}
                                      className="rounded-xl border border-slate-200/90 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-900"
                                    >
                                      <div className="flex flex-wrap items-start gap-2">
                                        <button
                                          type="button"
                                          onClick={() => toggleOperation(opKey)}
                                          className="inline-flex h-6 w-6 items-center justify-center rounded border border-slate-300 text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-800"
                                          title={opOpen ? "收起详情" : "展开详情"}
                                        >
                                          {opOpen ? (
                                            <ChevronUp className="h-3.5 w-3.5" />
                                          ) : (
                                            <ChevronDown className="h-3.5 w-3.5" />
                                          )}
                                        </button>
                                        <span className="inline-flex items-center rounded-md bg-indigo-50 px-2 py-0.5 font-mono text-xs font-bold text-indigo-800 ring-1 ring-indigo-200/80 dark:bg-indigo-950/50 dark:text-indigo-200 dark:ring-indigo-700/50">
                                          {String(oid)}
                                        </span>
                                        <h3 className="text-base font-semibold text-slate-900 dark:text-white">
                                          {oname}
                                        </h3>
                                        {otype ? (
                                          <span className="inline-flex items-center rounded-md bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-700 ring-1 ring-slate-200 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-600">
                                            {otype}
                                          </span>
                                        ) : null}
                                        <span className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-semibold ring-1 ring-inset ${automationBadgeClass(opAuto)}`}>
                                          {opAuto === "人工" ? "人工确认" : opAuto}
                                        </span>
                                        <span
                                          className="ml-auto inline-flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400"
                                          title="操作风险"
                                        >
                                          <span
                                            className={`h-2 w-2 rounded-full ${riskDotClass(orisk)}`}
                                          />
                                        </span>
                                      </div>
                                      {opOpen ? (
                                        <div>
                                          {odesc ? (
                                            <p className="mt-2 text-sm leading-relaxed text-slate-600 dark:text-slate-400">
                                              {odesc}
                                            </p>
                                          ) : null}

                                          {cmd ? (
                                            <div className="mt-3">
                                          <div className="mb-1.5 flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-500">
                                            <Terminal className="h-3.5 w-3.5" aria-hidden />
                                            命令
                                          </div>
                                          <pre className="overflow-x-auto rounded-lg border border-slate-800 bg-slate-900 p-3 font-mono text-xs leading-relaxed text-green-400 shadow-inner">
                                            {String(cmd)}
                                          </pre>
                                            </div>
                                          ) : null}

                                          {params.length > 0 ? (
                                            <div className="mt-3 overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-700">
                                          <table className="min-w-full text-left text-sm">
                                            <thead>
                                              <tr className="border-b border-slate-200 bg-slate-50 text-xs font-semibold uppercase tracking-wide text-slate-500 dark:border-slate-700 dark:bg-slate-800/80 dark:text-slate-400">
                                                <th className="px-3 py-2">参数名</th>
                                                <th className="px-3 py-2">说明</th>
                                                <th className="px-3 py-2">是否必填</th>
                                                <th className="px-3 py-2">默认值</th>
                                              </tr>
                                            </thead>
                                            <tbody className="divide-y divide-slate-100 dark:divide-slate-800">
                                              {params.map((p, pi) => (
                                                <tr
                                                  key={pi}
                                                  className="bg-white dark:bg-slate-900/80"
                                                >
                                                  <td className="px-3 py-2 font-mono text-xs font-semibold text-slate-900 dark:text-slate-100">
                                                    {p.name ?? p.key ?? "—"}
                                                  </td>
                                                  <td className="px-3 py-2 text-slate-600 dark:text-slate-400">
                                                    {p.description ??
                                                      p.desc ??
                                                      p.说明 ??
                                                      "—"}
                                                  </td>
                                                  <td className="px-3 py-2 text-slate-700 dark:text-slate-300">
                                                    {p.required === true ||
                                                    p.必填 === true ||
                                                    p.required === "是"
                                                      ? "是"
                                                      : p.required === false ||
                                                          p.必填 === false
                                                        ? "否"
                                                        : String(
                                                            p.required ??
                                                              p.必填 ??
                                                              "—",
                                                          )}
                                                  </td>
                                                  <td className="px-3 py-2 font-mono text-xs text-slate-600 dark:text-slate-400">
                                                    {p.default ??
                                                      p.defaultValue ??
                                                      p.默认值 ??
                                                      "—"}
                                                  </td>
                                                </tr>
                                              ))}
                                            </tbody>
                                          </table>
                                            </div>
                                          ) : null}

                                          {tips.length > 0 ? (
                                            <div className="mt-3 space-y-2">
                                          {tips.map((t, ti) => (
                                            <div
                                              key={ti}
                                              className="flex gap-3 rounded-lg border border-amber-300/80 bg-amber-50/90 p-3 text-sm text-amber-950 shadow-sm dark:border-amber-700/50 dark:bg-amber-950/30 dark:text-amber-100"
                                            >
                                              <Info
                                                className="mt-0.5 h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400"
                                                aria-hidden
                                              />
                                              <p className="leading-relaxed">{t}</p>
                                            </div>
                                          ))}
                                            </div>
                                          ) : null}
                                        </div>
                                      ) : null}
                                    </li>
                                  );
                                })}
                              </ul>
                            )}
                        </div>
                      ) : null}
                    </article>
                  </li>
                );
              })}
            </ul>
          </div>
        </>
      ) : tab === "benchmark" ? (
        <section className="rounded-2xl border border-slate-200/90 bg-white/95 shadow-sm dark:border-slate-700 dark:bg-slate-900/90">
          {benchmark === null ? (
            <div className="flex min-h-[280px] flex-col items-center justify-center gap-3 p-8">
              <Loader2 className="h-9 w-9 animate-spin text-indigo-600 dark:text-indigo-400" />
              <p className="text-sm text-slate-500">加载对标矩阵…</p>
            </div>
          ) : (
            <div className="overflow-x-auto rounded-2xl">
              <table className="min-w-[920px] w-full border-collapse text-sm">
                <thead>
                  <tr className="border-b border-slate-200 bg-gradient-to-r from-slate-50 to-slate-100/80 text-left text-xs font-bold uppercase tracking-wider text-slate-600 dark:border-slate-700 dark:from-slate-900 dark:to-slate-800 dark:text-slate-400">
                    <th className="px-4 py-3">能力维度</th>
                    <th className="px-4 py-3">阿里云</th>
                    <th className="px-4 py-3">腾讯云</th>
                    <th className="px-4 py-3">字节跳动</th>
                    <th className="px-4 py-3">当前平台</th>
                    <th className="px-4 py-3">差距等级</th>
                    <th className="px-4 py-3">升级计划</th>
                  </tr>
                </thead>
                <tbody>
                  {benchmarkRows.length === 0 ? (
                    <tr>
                      <td
                        colSpan={7}
                        className="px-4 py-12 text-center text-slate-500 dark:text-slate-500"
                      >
                        暂无对标数据（请配置 /api/manual/benchmark）。
                      </td>
                    </tr>
                  ) : (
                    benchmarkRows.map((row, ri) => {
                      const dim =
                        row.dimension ??
                        row.能力维度 ??
                        row.name ??
                        "—";
                      const aliyun =
                        row.aliyun ?? row.阿里云 ?? row.ali ?? "—";
                      const tencent =
                        row.tencent ?? row.腾讯云 ?? "—";
                      const bytedance =
                        row.bytedance ??
                        row.字节跳动 ??
                        row.byte ??
                        "—";
                      const current =
                        row.current ?? row.当前平台 ?? row.platform ?? "—";
                      const gapLevel =
                        row.gapLevel ?? row.差距等级 ?? row.gap ?? "—";
                      const plan =
                        row.upgradePlan ?? row.升级计划 ?? row.plan ?? "—";
                      const weak = isBenchmarkRowWeak(row);

                      return (
                        <tr
                          key={ri}
                          className="border-b border-slate-100 transition-colors duration-300 hover:bg-indigo-50/50 dark:border-slate-800 dark:hover:bg-indigo-950/20"
                        >
                          <td className="px-4 py-3 font-semibold text-slate-900 dark:text-white">
                            {dim}
                          </td>
                          <td className="max-w-[200px] px-4 py-3 text-slate-600 dark:text-slate-400">
                            {aliyun}
                          </td>
                          <td className="max-w-[200px] px-4 py-3 text-slate-600 dark:text-slate-400">
                            {tencent}
                          </td>
                          <td className="max-w-[200px] px-4 py-3 text-slate-600 dark:text-slate-400">
                            {bytedance}
                          </td>
                          <td
                            className={`max-w-[220px] px-4 py-3 font-medium ${
                              weak
                                ? "text-red-600 dark:text-red-400"
                                : "text-slate-700 dark:text-slate-300"
                            }`}
                          >
                            {current}
                          </td>
                          <td className="px-4 py-3">
                            <span
                              className={`inline-flex rounded-md px-2 py-0.5 text-xs font-bold ring-1 ring-inset ${gapBadgeClass(gapLevel)}`}
                            >
                              {String(gapLevel).toUpperCase()}
                            </span>
                          </td>
                          <td className="max-w-[260px] px-4 py-3 text-slate-600 dark:text-slate-400">
                            {plan}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          )}
        </section>
      ) : (
        <section className="space-y-5">
          <div className="rounded-xl border border-slate-200 bg-white p-4 text-sm text-slate-700 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-300">
            当前样例目录：<span className="font-mono">{samples?.folder || "/hbaseTest"}</span>
          </div>

          <div className="grid gap-5 lg:grid-cols-2">
            <div className="rounded-2xl border border-slate-200/90 bg-white/95 p-4 shadow-sm dark:border-slate-700 dark:bg-slate-900/90">
              <h3 className="mb-3 text-base font-semibold text-slate-900 dark:text-white">脚本样例 ({samples?.script_count ?? 0})</h3>
              <div className="space-y-4">
                {(samples?.scripts || []).map((s) => (
                  <div key={s.name} className="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
                    <p className="text-sm font-semibold text-slate-900 dark:text-white">{s.name}</p>
                    <p className="mb-2 text-xs text-slate-500">{s.path}</p>
                    <pre className="max-h-64 overflow-auto rounded-lg border border-slate-800 bg-slate-900 p-3 font-mono text-xs leading-relaxed text-green-400">
                      {s.content}
                    </pre>
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-2xl border border-slate-200/90 bg-white/95 p-4 shadow-sm dark:border-slate-700 dark:bg-slate-900/90">
              <h3 className="mb-3 text-base font-semibold text-slate-900 dark:text-white">JAR 样例 ({samples?.jar_count ?? 0})</h3>
              <div className="space-y-3">
                {(samples?.jars || []).map((j) => (
                  <div key={j.name} className="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-700 dark:bg-slate-800/40">
                    <p className="text-sm font-semibold text-slate-900 dark:text-white">{j.name}</p>
                    <p className="text-xs text-slate-500">大小：{j.size_bytes} bytes</p>
                    <p className="truncate text-xs text-slate-500">SHA256：{j.sha256}</p>
                    <p className="mt-2 text-xs text-slate-700 dark:text-slate-300">用法：{j.usage_example}</p>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>
      )}
    </div>
  );
}
