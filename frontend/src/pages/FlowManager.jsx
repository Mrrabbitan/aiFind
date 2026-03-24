import { useCallback, useEffect, useState } from "react";
import {
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Clock,
  Layers,
  Loader2,
  Plus,
  Play,
  XCircle,
  Zap,
} from "lucide-react";
import { api } from "../api";

function TaskStatusBadge({ status }) {
  const st = (status || "pending").toLowerCase();
  const base = "inline-flex rounded-md px-2 py-0.5 text-xs font-medium ring-1 ring-inset";
  if (st === "completed") {
    return (
      <span className={`${base} bg-emerald-100 text-emerald-800 ring-emerald-600/20 dark:bg-emerald-950/50 dark:text-emerald-200`}>
        已完成
      </span>
    );
  }
  if (st === "running") {
    return (
      <span
        className={`${base} animate-pulse bg-sky-100 text-sky-800 ring-sky-600/20 dark:bg-sky-950/50 dark:text-sky-200`}
      >
        运行中
      </span>
    );
  }
  if (st === "failed") {
    return (
      <span className={`${base} bg-red-100 text-red-800 ring-red-600/20 dark:bg-red-950/50 dark:text-red-200`}>
        失败
      </span>
    );
  }
  return (
    <span className={`${base} bg-zinc-100 text-zinc-700 ring-zinc-600/20 dark:bg-zinc-800 dark:text-zinc-300`}>
      待执行
    </span>
  );
}

function StepStatusIcon({ status }) {
  const s = (status || "pending").toLowerCase();
  if (s === "completed") {
    return <CheckCircle2 className="h-5 w-5 text-emerald-600 dark:text-emerald-400" />;
  }
  if (s === "running") {
    return <Loader2 className="h-5 w-5 animate-spin text-blue-600 dark:text-blue-400" />;
  }
  if (s === "failed") {
    return <XCircle className="h-5 w-5 text-red-600 dark:text-red-400" />;
  }
  return <Clock className="h-5 w-5 text-zinc-400" />;
}

function AutomationBadge({ automation }) {
  const a = (automation || "manual").toLowerCase();
  if (a === "auto") {
    return (
      <span className="inline-flex items-center gap-0.5 rounded-md bg-emerald-100 px-1.5 py-0.5 text-[10px] font-medium text-emerald-800 dark:bg-emerald-950/50 dark:text-emerald-200">
        <Zap className="h-3 w-3" />
        自动
      </span>
    );
  }
  if (a === "semi-auto") {
    return (
      <span className="rounded-md bg-amber-100 px-1.5 py-0.5 text-[10px] font-medium text-amber-900 dark:bg-amber-950/40 dark:text-amber-100">
        半自动
      </span>
    );
  }
  return (
    <span className="rounded-md bg-zinc-100 px-1.5 py-0.5 text-[10px] font-medium text-zinc-600 dark:bg-zinc-400">
      手动
    </span>
  );
}

function ProgressBar({ value }) {
  const pct = Math.min(100, Math.max(0, Number(value) || 0));
  return (
    <div className="flex items-center gap-2">
      <div className="h-2 flex-1 overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-700">
        <div
          className="h-full rounded-full bg-blue-600 transition-all dark:bg-blue-500"
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="w-10 shrink-0 text-right text-xs tabular-nums text-zinc-500">{pct}%</span>
    </div>
  );
}

export default function FlowManager() {
  const [tasks, setTasks] = useState([]);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState(null);

  const [expandedIds, setExpandedIds] = useState(() => new Set());
  const [details, setDetails] = useState({});
  const [detailLoading, setDetailLoading] = useState({});
  const [detailErrors, setDetailErrors] = useState({});

  const [modalOpen, setModalOpen] = useState(false);
  const [form, setForm] = useState({
    name: "",
    table_name: "",
    task_type: "full+incremental",
    file_size_gb: 1,
  });
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState(null);

  const [artifactsOpen, setArtifactsOpen] = useState({});

  const loadList = useCallback(async () => {
    setListError(null);
    const list = await api.flows.list();
    setTasks(Array.isArray(list) ? list : []);
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        await loadList();
      } catch (e) {
        if (!cancelled) setListError(e?.message || "加载任务列表失败");
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [loadList]);

  const toggleExpand = async (taskId) => {
    const wasOpen = expandedIds.has(taskId);
    setExpandedIds((prev) => {
      const next = new Set(prev);
      if (next.has(taskId)) next.delete(taskId);
      else next.add(taskId);
      return next;
    });

    if (!wasOpen && !details[taskId]) {
      setDetailLoading((d) => ({ ...d, [taskId]: true }));
      setDetailErrors((d) => ({ ...d, [taskId]: null }));
      try {
        const d = await api.flows.get(taskId);
        setDetails((prev) => ({ ...prev, [taskId]: d }));
      } catch (e) {
        setDetailErrors((prev) => ({ ...prev, [taskId]: e?.message || "加载详情失败" }));
      } finally {
        setDetailLoading((d) => ({ ...d, [taskId]: false }));
      }
    }
  };

  const refreshTaskDetail = async (taskId) => {
    setDetailLoading((d) => ({ ...d, [taskId]: true }));
    setDetailErrors((d) => ({ ...d, [taskId]: null }));
    try {
      const d = await api.flows.get(taskId);
      setDetails((prev) => ({ ...prev, [taskId]: d }));
    } catch (e) {
      setDetailErrors((prev) => ({ ...prev, [taskId]: e?.message || "刷新详情失败" }));
    } finally {
      setDetailLoading((d) => ({ ...d, [taskId]: false }));
    }
  };

  const handleCreate = async (e) => {
    e.preventDefault();
    setFormError(null);
    setSubmitting(true);
    try {
      await api.flows.create({
        name: form.name.trim(),
        table_name: form.table_name.trim(),
        task_type: form.task_type,
        file_size_gb: Number(form.file_size_gb) || 1,
      });
      setModalOpen(false);
      setForm({
        name: "",
        table_name: "",
        task_type: "full+incremental",
        file_size_gb: 1,
      });
      await loadList();
    } catch (err) {
      setFormError(err?.message || "创建失败");
    } finally {
      setSubmitting(false);
    }
  };

  const handleExecute = async (taskId) => {
    try {
      await api.flows.execute(taskId);
      await loadList();
      if (expandedIds.has(taskId)) await refreshTaskDetail(taskId);
    } catch (e) {
      setDetailErrors((prev) => ({
        ...prev,
        [taskId]: e?.message || "开始执行失败",
      }));
    }
  };

  const handleCompleteStep = async (taskId, stepOrder) => {
    try {
      await api.flows.completeStep(taskId, stepOrder);
      await loadList();
      if (expandedIds.has(taskId)) await refreshTaskDetail(taskId);
    } catch (e) {
      setDetailErrors((prev) => ({
        ...prev,
        [taskId]: e?.message || "完成步骤失败",
      }));
    }
  };

  const toggleArtifacts = (taskId) => {
    setArtifactsOpen((prev) => ({ ...prev, [taskId]: !prev[taskId] }));
  };

  if (loading) {
    return (
      <div className="flex min-h-[50vh] flex-col items-center justify-center gap-3 text-zinc-500">
        <Loader2 className="h-10 w-10 animate-spin text-blue-600" />
        <p className="text-sm">正在加载流程任务…</p>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-5xl space-y-8 px-4 py-8 sm:px-6 lg:px-8">
      <header className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-zinc-900 dark:text-zinc-50">
            流程管理
          </h1>
          <p className="mt-1 text-sm text-zinc-500 dark:text-zinc-400">
            创建与管理采集任务流程
          </p>
        </div>
        <button
          type="button"
          onClick={() => {
            setFormError(null);
            setModalOpen(true);
          }}
          className="inline-flex items-center justify-center gap-2 rounded-lg bg-blue-600 px-4 py-2.5 text-sm font-medium text-white shadow-sm transition hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 dark:focus:ring-offset-zinc-900"
        >
          <Plus className="h-4 w-4" />
          新建采集任务
        </button>
      </header>

      {listError ? (
        <div className="rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-800 dark:border-red-900 dark:bg-red-950/40 dark:text-red-200">
          {listError}
        </div>
      ) : null}

      <div className="space-y-4">
        {tasks.length === 0 ? (
          <p className="rounded-xl border border-dashed border-zinc-300 bg-zinc-50 py-12 text-center text-sm text-zinc-500 dark:border-zinc-700 dark:bg-zinc-900/50">
            暂无任务，点击「新建采集任务」开始
          </p>
        ) : (
          tasks.map((task) => {
            const open = expandedIds.has(task.id);
            const detail = details[task.id];
            const dLoading = detailLoading[task.id];
            const dErr = detailErrors[task.id];
            const steps = (detail?.steps ?? []).slice().sort((a, b) => a.step_order - b.step_order);
            const runningStep = steps.find((s) => s.status === "running");
            const snap = detail?.config_snapshot || {};
            const hasArtifacts =
              snap.hbase_cmd || snap.flink_config || snap.bulkload_cmd;

            return (
              <article
                key={task.id}
                className="rounded-xl border border-zinc-200/80 bg-white p-5 shadow-sm transition-colors dark:border-zinc-700 dark:bg-zinc-900/95"
              >
                <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                  <div className="min-w-0 flex-1 space-y-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <Layers className="h-4 w-4 shrink-0 text-zinc-400" />
                      <h2 className="truncate text-lg font-semibold text-zinc-900 dark:text-zinc-50">
                        {task.name}
                      </h2>
                      <TaskStatusBadge status={task.status} />
                    </div>
                    <p className="font-mono text-xs text-zinc-500 dark:text-zinc-400">
                      目标表：{task.table_name}
                    </p>
                    <ProgressBar value={task.progress} />
                  </div>
                  <button
                    type="button"
                    onClick={() => toggleExpand(task.id)}
                    className="inline-flex shrink-0 items-center gap-1 rounded-lg border border-zinc-200 bg-white px-3 py-2 text-sm font-medium text-zinc-700 shadow-sm hover:bg-zinc-50 dark:border-zinc-600 dark:bg-zinc-800 dark:text-zinc-200 dark:hover:bg-zinc-700"
                  >
                    查看详情
                    {open ? (
                      <ChevronUp className="h-4 w-4" />
                    ) : (
                      <ChevronDown className="h-4 w-4" />
                    )}
                  </button>
                </div>

                {open ? (
                  <div className="mt-6 border-t border-zinc-100 pt-6 dark:border-zinc-800">
                    {dLoading && !detail ? (
                      <div className="flex items-center gap-2 text-sm text-zinc-500">
                        <Loader2 className="h-4 w-4 animate-spin" />
                        加载详情…
                      </div>
                    ) : null}
                    {dErr ? (
                      <p className="text-sm text-red-600 dark:text-red-400">{dErr}</p>
                    ) : null}

                    {detail ? (
                      <>
                        <div className="mb-4 flex flex-wrap gap-2">
                          {task.status === "pending" ? (
                            <button
                              type="button"
                              onClick={() => handleExecute(task.id)}
                              className="inline-flex items-center gap-1.5 rounded-lg bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-700"
                            >
                              <Play className="h-4 w-4" />
                              开始执行
                            </button>
                          ) : null}
                          {task.status === "running" && runningStep ? (
                            <button
                              type="button"
                              onClick={() =>
                                handleCompleteStep(task.id, runningStep.step_order)
                              }
                              className="inline-flex items-center gap-1.5 rounded-lg bg-blue-600 px-3 py-2 text-sm font-medium text-white hover:bg-blue-700"
                            >
                              <CheckCircle2 className="h-4 w-4" />
                              完成当前步骤
                            </button>
                          ) : null}
                        </div>

                        {hasArtifacts ? (
                          <div className="mb-6 rounded-lg border border-zinc-200 dark:border-zinc-700">
                            <button
                              type="button"
                              onClick={() => toggleArtifacts(task.id)}
                              className="flex w-full items-center justify-between px-4 py-3 text-left text-sm font-medium text-zinc-800 dark:text-zinc-200"
                            >
                              生成产物
                              {artifactsOpen[task.id] ? (
                                <ChevronUp className="h-4 w-4" />
                              ) : (
                                <ChevronDown className="h-4 w-4" />
                              )}
                            </button>
                            {artifactsOpen[task.id] ? (
                              <div className="space-y-4 border-t border-zinc-100 px-4 py-4 dark:border-zinc-800">
                                {snap.hbase_cmd ? (
                                  <div>
                                    <p className="mb-1 text-xs font-medium text-zinc-500">
                                      HBase 命令
                                    </p>
                                    <pre className="max-h-40 overflow-auto rounded-md bg-zinc-950 p-3 text-xs text-zinc-100">
                                      {snap.hbase_cmd}
                                    </pre>
                                  </div>
                                ) : null}
                                {snap.flink_config ? (
                                  <div>
                                    <p className="mb-1 text-xs font-medium text-zinc-500">
                                      Flink 配置
                                    </p>
                                    <pre className="max-h-48 overflow-auto rounded-md bg-zinc-950 p-3 text-xs text-zinc-100">
                                      {typeof snap.flink_config === "string"
                                        ? snap.flink_config
                                        : JSON.stringify(snap.flink_config, null, 2)}
                                    </pre>
                                  </div>
                                ) : null}
                                {snap.bulkload_cmd ? (
                                  <div>
                                    <p className="mb-1 text-xs font-medium text-zinc-500">
                                      BulkLoad 命令
                                    </p>
                                    <pre className="max-h-40 overflow-auto rounded-md bg-zinc-950 p-3 text-xs text-zinc-100">
                                      {snap.bulkload_cmd}
                                    </pre>
                                  </div>
                                ) : null}
                              </div>
                            ) : null}
                          </div>
                        ) : null}

                        <h3 className="mb-4 text-base font-semibold text-zinc-900 dark:text-zinc-50">
                          流程步骤
                        </h3>
                        <ol className="relative ml-1 border-l-2 border-zinc-200 pl-11 dark:border-zinc-700">
                          {steps.map((step) => (
                            <li
                              key={step.id ?? step.step_order}
                              className="relative pb-6 last:pb-0"
                            >
                              <span className="absolute -left-[1.15rem] top-1 flex h-8 w-8 items-center justify-center rounded-full border-2 border-white bg-zinc-100 text-xs font-bold text-zinc-700 shadow-sm dark:border-zinc-900 dark:bg-zinc-800 dark:text-zinc-200">
                                {step.step_order}
                              </span>
                              <div className="rounded-lg border border-zinc-200/80 bg-zinc-50/60 px-3 py-2.5 dark:border-zinc-700 dark:bg-zinc-800/40">
                                <div className="flex flex-wrap items-start justify-between gap-3">
                                  <div className="min-w-0 flex-1">
                                    <p className="font-medium leading-6 text-zinc-900 dark:text-zinc-50">
                                    {step.name}
                                    </p>
                                    <p className="mt-1 text-sm leading-6 text-zinc-600 dark:text-zinc-300">
                                    {step.description}
                                    </p>
                                  </div>
                                  <div className="flex items-center gap-2">
                                    <AutomationBadge automation={step.automation} />
                                    <StepStatusIcon status={step.status} />
                                  </div>
                                </div>
                                {Array.isArray(step.sub_steps) && step.sub_steps.length > 0 ? (
                                  <ul className="mt-3 list-disc space-y-1.5 pl-5 text-sm leading-6 text-zinc-600 dark:text-zinc-300">
                                    {step.sub_steps.map((sub, i) => (
                                      <li key={i}>{sub}</li>
                                    ))}
                                  </ul>
                                ) : null}
                              </div>
                            </li>
                          ))}
                        </ol>
                      </>
                    ) : null}
                  </div>
                ) : null}
              </article>
            );
          })
        )}
      </div>

      {modalOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
          <button
            type="button"
            className="absolute inset-0 bg-zinc-900/60 backdrop-blur-sm"
            aria-label="关闭"
            onClick={() => !submitting && setModalOpen(false)}
          />
          <div className="relative z-10 w-full max-w-md rounded-xl border border-zinc-200 bg-white p-6 shadow-xl dark:border-zinc-700 dark:bg-zinc-900">
            <h2 className="text-lg font-semibold text-zinc-900 dark:text-zinc-50">
              新建采集任务
            </h2>
            <form onSubmit={handleCreate} className="mt-4 space-y-4">
              <div>
                <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300">
                  任务名称
                </label>
                <input
                  required
                  type="text"
                  value={form.name}
                  onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
                  className="mt-1 w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400 dark:border-zinc-600 dark:bg-zinc-800 dark:text-white dark:placeholder:text-zinc-400"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300">
                  目标表名
                </label>
                <input
                  required
                  type="text"
                  value={form.table_name}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, table_name: e.target.value }))
                  }
                  className="mt-1 w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400 dark:border-zinc-600 dark:bg-zinc-800 dark:text-white dark:placeholder:text-zinc-400"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300">
                  任务类型
                </label>
                <select
                  value={form.task_type}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, task_type: e.target.value }))
                  }
                  className="mt-1 w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-slate-900 dark:border-zinc-600 dark:bg-zinc-800 dark:text-white"
                >
                  <option value="full+incremental">全量 + 增量</option>
                  <option value="incremental-only">仅增量</option>
                </select>
              </div>
              <div>
                <label className="block text-sm font-medium text-zinc-700 dark:text-zinc-300">
                  文件大小 (GB)
                </label>
                <input
                  required
                  type="number"
                  min={0.01}
                  step={0.1}
                  value={form.file_size_gb}
                  onChange={(e) =>
                    setForm((f) => ({ ...f, file_size_gb: e.target.value }))
                  }
                  className="mt-1 w-full rounded-lg border border-zinc-300 bg-white px-3 py-2 text-sm text-slate-900 dark:border-zinc-600 dark:bg-zinc-800 dark:text-white"
                />
              </div>
              {formError ? (
                <p className="text-sm text-red-600 dark:text-red-400">{formError}</p>
              ) : null}
              <div className="flex justify-end gap-2 pt-2">
                <button
                  type="button"
                  disabled={submitting}
                  onClick={() => setModalOpen(false)}
                  className="rounded-lg border border-zinc-300 px-4 py-2 text-sm font-medium text-zinc-700 hover:bg-zinc-50 disabled:opacity-50 dark:border-zinc-600 dark:text-zinc-300 dark:hover:bg-zinc-800"
                >
                  取消
                </button>
                <button
                  type="submit"
                  disabled={submitting}
                  className="inline-flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
                >
                  {submitting ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : null}
                  提交
                </button>
              </div>
            </form>
          </div>
        </div>
      ) : null}
    </div>
  );
}
