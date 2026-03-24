import React, { useCallback, useEffect, useState } from "react";
import { api } from "../api";
import {
  Sparkles,
  ShieldCheck,
  Loader2,
  ChevronDown,
  ChevronRight,
  X,
  CheckCircle2,
  XCircle,
  AlertTriangle,
} from "lucide-react";

const CATEGORY_STYLES = {
  flink: "bg-sky-100 text-sky-800 ring-sky-200",
  bulkload: "bg-amber-100 text-amber-900 ring-amber-200",
  export: "bg-emerald-100 text-emerald-800 ring-emerald-200",
};

function Modal({ open, title, onClose, children }) {
  if (!open) return null;
  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
    >
      <button
        type="button"
        className="absolute inset-0 bg-slate-900/40 backdrop-blur-[1px]"
        aria-label="关闭"
        onClick={onClose}
      />
      <div className="relative z-10 w-full max-w-lg rounded-xl border border-slate-200 bg-white shadow-xl">
        <div className="flex items-center justify-between border-b border-slate-100 px-5 py-4">
          <h2 className="text-base font-semibold text-slate-900">{title}</h2>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg p-1 text-slate-500 hover:bg-slate-100 hover:text-slate-800"
          >
            <X className="h-5 w-5" />
          </button>
        </div>
        <div className="max-h-[min(70vh,560px)] overflow-y-auto p-5">{children}</div>
      </div>
    </div>
  );
}

function CategoryBadge({ category }) {
  const c = (category || "").toLowerCase();
  const cls = CATEGORY_STYLES[c] ?? "bg-slate-100 text-slate-700 ring-slate-200";
  const label =
    c === "flink"
      ? "Flink"
      : c === "bulkload"
        ? "BulkLoad"
        : c === "export"
          ? "Export"
          : category || "—";
  return (
    <span
      className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${cls}`}
    >
      {label}
    </span>
  );
}

export default function ParamManager() {
  const [templates, setTemplates] = useState([]);
  const [listLoading, setListLoading] = useState(true);
  const [listError, setListError] = useState(null);
  const [expanded, setExpanded] = useState(() => new Set());

  const [recommendOpen, setRecommendOpen] = useState(false);
  const [tableName, setTableName] = useState("");
  const [recommendLoading, setRecommendLoading] = useState(false);
  const [recommendResult, setRecommendResult] = useState(null);
  const [recommendError, setRecommendError] = useState(null);

  const [validateOpen, setValidateOpen] = useState(false);
  const [paramsJson, setParamsJson] = useState('{\n  "hbasezk": "10.0.0.1,10.0.0.2",\n  "setParalizem": "100",\n  "sourceTopic": "my-topic"\n}');
  const [validateLoading, setValidateLoading] = useState(false);
  const [validateResults, setValidateResults] = useState(null);
  const [validateError, setValidateError] = useState(null);

  const loadTemplates = useCallback(async () => {
    setListLoading(true);
    setListError(null);
    try {
      const data = await api.params.list();
      setTemplates(Array.isArray(data) ? data : []);
    } catch (e) {
      setListError(e.message || "加载失败");
      setTemplates([]);
    } finally {
      setListLoading(false);
    }
  }, []);

  useEffect(() => {
    loadTemplates();
  }, [loadTemplates]);

  const toggleExpand = (id) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const submitRecommend = async (e) => {
    e.preventDefault();
    setRecommendLoading(true);
    setRecommendError(null);
    setRecommendResult(null);
    try {
      const rec = await api.params.recommend(tableName.trim());
      setRecommendResult(rec);
    } catch (err) {
      setRecommendError(err.message || "推荐失败");
    } finally {
      setRecommendLoading(false);
    }
  };

  const submitValidate = async (e) => {
    e.preventDefault();
    setValidateLoading(true);
    setValidateError(null);
    setValidateResults(null);
    try {
      const parsed = JSON.parse(paramsJson);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error("参数须为 JSON 对象");
      }
      const strMap = Object.fromEntries(
        Object.entries(parsed).map(([k, v]) => [k, String(v)]),
      );
      const res = await api.params.validate(strMap);
      setValidateResults(res?.results ?? []);
    } catch (err) {
      if (err instanceof SyntaxError) {
        setValidateError("JSON 格式无效");
      } else {
        setValidateError(err.message || "校验失败");
      }
    } finally {
      setValidateLoading(false);
    }
  };

  const confidencePct =
    recommendResult?.confidence != null
      ? Math.round(Number(recommendResult.confidence) * 100)
      : 0;

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <h2 className="text-xl font-semibold tracking-tight text-slate-900">
          参数管理
        </h2>
        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => {
              setRecommendOpen(true);
              setRecommendResult(null);
              setRecommendError(null);
            }}
            className="inline-flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-indigo-700"
          >
            <Sparkles className="h-4 w-4" />
            AI 参数推荐
          </button>
          <button
            type="button"
            onClick={() => {
              setValidateOpen(true);
              setValidateResults(null);
              setValidateError(null);
            }}
            className="inline-flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-800 shadow-sm hover:bg-slate-50"
          >
            <ShieldCheck className="h-4 w-4 text-indigo-600" />
            参数校验
          </button>
        </div>
      </div>

      <Modal
        open={recommendOpen}
        title="AI 参数推荐"
        onClose={() => !recommendLoading && setRecommendOpen(false)}
      >
        <form onSubmit={submitRecommend} className="space-y-4">
          <div>
            <label htmlFor="table_name" className="mb-1 block text-sm font-medium text-slate-700">
              表名 (table_name)
            </label>
            <input
              id="table_name"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              placeholder="例如 tf_oh_special_blacklist"
              className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none ring-indigo-500 focus:border-indigo-500 focus:ring-2"
              required
            />
          </div>
          <button
            type="submit"
            disabled={recommendLoading}
            className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-indigo-600 py-2.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-60"
          >
            {recommendLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Sparkles className="h-4 w-4" />
            )}
            获取推荐
          </button>
        </form>
        {recommendError && (
          <p className="mt-3 text-sm text-red-600">{recommendError}</p>
        )}
        {recommendResult && (
          <div className="mt-5 rounded-xl border border-indigo-100 bg-gradient-to-br from-indigo-50/80 to-white p-4 shadow-sm">
            <p className="text-xs font-medium uppercase tracking-wide text-indigo-600">
              推荐结果
            </p>
            <dl className="mt-3 grid gap-2 text-sm">
              <div className="flex justify-between gap-2">
                <dt className="text-slate-500">并行度</dt>
                <dd className="font-mono font-medium text-slate-900">
                  {recommendResult.recommended_parallelism}
                </dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-slate-500">Topic</dt>
                <dd className="break-all font-mono text-slate-900">
                  {recommendResult.recommended_topic}
                </dd>
              </div>
              <div className="flex justify-between gap-2">
                <dt className="text-slate-500">Group ID</dt>
                <dd className="break-all font-mono text-sm text-slate-900">
                  {recommendResult.recommended_group_id}
                </dd>
              </div>
            </dl>
            <div className="mt-4">
              <div className="mb-1 flex justify-between text-xs text-slate-600">
                <span>置信度</span>
                <span className="font-semibold text-indigo-700">{confidencePct}%</span>
              </div>
              <div className="h-2 overflow-hidden rounded-full bg-indigo-100">
                <div
                  className="h-full rounded-full bg-indigo-600 transition-all duration-500"
                  style={{ width: `${confidencePct}%` }}
                />
              </div>
            </div>
            {recommendResult.basis && (
              <p className="mt-3 text-xs leading-relaxed text-slate-600">
                {recommendResult.basis}
              </p>
            )}
          </div>
        )}
      </Modal>

      <Modal
        open={validateOpen}
        title="参数校验"
        onClose={() => !validateLoading && setValidateOpen(false)}
      >
        <form onSubmit={submitValidate} className="space-y-4">
          <div>
            <label htmlFor="params_json" className="mb-1 block text-sm font-medium text-slate-700">
              参数 JSON
            </label>
            <textarea
              id="params_json"
              value={paramsJson}
              onChange={(e) => setParamsJson(e.target.value)}
              rows={10}
              className="w-full resize-y rounded-lg border border-slate-200 px-3 py-2 font-mono text-xs leading-relaxed outline-none ring-indigo-500 focus:border-indigo-500 focus:ring-2"
            />
          </div>
          <button
            type="submit"
            disabled={validateLoading}
            className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-slate-900 py-2.5 text-sm font-medium text-white hover:bg-slate-800 disabled:opacity-60"
          >
            {validateLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <ShieldCheck className="h-4 w-4" />
            )}
            执行校验
          </button>
        </form>
        {validateError && (
          <p className="mt-3 text-sm text-red-600">{validateError}</p>
        )}
        {validateResults && validateResults.length > 0 && (
          <ul className="mt-4 space-y-2 border-t border-slate-100 pt-4">
            {validateResults.map((row, idx) => (
              <li
                key={idx}
                className="flex gap-3 rounded-lg border border-slate-100 bg-slate-50/80 px-3 py-2 text-sm"
              >
                {row.level === "success" && (
                  <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-emerald-600" aria-hidden />
                )}
                {row.level === "error" && (
                  <XCircle className="mt-0.5 h-5 w-5 shrink-0 text-red-600" aria-hidden />
                )}
                {row.level === "warning" && (
                  <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-amber-500" aria-hidden />
                )}
                <div className="min-w-0 flex-1">
                  {row.field ? (
                    <span className="font-mono text-xs text-slate-500">{row.field}: </span>
                  ) : null}
                  <span className="text-slate-800">{row.message}</span>
                </div>
              </li>
            ))}
          </ul>
        )}
      </Modal>

      <section className="rounded-xl border border-slate-200 bg-white p-5 shadow-sm">
        <h3 className="mb-4 text-sm font-semibold text-slate-800">参数模板</h3>
        {listLoading && (
          <div className="flex min-h-[160px] items-center justify-center">
            <Loader2 className="h-8 w-8 animate-spin text-indigo-600" aria-hidden />
          </div>
        )}
        {listError && !listLoading && (
          <p className="text-sm text-red-600">{listError}</p>
        )}
        {!listLoading && !listError && templates.length === 0 && (
          <p className="text-sm text-slate-500">暂无模板</p>
        )}
        <div className="grid gap-4 sm:grid-cols-2">
          {!listLoading &&
            templates.map((t) => {
              const isOpen = expanded.has(t.id);
              const entries = Object.entries(t.params || {});
              return (
                <article
                  key={t.id}
                  className="overflow-hidden rounded-xl border border-slate-200 bg-slate-50/40 shadow-sm transition-shadow hover:shadow-md"
                >
                  <button
                    type="button"
                    onClick={() => toggleExpand(t.id)}
                    className="flex w-full items-start gap-2 p-4 text-left"
                  >
                    {isOpen ? (
                      <ChevronDown className="mt-0.5 h-5 w-5 shrink-0 text-slate-400" />
                    ) : (
                      <ChevronRight className="mt-0.5 h-5 w-5 shrink-0 text-slate-400" />
                    )}
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <h4 className="font-semibold text-slate-900">{t.name}</h4>
                        <CategoryBadge category={t.category} />
                      </div>
                      <p className="mt-1 font-mono text-xs text-slate-600">
                        {t.table_pattern}
                      </p>
                      {t.description && (
                        <p className="mt-2 text-sm text-slate-600">{t.description}</p>
                      )}
                    </div>
                  </button>
                  {isOpen && (
                    <div className="border-t border-slate-200 bg-white px-4 pb-4 pt-2">
                      <p className="mb-2 text-xs font-medium text-slate-500">参数键值</p>
                      <div className="max-h-64 overflow-auto rounded-lg border border-slate-100">
                        <table className="w-full text-left text-xs">
                          <thead className="sticky top-0 bg-slate-100">
                            <tr>
                              <th className="px-3 py-2 font-medium text-slate-700">Key</th>
                              <th className="px-3 py-2 font-medium text-slate-700">Value</th>
                            </tr>
                          </thead>
                          <tbody>
                            {entries.length === 0 ? (
                              <tr>
                                <td colSpan={2} className="px-3 py-3 text-slate-500">
                                  无参数
                                </td>
                              </tr>
                            ) : (
                              entries.map(([k, v]) => (
                                <tr key={k} className="border-t border-slate-100">
                                  <td className="px-3 py-1.5 font-mono text-slate-800">{k}</td>
                                  <td className="break-all px-3 py-1.5 font-mono text-slate-600">
                                    {typeof v === "object" ? JSON.stringify(v) : String(v)}
                                  </td>
                                </tr>
                              ))
                            )}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}
                </article>
              );
            })}
        </div>
      </section>
    </div>
  );
}
