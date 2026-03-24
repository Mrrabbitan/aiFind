import React, { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "../api";
import {
  Search,
  Loader2,
  ThumbsUp,
  Plus,
  BookOpen,
  Eye,
  X,
  FileText,
  Shield,
  Wrench,
  Bug,
  Sparkles,
} from "lucide-react";

const CATEGORIES = [
  { id: "", label: "全部", Icon: BookOpen },
  { id: "best-practice", label: "最佳实践", Icon: Sparkles },
  { id: "operation", label: "操作指南", Icon: Wrench },
  { id: "reference", label: "参考文档", Icon: FileText },
  { id: "troubleshooting", label: "故障排查", Icon: Bug },
];

const CATEGORY_BADGE = {
  "best-practice": "bg-violet-100 text-violet-800 ring-violet-200",
  operation: "bg-sky-100 text-sky-800 ring-sky-200",
  reference: "bg-slate-100 text-slate-800 ring-slate-200",
  troubleshooting: "bg-red-100 text-red-800 ring-red-200",
};

function categoryLabel(id) {
  const c = CATEGORIES.find((x) => x.id === id);
  return c?.label ?? id;
}

function splitTags(tags) {
  if (!tags || typeof tags !== "string") return [];
  return tags
    .split(/[,，]/)
    .map((t) => t.trim())
    .filter(Boolean);
}

/** 简易内联：`**粗体**` 与 `` `代码` `` */
function formatInline(text) {
  if (!text) return null;
  const parts = [];
  let i = 0;
  const push = (s, key) => {
    if (s) parts.push(<span key={key}>{s}</span>);
  };
  while (i < text.length) {
    const bold = text.indexOf("**", i);
    const code = text.indexOf("`", i);
    let next = -1;
    let mode = null;
    if (bold >= 0 && (code < 0 || bold <= code)) {
      next = bold;
      mode = "bold";
    } else if (code >= 0) {
      next = code;
      mode = "code";
    }
    if (next < 0) {
      push(text.slice(i), `t-${i}`);
      break;
    }
    push(text.slice(i, next), `t-${i}`);
    if (mode === "bold") {
      const end = text.indexOf("**", next + 2);
      if (end < 0) {
        push(text.slice(next), `rest-${next}`);
        break;
      }
      parts.push(
        <strong key={`b-${next}`} className="font-semibold text-slate-900">
          {text.slice(next + 2, end)}
        </strong>,
      );
      i = end + 2;
    } else {
      const end = text.indexOf("`", next + 1);
      if (end < 0) {
        push(text.slice(next), `restc-${next}`);
        break;
      }
      parts.push(
        <code
          key={`c-${next}`}
          className="rounded bg-slate-100 px-1 py-0.5 font-mono text-[0.85em] text-rose-700"
        >
          {text.slice(next + 1, end)}
        </code>,
      );
      i = end + 1;
    }
  }
  return parts;
}

function MarkdownBasic({ content }) {
  if (!content) return null;
  const blocks = content.split(/\n\n+/);
  return (
    <div className="space-y-3 text-sm leading-relaxed text-slate-700">
      {blocks.map((block, bi) => {
        const b = block.trim();
        if (!b) return null;
        if (b.startsWith("## ")) {
          return (
            <h3 key={bi} className="text-base font-semibold text-slate-900">
              {b.slice(3).trim()}
            </h3>
          );
        }
        if (b.startsWith("```")) {
          const lines = b.split("\n");
          const inner = lines.slice(1, lines[lines.length - 1] === "```" ? -1 : undefined).join("\n");
          return (
            <pre
              key={bi}
              className="overflow-x-auto rounded-lg bg-slate-900 p-3 font-mono text-xs text-slate-100"
            >
              {inner}
            </pre>
          );
        }
        if (b.startsWith("|")) {
          return (
            <pre
              key={bi}
              className="overflow-x-auto rounded-lg border border-slate-200 bg-slate-50 p-3 font-mono text-xs text-slate-800"
            >
              {b}
            </pre>
          );
        }
        if (b.startsWith("- ") || b.startsWith("* ")) {
          const items = b.split("\n").filter((line) => /^[-*]\s/.test(line));
          return (
            <ul key={bi} className="list-inside list-disc space-y-1 text-slate-700">
              {items.map((line, li) => (
                <li key={li}>{formatInline(line.replace(/^[-*]\s+/, ""))}</li>
              ))}
            </ul>
          );
        }
        return (
          <p key={bi} className="whitespace-pre-wrap">
            {formatInline(b)}
          </p>
        );
      })}
    </div>
  );
}

function Modal({ open, title, onClose, children }) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
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
            className="rounded-lg p-1 text-slate-500 hover:bg-slate-100"
          >
            <X className="h-5 w-5" />
          </button>
        </div>
        <div className="max-h-[min(75vh,620px)] overflow-y-auto p-5">{children}</div>
      </div>
    </div>
  );
}

export default function Knowledge() {
  const [query, setQuery] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [category, setCategory] = useState("");
  const [articles, setArticles] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedId, setExpandedId] = useState(null);
  const [helpfulBusy, setHelpfulBusy] = useState(null);

  const [createOpen, setCreateOpen] = useState(false);
  const [createLoading, setCreateLoading] = useState(false);
  const [createForm, setCreateForm] = useState({
    title: "",
    category: "operation",
    tags: "",
    content: "",
  });
  const [createError, setCreateError] = useState(null);

  useEffect(() => {
    const t = setTimeout(() => setDebouncedQ(query), 320);
    return () => clearTimeout(t);
  }, [query]);

  const listParams = useMemo(
    () => ({
      q: debouncedQ,
      ...(category ? { category } : {}),
    }),
    [debouncedQ, category],
  );

  const loadList = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await api.knowledge.list(listParams);
      setArticles(Array.isArray(data) ? data : []);
    } catch (e) {
      setError(e.message || "加载失败");
      setArticles([]);
    } finally {
      setLoading(false);
    }
  }, [listParams]);

  useEffect(() => {
    loadList();
  }, [loadList]);

  const toggleExpand = (id) => {
    setExpandedId((prev) => (prev === id ? null : id));
  };

  const markHelpful = async (id, e) => {
    e.stopPropagation();
    setHelpfulBusy(id);
    try {
      await api.knowledge.helpful(id);
      setArticles((prev) =>
        prev.map((a) => (a.id === id ? { ...a, helpful: (a.helpful || 0) + 1 } : a)),
      );
    } catch (err) {
      setError(err.message || "操作失败");
    } finally {
      setHelpfulBusy(null);
    }
  };

  const submitCreate = async (e) => {
    e.preventDefault();
    setCreateLoading(true);
    setCreateError(null);
    try {
      await api.knowledge.create({
        title: createForm.title.trim(),
        category: createForm.category,
        tags: createForm.tags.trim(),
        content: createForm.content,
      });
      setCreateOpen(false);
      setCreateForm({ title: "", category: "operation", tags: "", content: "" });
      await loadList();
    } catch (err) {
      setCreateError(err.message || "创建失败");
    } finally {
      setCreateLoading(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
        <div className="relative max-w-xl flex-1">
          <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
          <input
            type="search"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="搜索知识库..."
            className="w-full rounded-xl border border-slate-200 bg-white py-2.5 pl-10 pr-4 text-sm outline-none ring-indigo-500 focus:border-indigo-500 focus:ring-2"
          />
        </div>
        <button
          type="button"
          onClick={() => {
            setCreateOpen(true);
            setCreateError(null);
          }}
          className="inline-flex items-center justify-center gap-2 self-start rounded-xl bg-indigo-600 px-4 py-2.5 text-sm font-medium text-white shadow-sm hover:bg-indigo-700"
        >
          <Plus className="h-4 w-4" />
          新建知识
        </button>
      </div>

      <div className="flex flex-wrap gap-2 border-b border-slate-200 pb-1">
        {CATEGORIES.map(({ id, label, Icon }) => (
          <button
            key={id || "all"}
            type="button"
            onClick={() => setCategory(id)}
            className={[
              "inline-flex items-center gap-1.5 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
              category === id
                ? "bg-slate-900 text-white"
                : "text-slate-600 hover:bg-slate-100",
            ].join(" ")}
          >
            <Icon className="h-4 w-4 opacity-80" />
            {label}
          </button>
        ))}
      </div>

      <Modal
        open={createOpen}
        title="新建知识"
        onClose={() => !createLoading && setCreateOpen(false)}
      >
        <form onSubmit={submitCreate} className="space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-700">标题</label>
            <input
              required
              value={createForm.title}
              onChange={(e) => setCreateForm((f) => ({ ...f, title: e.target.value }))}
              className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none ring-indigo-500 focus:border-indigo-500 focus:ring-2"
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-700">分类</label>
            <select
              value={createForm.category}
              onChange={(e) => setCreateForm((f) => ({ ...f, category: e.target.value }))}
              className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none ring-indigo-500 focus:border-indigo-500 focus:ring-2"
            >
              {CATEGORIES.filter((c) => c.id).map((c) => (
                <option key={c.id} value={c.id}>
                  {c.label}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-700">标签</label>
            <input
              value={createForm.tags}
              onChange={(e) => setCreateForm((f) => ({ ...f, tags: e.target.value }))}
              placeholder="逗号分隔，如 Flink, 配置"
              className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none ring-indigo-500 focus:border-indigo-500 focus:ring-2"
            />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-slate-700">正文</label>
            <textarea
              required
              rows={8}
              value={createForm.content}
              onChange={(e) => setCreateForm((f) => ({ ...f, content: e.target.value }))}
              className="w-full resize-y rounded-lg border border-slate-200 px-3 py-2 font-mono text-xs leading-relaxed outline-none ring-indigo-500 focus:border-indigo-500 focus:ring-2"
            />
          </div>
          {createError && <p className="text-sm text-red-600">{createError}</p>}
          <button
            type="submit"
            disabled={createLoading}
            className="inline-flex w-full items-center justify-center gap-2 rounded-lg bg-indigo-600 py-2.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-60"
          >
            {createLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <BookOpen className="h-4 w-4" />}
            保存
          </button>
        </form>
      </Modal>

      {loading && (
        <div className="flex min-h-[200px] items-center justify-center rounded-xl border border-slate-200 bg-white">
          <Loader2 className="h-9 w-9 animate-spin text-indigo-600" />
        </div>
      )}
      {error && !loading && (
        <p className="rounded-lg border border-red-100 bg-red-50 px-4 py-3 text-sm text-red-700">
          {error}
        </p>
      )}
      {!loading && !error && articles.length === 0 && (
        <p className="text-center text-sm text-slate-500">暂无文章</p>
      )}

      <div className="grid gap-4 md:grid-cols-2">
        {!loading &&
          articles.map((a) => {
            const expanded = expandedId === a.id;
            const cat = a.category || "";
            const badgeCls =
              CATEGORY_BADGE[cat] ?? "bg-slate-100 text-slate-700 ring-slate-200";
            const preview = (a.content || "").slice(0, 150);
            const tags = splitTags(a.tags);
            return (
              <article
                key={a.id}
                role="button"
                tabIndex={0}
                onClick={() => toggleExpand(a.id)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    toggleExpand(a.id);
                  }
                }}
                className={[
                  "flex flex-col rounded-xl border bg-white p-4 text-left shadow-sm transition-all outline-none ring-indigo-500 hover:shadow-md focus-visible:ring-2",
                  expanded ? "border-indigo-200 ring-1 ring-indigo-100" : "border-slate-200",
                ].join(" ")}
              >
                <div className="mb-2 flex flex-wrap items-center gap-2">
                  <span
                    className={`inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${badgeCls}`}
                  >
                    <Shield className="mr-1 h-3 w-3 opacity-70" />
                    {categoryLabel(cat)}
                  </span>
                </div>
                <h3 className="text-base font-semibold text-slate-900">{a.title}</h3>
                <div className="mt-2 flex flex-wrap gap-1">
                  {tags.map((t) => (
                    <span
                      key={t}
                      className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600"
                    >
                      {t}
                    </span>
                  ))}
                </div>
                {!expanded && (
                  <p className="mt-3 line-clamp-3 text-sm text-slate-600">{preview}</p>
                )}
                {expanded && (
                  <div className="mt-3 border-t border-slate-100 pt-3">
                    <MarkdownBasic content={a.content} />
                  </div>
                )}
                <div className="mt-4 flex flex-wrap items-center justify-between gap-2 border-t border-slate-50 pt-3">
                  <div className="flex items-center gap-3 text-xs text-slate-500">
                    <span className="inline-flex items-center gap-1">
                      <Eye className="h-3.5 w-3.5" />
                      {a.views ?? 0}
                    </span>
                    <span className="inline-flex items-center gap-1">
                      <ThumbsUp className="h-3.5 w-3.5" />
                      {a.helpful ?? 0}
                    </span>
                  </div>
                  <button
                    type="button"
                    onClick={(e) => markHelpful(a.id, e)}
                    disabled={helpfulBusy === a.id}
                    className="inline-flex items-center gap-1.5 rounded-lg border border-slate-200 bg-slate-50 px-3 py-1.5 text-xs font-medium text-slate-800 hover:bg-slate-100 disabled:opacity-50"
                  >
                    {helpfulBusy === a.id ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : (
                      <ThumbsUp className="h-3.5 w-3.5" />
                    )}
                    有帮助
                  </button>
                </div>
              </article>
            );
          })}
      </div>
    </div>
  );
}
