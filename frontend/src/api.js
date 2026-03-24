const BASE = "/api";

async function request(path, opts = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...opts.headers },
    ...opts,
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json();
}

export const api = {
  dashboard: () => request("/dashboard"),
  flows: {
    list: () => request("/flows"),
    get: (id) => request(`/flows/${id}`),
    create: (data) => request("/flows", { method: "POST", body: JSON.stringify(data) }),
    execute: (id) => request(`/flows/${id}/execute`, { method: "POST" }),
    completeStep: (taskId, step) =>
      request(`/flows/${taskId}/step/${step}/complete`, { method: "POST" }),
  },
  params: {
    list: () => request("/params"),
    get: (id) => request(`/params/${id}`),
    create: (data) => request("/params", { method: "POST", body: JSON.stringify(data) }),
    validate: (params) => request("/params/validate", { method: "POST", body: JSON.stringify({ params }) }),
    recommend: (table_name) =>
      request("/params/recommend", { method: "POST", body: JSON.stringify({ table_name }) }),
  },
  monitor: {
    events: (severity) => request(`/monitor/events${severity ? `?severity=${severity}` : ""}`),
    metrics: (source) => request(`/monitor/metrics${source ? `?source=${source}` : ""}`),
    resolve: (id) => request(`/monitor/events/${id}/resolve`, { method: "POST" }),
    detectAnomaly: (metrics) =>
      request("/monitor/anomaly-detect", { method: "POST", body: JSON.stringify({ metrics }) }),
  },
  knowledge: {
    list: (query) => {
      const sp = new URLSearchParams();
      if (typeof query === "string") {
        if (query.trim()) sp.set("q", query.trim());
      } else if (query && typeof query === "object") {
        if (query.q?.trim()) sp.set("q", query.q.trim());
        if (query.category) sp.set("category", query.category);
      }
      const qs = sp.toString();
      return request(`/knowledge${qs ? `?${qs}` : ""}`);
    },
    get: (id) => request(`/knowledge/${id}`),
    create: (data) => request("/knowledge", { method: "POST", body: JSON.stringify(data) }),
    helpful: (id) => request(`/knowledge/${id}/helpful`, { method: "POST" }),
  },
};
