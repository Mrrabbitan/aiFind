import { NavLink, Outlet, useLocation } from "react-router-dom";
import {
  LayoutDashboard,
  GitBranch,
  Settings,
  Activity,
  BookOpen,
  Library,
  Zap,
} from "lucide-react";
import Copilot from "./Copilot";

const navItems = [
  { to: "/", end: true, label: "工作台", Icon: LayoutDashboard },
  { to: "/flows", label: "流程管理", Icon: GitBranch },
  { to: "/params", label: "参数管理", Icon: Settings },
  { to: "/monitor", label: "监控中心", Icon: Activity },
  { to: "/knowledge", label: "知识库", Icon: BookOpen },
  { to: "/manual", label: "操作手册", Icon: Library },
];

const PAGE_TITLES = {
  "/": "工作台",
  "/flows": "流程管理",
  "/params": "参数管理",
  "/monitor": "监控中心",
  "/knowledge": "知识库",
  "/manual": "操作手册",
};

export default function Layout() {
  const { pathname } = useLocation();
  const pageTitle = PAGE_TITLES[pathname] ?? "智能订单采集运营平台";

  return (
    <div className="flex min-h-screen bg-slate-50">
      <aside className="flex w-60 shrink-0 flex-col bg-slate-900 text-white">
        <div className="flex items-center gap-2 border-b border-slate-800 px-4 py-5">
          <Zap className="h-8 w-8 shrink-0 text-indigo-400" aria-hidden />
          <span className="text-sm font-semibold leading-tight">
            智能订单采集运营平台
          </span>
        </div>
        <nav className="flex flex-1 flex-col gap-1 p-3">
          {navItems.map(({ to, end, label, Icon }) => (
            <NavLink
              key={to}
              to={to}
              end={end}
              className={({ isActive }) =>
                [
                  "flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors",
                  isActive
                    ? "bg-indigo-600 text-white shadow-sm"
                    : "text-slate-300 hover:bg-slate-800 hover:text-white",
                ].join(" ")
              }
            >
              <Icon className="h-5 w-5 shrink-0" aria-hidden />
              {label}
            </NavLink>
          ))}
        </nav>
      </aside>

      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <header className="shrink-0 border-b border-slate-200 bg-white px-6 py-4 shadow-sm">
          <h1 className="text-lg font-semibold tracking-tight text-slate-900">
            {pageTitle}
          </h1>
        </header>
        <main className="min-h-0 flex-1 overflow-auto p-6">
          <Outlet />
        </main>
      </div>
      <Copilot />
    </div>
  );
}
