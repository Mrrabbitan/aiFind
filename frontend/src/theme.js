export const THEME_KEY = "theme";
export const THEME_VALUES = ["light", "dark", "system"];

function getStoredTheme() {
  const v = localStorage.getItem(THEME_KEY);
  return THEME_VALUES.includes(v) ? v : "system";
}

function getSystemIsDark() {
  return window.matchMedia("(prefers-color-scheme: dark)").matches;
}

export function applyTheme(themeValue) {
  const value = THEME_VALUES.includes(themeValue) ? themeValue : "system";
  const useDark = value === "dark" || (value === "system" && getSystemIsDark());
  document.documentElement.classList.toggle("dark", useDark);
  document.documentElement.setAttribute("data-theme", value);
}

export function initTheme() {
  applyTheme(getStoredTheme());
}

export function setTheme(value) {
  const next = THEME_VALUES.includes(value) ? value : "system";
  localStorage.setItem(THEME_KEY, next);
  applyTheme(next);
}

export function getTheme() {
  return getStoredTheme();
}
