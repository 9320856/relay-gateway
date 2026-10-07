import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const source = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), "assets", "app.js"), "utf8");

class Classes {
  values = new Set();
  add(...names) { names.forEach((name) => this.values.add(name)); }
  remove(...names) { names.forEach((name) => this.values.delete(name)); }
  contains(name) { return this.values.has(name); }
  toggle(name, force = !this.contains(name)) {
    if (force) this.add(name); else this.remove(name);
    return force;
  }
}

class Element {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.dataset = {};
    this.attributes = new Map();
    this.listeners = new Map();
    this.classList = new Classes();
    this.value = "";
    this.textContent = "";
    this.style = {};
  }
  set className(value) { this.classList = new Classes(); this.classList.add(...value.split(/\s+/).filter(Boolean)); }
  get className() { return [...this.classList.values].join(" "); }
  get firstElementChild() { return this.children[0] || null; }
  get options() { return this.children.filter((child) => child.tagName === "OPTION"); }
  append(...children) { children.forEach((child) => { child.parentElement = this; this.children.push(child); }); }
  replaceChildren(...children) {
    this.children = [];
    if (this.tagName === "SELECT") this.value = "";
    this.append(...children);
  }
  setAttribute(name, value) { this.attributes.set(name, String(value)); }
  getAttribute(name) { return this.attributes.get(name) ?? null; }
  removeAttribute(name) { this.attributes.delete(name); }
  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }
  dispatchEvent(event) {
    event.currentTarget = this;
    event.target ||= this;
    for (const listener of this.listeners.get(event.type) || []) listener(event);
  }
  fire(type, values = {}) { this.dispatchEvent({ type, preventDefault() {}, stopPropagation() {}, ...values }); }
  contains(element) { return element === this || this.children.some((child) => child.contains(element)); }
  matches(selector) {
    if (selector === "[data-dashboard-panel]") return this.dataset.dashboardPanel !== undefined;
    if (selector === "[data-playground-options]") return this.dataset.playgroundOptions !== undefined;
    if (selector === '[data-dashboard-view="settings"]') return this.dataset.dashboardView === "settings";
    if (selector.startsWith(".")) return selector.slice(1).split(".").every((name) => this.classList.contains(name));
    return this.tagName === selector.toUpperCase();
  }
  querySelectorAll(selector) {
    const selectors = selector.split(",").map((value) => value.trim());
    const found = [];
    for (const child of this.children) {
      if (selectors.some((value) => child.matches(value))) found.push(child);
      found.push(...child.querySelectorAll(selector));
    }
    return found;
  }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  getBoundingClientRect() { return { top: 300, bottom: 340 }; }
  scrollIntoView() {}
  focus() { this.focused = true; }
  showModal() { this.open = true; }
  close() { this.open = false; this.fire("close"); }
}

function harness(page = "dashboard", hash = "") {
  const document = new Element("document");
  document.body = new Element("body");
  document.body.dataset.page = page;
  document.append(document.body);
  document.elements = new Map();
  document.createElement = (tag) => new Element(tag);
  document.getElementById = (id) => document.elements.get(id) || null;
  const window = new Element("window");
  window.location = { origin: "http://gateway.test:8080", href: "http://gateway.test:8080/dashboard", hash };
  window.history = { state: null, replaceState(_state, _title, hash) { window.location.hash = hash; } };
  window.innerHeight = 900;
  const requests = [];
  const context = vm.createContext({
    document, window, location: window.location, console, URL, URLSearchParams,
    Event: class { constructor(type) { this.type = type; } },
    FormData: class { entries() { return []; } },
    navigator: {},
    fetch: async (url) => { requests.push(url); throw new Error("unexpected request"); },
    setTimeout: () => 1,
    clearTimeout() {},
  });
  vm.runInContext(source, context, { filename: "app.js" });
  const add = (id, tag = "div", parent = document.body) => {
    const element = new Element(tag);
    element.id = id;
    document.elements.set(id, element);
    parent.append(element);
    return element;
  };
  return { context, document, window, requests, add, run: (code) => vm.runInContext(code, context) };
}

test("dashboard hash navigation supports direct links and back without recreating forms or fetching", () => {
  const h = harness("dashboard", "#channels");
  const panels = {};
  const nav = {};
  for (const view of ["overview", "channels", "playground"]) {
    panels[view] = h.add(`panel-${view}`);
    panels[view].dataset.dashboardPanel = view;
    nav[view] = h.add(`nav-${view}`, "a");
    nav[view].className = "nav-link";
    nav[view].dataset.dashboardView = view;
    nav[view].setAttribute("href", `/dashboard${view === "overview" ? "" : `#${view}`}`);
  }
  for (const id of ["workspace-title", "workspace-description", "workspace-breadcrumb"]) h.add(id);
  const settingsLink = h.add("nav-settings", "a");
  settingsLink.className = "nav-link";
  settingsLink.dataset.dashboardView = "settings";
  settingsLink.setAttribute("href", "/dashboard#settings");
  const prompt = h.add("persistent-prompt", "textarea", panels.playground);
  prompt.value = "Keep this unfinished prompt";
  h.run("settingsOpened = []; openSettings = (name) => settingsOpened.push(name); initDashboardViews()");
  const expectView = (view) => {
    for (const name of Object.keys(panels)) {
      assert.equal(panels[name].classList.contains("hidden"), name !== view);
      assert.equal(nav[name].getAttribute("aria-current"), name === view ? "page" : null);
    }
  };
  expectView("channels");
  assert.equal(h.document.getElementById("workspace-title").textContent, "渠道管理");
  h.window.location.hash = "#playground";
  h.window.fire("hashchange");
  expectView("playground");
  h.window.location.hash = "#channels"; // The same event is emitted by browser Back.
  h.window.fire("hashchange");
  expectView("channels");
  for (const hash of ["", "#unknown", "#constructor"]) {
    h.window.location.hash = hash;
    h.window.fire("hashchange");
    expectView("overview");
  }
  h.window.location.hash = "#settings";
  h.window.fire("hashchange");
  expectView("overview");
  assert.equal(h.run("settingsOpened.join(',')"), "runtime");
  settingsLink.fire("click");
  assert.equal(h.run("settingsOpened.join(',')"), "runtime,runtime", "the settings link still opens its dialog when the hash is unchanged");
  assert.equal(prompt.value, "Keep this unfinished prompt");
  assert.equal(panels.playground.firstElementChild, prompt);
  assert.deepEqual(h.requests, []);
});

test("channel filters change rows while keeping all-channel statistics and routing state", () => {
  const h = harness();
  for (const id of ["channel-list", "channels-empty", "channels-table-wrap", "channels-filter-empty", "channel-count", "stat-channels", "stat-healthy", "stat-models", "stat-healthy-bar", "channel-search", "clear-channel-filters"]) h.add(id);
  const status = h.add("channel-status-filter", "select");
  status.value = "all";
  const routingSelect = h.add("routing-select", "select");
  const originalQuery = h.document.querySelector.bind(h.document);
  h.document.querySelector = (selector) => selector === "#playground-form select[name=channel_id]" ? routingSelect : originalQuery(selector);
  h.run(`
    channels = [
      { id: "a", name: "Primary", base_url: "https://one.example", type: "openai", enabled: true, last_status: "healthy", selected_models: ["alpha"], media_retention: "disabled" },
      { id: "b", name: "Backup", base_url: "https://two.example", type: "anthropic", enabled: true, last_status: "error", selected_models: ["beta"], media_retention: "best_effort" },
      { id: "c", name: "Paused", base_url: "https://three.example", type: "openai", enabled: false, last_status: "error", selected_models: ["gamma"], media_retention: "required" }
    ];
    playgroundUpdates = 0; updatePlaygroundModels = () => playgroundUpdates++;
    renderChannels(); bindChannelFilters();
  `);
  const list = h.document.getElementById("channel-list");
  assert.equal(list.children.length, 3);
  assert.equal(list.children[0].children.length, 7);
  assert.equal(list.children[0].children[3].firstElementChild.textContent, "运行正常");
  assert.equal(list.children[1].children[3].firstElementChild.textContent, "连接异常");
  assert.equal(list.children[2].children[3].firstElementChild.textContent, "已停用");
  assert.deepEqual(list.children.map((row) => row.children[5].firstElementChild.textContent), ["不保存", "尽力保存", "必须保存"]);
  routingSelect.value = "b";
  const originalRoutingOption = routingSelect.firstElementChild;
  const search = h.document.getElementById("channel-search");
  search.value = "BETA";
  search.fire("input");
  assert.equal(list.children.length, 1);
  assert.equal(list.firstElementChild.firstElementChild.firstElementChild.textContent, "Backup");
  assert.equal(h.document.getElementById("stat-channels").textContent, "3");
  assert.equal(h.document.getElementById("stat-healthy").textContent, "1");
  assert.equal(h.document.getElementById("stat-models").textContent, "3");
  assert.equal(h.document.getElementById("channel-count").textContent, "1 / 3 个渠道");
  assert.equal(routingSelect.value, "b");
  assert.equal(routingSelect.firstElementChild, originalRoutingOption);
  assert.equal(h.run("playgroundUpdates"), 1);
  search.value = "";
  status.value = "error";
  status.fire("change");
  assert.equal(list.children.length, 1, "disabled channels are not reported as active errors");
  status.value = "disabled";
  status.fire("change");
  assert.equal(list.children.length, 1);
  search.value = "does-not-exist";
  search.fire("input");
  assert.equal(h.document.getElementById("channels-empty").classList.contains("hidden"), true);
  assert.equal(h.document.getElementById("channels-filter-empty").classList.contains("hidden"), false);
  h.document.getElementById("clear-channel-filters").fire("click");
  assert.equal(list.children.length, 3);
  assert.equal(status.value, "all");
  h.run("channels = []; renderChannels({refreshPlayground: false})");
  assert.equal(h.document.getElementById("channels-empty").classList.contains("hidden"), false);
  assert.equal(h.document.getElementById("channels-filter-empty").classList.contains("hidden"), true);
});

test("settings route follows Back, Close and direct links without closing account-menu dialogs", () => {
  const h = harness("dashboard", "#channels");
  const dialog = h.add("settings-dialog", "dialog");
  h.run("openSettings = () => byId('settings-dialog').showModal(); initDashboardViews()");
  h.window.location.hash = "#settings";
  h.window.fire("hashchange");
  assert.equal(dialog.open, true);
  h.window.location.hash = "#channels";
  h.window.fire("hashchange");
  assert.equal(dialog.open, false, "Back must dismiss the dialog opened by the route");
  h.window.location.hash = "#settings";
  h.window.fire("hashchange");
  dialog.close();
  assert.equal(h.window.location.hash, "#channels", "Close restores the previous workspace without adding history");
  h.run("openSettings('account')");
  h.window.location.hash = "#playground";
  h.window.fire("hashchange");
  assert.equal(dialog.open, true, "an account-menu dialog is independent of the settings route");

  const direct = harness("dashboard", "#settings");
  const directDialog = direct.add("settings-dialog", "dialog");
  direct.run("openSettings = () => byId('settings-dialog').showModal(); initDashboardViews()");
  assert.equal(directDialog.open, true);
  directDialog.close();
  assert.equal(direct.window.location.hash, "#overview");
});

test("overview navigation uses a fragment to preserve the mounted Playground document", () => {
  const html = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), "dashboard.html"), "utf8");
  assert.match(html, /href="\/dashboard#overview"[^>]*id="nav-dashboard"/);
});

test("selected log detail refreshes in the background when its authoritative list status changes", () => {
  const h = harness("logs");
  for (const id of ["log-list", "logs-empty", "page-info", "prev-page", "next-page", "inspector-empty", "inspector-content", "visual-media-view"]) h.add(id);
  const video = h.add("playing-video", "video", h.document.getElementById("visual-media-view"));
  h.run(`
    detailOpens = []; openLog = (id, options) => detailOpens.push({id, ...options});
    activeLogID = 'selected'; activeLogRecord = {id:'selected', outcome:'running', status_code:0}; logTotal = 1;
    renderLogs([{id:'selected', outcome:'success', status_code:200}]);
  `);
  assert.equal(h.run("detailOpens.length"), 1);
  assert.equal(h.run("detailOpens[0].id"), "selected");
  assert.equal(h.run("detailOpens[0].background"), true);
  assert.equal(h.run("detailOpens[0].token === activeLogRequestToken"), true);
  assert.equal(h.run("activeLogID"), "selected");
  assert.equal(h.document.getElementById("visual-media-view").firstElementChild, video);
  h.run("activeLogRecord = {id:'selected', outcome:'success', status_code:200}; renderLogs([{id:'selected', outcome:'success', status_code:200}])");
  assert.equal(h.run("detailOpens.length"), 1, "unchanged lists do not refetch or replace playing media");
});

test("log list selection never reveals the mobile inspector until a record is activated", () => {
  const h = harness("logs");
  for (const id of ["log-list", "logs-empty", "page-info", "prev-page", "next-page"]) h.add(id);
  h.run(`
    detailOpens = []; openLog = (id, options) => detailOpens.push({id, ...options});
    logTotal = 2; renderLogs([{id:'first'}, {id:'second'}]);
  `);
  assert.equal(h.run("detailOpens[0].id"), "first");
  assert.equal(h.run("detailOpens[0].reveal"), false, "initial selection keeps the list visible");
  const list = h.document.getElementById("log-list");
  list.children[1].fire("click");
  assert.equal(h.run("detailOpens[1].id"), "second");
  assert.notEqual(h.run("detailOpens[1].reveal"), false, "an explicit click opens the inspector");
  list.children[0].fire("keydown", {key: "Enter"});
  assert.notEqual(h.run("detailOpens[2].reveal"), false, "keyboard activation also opens the inspector");
  h.run("renderLogs([{id:'filtered'}])");
  assert.equal(h.run("detailOpens[3].id"), "filtered");
  assert.equal(h.run("detailOpens[3].reveal"), false, "a new filter result cannot reopen the inspector");
});

test("switching Playground kinds enables only current parameters and preserves entered values", () => {
  const h = harness();
  const form = h.add("playground-form", "form");
  form.elements = {kind: new Element("input"), prompt: new Element("textarea")};
  const image = h.add("image-options", "fieldset", form);
  image.dataset.playgroundOptions = "image";
  const count = h.add("image-count", "input", image);
  count.value = "0";
  const video = h.add("video-options", "fieldset", form);
  video.dataset.playgroundOptions = "video";
  const reference = h.add("video-reference", "input", video);
  reference.value = "unfinished URL";
  h.run("updatePlaygroundModels = () => {}; setPlaygroundKind('chat')");
  assert.equal(image.disabled, true);
  assert.equal(video.disabled, true);
  h.run("setPlaygroundKind('image')");
  assert.equal(image.disabled, false);
  assert.equal(video.disabled, true);
  assert.equal(count.value, "0", "switching kinds must not discard a user's unfinished parameters");
  h.run("setPlaygroundKind('video')");
  assert.equal(image.disabled, true);
  assert.equal(video.disabled, false);
  assert.equal(reference.value, "unfinished URL");
});

test("late settings responses preserve input entered before the load completes", async () => {
  const h = harness();
  const form = h.add("settings-form", "form");
  form.elements = { port: new Element("input"), audit_retention_days: new Element("input") };
  for (const id of ["token-prefix", "token-updated-at", "stat-token", "stat-token-container"]) h.add(id);
  h.run("request = () => new Promise(resolve => {finishSettings = resolve})");
  const loading = h.run("loadSettings()");
  form.elements.port.value = "9000";
  h.run("finishSettings({port:8000, audit_retention_days:14})");
  await loading;
  assert.equal(form.elements.port.value, "9000");
  assert.equal(form.elements.audit_retention_days.value, 14);
});

test("only the newest log-list response can replace the list, including stale errors", async () => {
  const h = harness("logs");
  h.run("pending = []; rendered = []; request = (path) => new Promise((resolve, reject) => pending.push({path, resolve, reject})); renderLogs = (rows) => rendered.push(rows.map(row => row.id).join(','))");
  const first = h.run("logPage = 1; loadLogs()");
  const second = h.run("logPage = 2; loadLogs()");
  h.run("pending[1].resolve({total: 80, data: [{id: 'new'}]})");
  await second;
  h.run("pending[0].resolve({total: 10, data: [{id: 'old'}]})");
  await first;
  assert.equal(h.run("rendered.join('|')"), "new");
  assert.equal(h.run("logTotal"), 80);
  assert.match(h.run("pending[1].path"), /page=2/);
  const obsolete = h.run("loadLogs()");
  const current = h.run("loadLogs()");
  h.run("pending[2].reject(new Error('obsolete failure')); pending[3].resolve({total: 1, data: [{id: 'current'}]})");
  await obsolete;
  await current;
  const failure = h.run("loadLogs()");
  h.run("pending[4].reject(new Error('current failure'))");
  await assert.rejects(failure, /current failure/);
  assert.equal(h.run("rendered.join('|')"), "new|current");
});

test("log-list refresh retains the selected record and mounted media", () => {
  const h = harness("logs");
  for (const id of ["log-list", "logs-empty", "page-info", "prev-page", "next-page", "inspector-empty", "inspector-content", "visual-media-view"]) h.add(id);
  const video = h.add("playing-video", "video", h.document.getElementById("visual-media-view"));
  h.run(`
    detailOpens = []; openLog = (id) => detailOpens.push(id);
    activeLogID = "b"; activeLogRecord = {id: "b"}; logTotal = 2;
    renderLogs([{id: "a", started_at: "2026-10-07T00:00:00Z"}, {id: "b", started_at: "2026-10-07T00:00:00Z"}]);
  `);
  const list = h.document.getElementById("log-list");
  assert.equal(list.children[0].classList.contains("active-row"), false);
  assert.equal(list.children[1].classList.contains("active-row"), true);
  assert.equal(h.run("detailOpens.length"), 0);
  assert.equal(h.document.getElementById("visual-media-view").firstElementChild, video);
  h.run("renderLogs([{id:'a'}])");
  assert.equal(h.run("detailOpens.join(',')"), "a");
  h.run("renderLogs([])");
  assert.equal(h.run("activeLogRecord"), null);
  assert.equal(h.run("activeLogID"), "");
});

test("dashboard initialization binds interactions before independent loads and keeps the identity gate", async () => {
  const h = harness();
  h.run(`
    calls = []; failures = [];
    loadIdentity = async () => { calls.push('identity'); return {}; };
    toast = (message) => failures.push(message);
    for (const name of ['bindLogout','bindUserMenu','bindChannelForm','bindChannelFilters','bindSettings','bindPlayground','enhanceAllSelects','bindMobileNav','bindOverview','initDashboardViews']) {
      globalThis[name] = () => calls.push(name);
    }
    for (const name of ['loadAdapterTypes','loadChannelProfiles','loadChannels','loadSettings','loadOverviewConnection']) {
      globalThis[name] = async () => { calls.push(name); if (name === 'loadChannelProfiles') throw new Error('Profiles unavailable'); };
    }
  `);
  await h.run("initDashboard()");
  const calls = JSON.parse(h.run("JSON.stringify(calls)"));
  assert.ok(calls.indexOf("bindPlayground") < calls.indexOf("loadAdapterTypes"));
  assert.ok(calls.indexOf("bindSettings") < calls.indexOf("initDashboardViews"));
  assert.ok(calls.includes("loadSettings") && calls.includes("loadChannels"));
  assert.equal(h.run("failures.join(',')"), "Profiles unavailable");
  h.run("calls = []; loadIdentity = async () => { throw new Error('identity unavailable') }");
  await assert.rejects(h.run("initDashboard()"), /identity unavailable/);
  assert.equal(h.run("calls.length"), 0);
});

test("opening another select and outside dismissal reset expanded state", () => {
  const h = harness();
  const controls = [];
  for (const name of ["one", "two"]) {
    const field = h.add(`${name}-field`);
    field.className = "custom-select-field";
    h.add(`${name}-input`, "input", field).value = "all";
    const trigger = h.add(`${name}-trigger`, "button", field);
    trigger.className = "custom-select-trigger";
    h.add(`${name}-label`, "span", trigger);
    const dropdown = h.add(`${name}-dropdown`, "div", field);
    dropdown.className = "combobox-dropdown custom-select-dropdown hidden";
    for (const value of ["all", "enabled"]) {
      const option = new Element();
      option.className = "combobox-item";
      option.dataset.value = value;
      dropdown.append(option);
    }
    controls.push(h.run(`setupCustomSelect({wrapperId:'${name}-field',inputId:'${name}-input',triggerId:'${name}-trigger',labelId:'${name}-label',dropdownId:'${name}-dropdown'})`));
  }
  controls[0].openDropdown();
  assert.equal(h.document.getElementById("one-trigger").getAttribute("aria-expanded"), "true");
  controls[1].openDropdown();
  assert.equal(h.document.getElementById("one-trigger").getAttribute("aria-expanded"), "false");
  assert.equal(h.document.getElementById("one-dropdown").classList.contains("hidden"), true);
  controls[1].selectOption("enabled", "启用");
  const options = h.document.getElementById("two-dropdown").children;
  assert.equal(options[1].getAttribute("role"), "option");
  assert.equal(options[1].getAttribute("aria-selected"), "true");
  controls[1].openDropdown();
  h.run("closeSelectDropdowns()");
  assert.equal(h.document.getElementById("two-trigger").getAttribute("aria-expanded"), "false");
});

test("overview reports actual local health and copies the current API base", async () => {
  const h = harness();
  const status = h.add("overview-connection-status");
  const apiBase = h.add("overview-api-base");
  const copy = h.add("copy-api-base", "button");
  const topbar = h.add("topbar-status");
  const dot = h.add("topbar-dot", "span", topbar);
  dot.className = "status-dot";
  const topbarText = h.add("topbar-text", "span", topbar);
  h.run("copied = ''; copyTextWithFeedback = value => copied = value; paths = []; health = {status:'ok'}; request = async path => {paths.push(path); return health}; bindOverview()");
  copy.fire("click");
  assert.equal(apiBase.textContent, "http://gateway.test:8080/v1");
  assert.equal(h.run("copied"), apiBase.textContent);
  await h.run("loadOverviewConnection()");
  assert.equal(status.dataset.status, "healthy");
  assert.equal(topbarText.textContent, "网关正常");
  assert.equal(dot.classList.contains("error"), false);
  h.run("health = {status:'degraded'}");
  await h.run("loadOverviewConnection()");
  assert.equal(status.dataset.status, "degraded");
  assert.equal(topbarText.textContent, "状态异常");
  assert.equal(dot.classList.contains("warning"), true);
  assert.equal(h.run("paths.join(',')"), "/health,/health");
  h.run("request = async () => {throw new Error('offline')}");
  await assert.rejects(h.run("loadOverviewConnection()"), /offline/);
  assert.equal(status.dataset.status, "error");
  assert.equal(topbarText.textContent, "连接异常");
  assert.equal(dot.classList.contains("error"), true);
});

test("mobile navigation announces its state and dismisses on Escape or link activation", () => {
  const h = harness("logs");
  const toggle = h.add("mobile-nav-toggle", "button");
  const sidebar = h.add("app-sidebar", "aside");
  const backdrop = h.add("sidebar-backdrop");
  const logsLink = h.add("nav-logs", "a", sidebar);
  logsLink.className = "nav-link";
  logsLink.setAttribute("href", "/logs");
  h.run("bindMobileNav()");
  assert.equal(logsLink.getAttribute("aria-current"), "page");
  toggle.fire("click");
  assert.equal(toggle.getAttribute("aria-expanded"), "true");
  assert.equal(backdrop.classList.contains("active"), true);
  h.document.fire("keydown", { key: "Escape" });
  assert.equal(toggle.getAttribute("aria-expanded"), "false");
  assert.equal(toggle.focused, true);
  toggle.fire("click");
  logsLink.fire("click");
  assert.equal(sidebar.classList.contains("mobile-open"), false);
  assert.equal(backdrop.classList.contains("active"), false);
});
