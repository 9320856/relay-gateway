"use strict";

let csrfToken = "";
let currentUser = null;

const byId = (id) => document.getElementById(id);
const formObject = (form) => Object.fromEntries(new FormData(form).entries());

async function request(path, options = {}) {
  const opts = { credentials: "same-origin", ...options, headers: { ...(options.headers || {}) } };
  if (opts.body && typeof opts.body !== "string") {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(opts.body);
  }
  if (opts.method && !["GET", "HEAD"].includes(opts.method.toUpperCase()) && csrfToken) {
    opts.headers["X-CSRF-Token"] = csrfToken;
  }
  let response;
  try {
    response = await fetch(path, opts);
  } catch (cause) {
    if (cause?.name === "AbortError") throw cause;
    throw new Error("无法连接网关，请检查网络和服务是否已启动", { cause });
  }
  if (response.status === 401 && path !== "/api/auth/login") {
    location.assign("/login");
    const error = new Error("登录已过期，请重新登录");
    error.status = response.status;
    throw error;
  }
  const type = response.headers.get("content-type") || "";
  let data;
  try {
    data = type.includes("application/json") ? await response.json() : await response.text();
  } catch (cause) {
    const error = new Error(`服务器返回的数据格式异常（HTTP ${response.status}），请重试或查看服务日志`, { cause });
    error.status = response.status;
    throw error;
  }
  if (!response.ok) {
    const detail = typeof data === "string" ? "" : data?.error?.message || data?.error;
    const error = new Error(typeof detail === "string" && detail ? detail : `请求失败（HTTP ${response.status}），请重试或查看服务日志`);
    error.status = response.status;
    error.detail = typeof data === "object" ? data.detail : "";
    throw error;
  }
  return data;
}

async function loadIdentity() {
  const me = await request("/api/auth/me");
  csrfToken = me.csrf_token;
  currentUser = me;
  const user = byId("current-user");
  if (user) user.textContent = me.username;
  const avatars = document.querySelectorAll(".account-avatar");
  if (avatars.length > 0 && me.username) {
    const initial = me.username.trim().charAt(0).toUpperCase();
    avatars.forEach((a) => { a.textContent = initial; });
  }
  return me;
}

function toast(message, type = "info") {
  const region = byId("toast-region");
  if (!region) return;
  const item = document.createElement("div");
  item.className = "toast";
  const dot = document.createElement("span");
  const dotType = type === "error" ? "error" : type === "success" ? "" : type === "warning" ? "warning" : "idle";
  dot.className = dotType ? `status-dot ${dotType}` : "status-dot";
  const text = document.createElement("span");
  text.textContent = String(message);
  item.append(dot, text);
  region.append(item);
  setTimeout(() => {
    item.style.opacity = "0";
    item.style.transform = "translateY(8px) scale(0.96)";
    item.style.transition = "all 150ms ease-out";
    setTimeout(() => item.remove(), 160);
  }, 3500);
}

function setBusy(button, busy) {
  if (!button) return;
  button.disabled = busy;
  if (busy) {
    button.classList.add("is-loading");
    if (!button.dataset.label) button.dataset.label = button.textContent;
    button.textContent = "处理中…";
  } else {
    button.classList.remove("is-loading");
    if (button.dataset.label) {
      button.textContent = button.dataset.label;
    }
  }
}

function createEmptyState(message, hint = "") {
  const box = document.createElement("div");
  box.className = "empty-state";
  const msg = document.createElement("p");
  msg.style.margin = "0";
  msg.style.fontWeight = "500";
  msg.textContent = message;
  box.append(msg);
  if (hint) {
    const sub = document.createElement("small");
    sub.style.marginTop = "4px";
    sub.style.color = "var(--color-text-muted)";
    sub.textContent = hint;
    box.append(sub);
  }
  return box;
}

async function logout() {
  try {
    await request("/api/auth/logout", { method: "POST" });
  } finally {
    location.assign("/login");
  }
}

function bindLogout() {
  const button = byId("logout-button");
  if (button) button.addEventListener("click", logout);
}

function copyText(value) {
  if (!value) return Promise.reject(new Error("没有可复制的内容"));
  if (!navigator.clipboard?.writeText) return Promise.reject(new Error("当前浏览器无法访问剪贴板，请使用 localhost 或 HTTPS 地址后重试"));
  return navigator.clipboard.writeText(value).then(() => toast("已复制到剪贴板"), () => { throw new Error("复制失败，请允许浏览器访问剪贴板后重试"); });
}

async function initSetup() {
  const form = byId("setup-form");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = formObject(form);
    const error = byId("auth-error");
    error.textContent = "";
    if (data.password !== data.confirm_password) {
      error.textContent = "两次输入的密码不一致";
      return;
    }
    const button = form.querySelector("button[type=submit]");
    setBusy(button, true);
    try {
      const result = await request("/api/auth/setup", {
        method: "POST",
        body: { username: data.username, password: data.password }
      });
      csrfToken = result.csrf_token;
      byId("setup-token").textContent = result.gateway_api_key;
      byId("setup-copy").dataset.value = result.gateway_api_key;
      byId("setup-step").classList.add("hidden");
      byId("setup-result").classList.remove("hidden");
    } catch (err) {
      error.textContent = err.message;
    } finally {
      setBusy(button, false);
    }
  });

  byId("setup-copy").addEventListener("click", (event) => {
    copyText(event.currentTarget.dataset.value || "").catch((err) => toast(err.message));
  });
}

async function initLogin() {
  const form = byId("login-form");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = formObject(form);
    const error = byId("auth-error");
    error.textContent = "";
    const button = form.querySelector("button[type=submit]");
    setBusy(button, true);
    try {
      await request("/api/auth/login", { method: "POST", body: data });
      location.assign("/dashboard");
    } catch (err) {
      error.textContent = err.message;
      setBusy(button, false);
    }
  });
}

let channels = [];
let adapterTypes = [];
let channelProfiles = [];
let channelModelOptions = [];
let playgroundPollToken = 0;

function parseLines(value) {
  return String(value || "").split(/[\n,]/).map((item) => item.trim()).filter(Boolean);
}

function uniqueStrings(values) {
  const seen = new Set();
  const result = [];
  values.forEach((value) => {
    const text = String(value || "").trim();
    if (text && !seen.has(text)) {
      seen.add(text);
      result.push(text);
    }
  });
  return result;
}

function parseJSON(value, fallback) {
  try {
    return JSON.parse(value || "") ?? fallback;
  } catch (_) {
    return fallback;
  }
}

function channelModels(channel, includeAliases = true) {
  const values = [];
  const synced = parseJSON(channel?.models_synced_raw, []);
  if (Array.isArray(synced)) values.push(...synced);
  values.push(...parseLines(channel?.models_raw).filter((model) => model !== "*"));
  const mapping = parseJSON(channel?.model_map_raw, {});
  if (mapping && typeof mapping === "object") {
    if (includeAliases) values.push(...Object.keys(mapping));
    values.push(...Object.values(mapping));
  }
  return uniqueStrings(values);
}

function cell(text, className = "") {
  const td = document.createElement("td");
  if (className) td.className = className;
  td.textContent = text ?? "—";
  return td;
}

function actionButton(label, action, channel, className = "") {
  const button = document.createElement("button");
  button.type = "button";
  button.className = `button ${className}`.trim();
  button.textContent = label;
  button.addEventListener("click", () => action(channel, button));
  return button;
}

function renderChannels() {
  const tbody = byId("channel-list");
  if (!tbody) return;
  tbody.replaceChildren();
  const hasChannels = channels.length !== 0;
  byId("channels-empty")?.classList.toggle("hidden", hasChannels);
  byId("channels-table-wrap")?.classList.toggle("hidden", !hasChannels);

  let healthy = 0;
  const models = new Set();
  const playgroundSelect = document.querySelector("#playground-form select[name=channel_id]");
  const selectedChannel = playgroundSelect?.value || "";
  if (playgroundSelect) {
    const first = document.createElement("option");
    first.value = "";
    first.textContent = "自动路由（权重分发）";
    playgroundSelect.replaceChildren(first);
  }

  channels.forEach((channel) => {
    if (channel.last_status === "healthy" && channel.enabled) healthy += 1;
    channelModels(channel).forEach((model) => models.add(model));
    const tr = document.createElement("tr");

    // 1. Channel Name and URL on next line (12px mono)
    const nameTd = document.createElement("td");
    const title = document.createElement("span");
    const sub = document.createElement("span");
    title.className = "row-title";
    title.textContent = channel.name || channel.id;
    sub.className = "row-sub";
    sub.textContent = channel.base_url || channel.id;
    nameTd.append(title, sub);
    tr.append(nameTd);

    // 2. Type badge
    const typeTd = document.createElement("td");
    const typeBadge = document.createElement("span");
    typeBadge.className = "badge type-badge";
    typeBadge.textContent = channel.type;
    typeTd.append(typeBadge);
    tr.append(typeTd);

    // 3. Priority & Weight
    tr.append(cell(`P${channel.priority || 1} · W${channel.weight || 1}`));

    // 4. Status badge (24px high, 8px px, 6px radius)
    const statusTd = document.createElement("td");
    const badge = document.createElement("span");
    const isHealthy = channel.enabled && channel.last_status === "healthy";
    const isError = channel.last_status === "error";
    badge.className = `badge status-badge ${isHealthy ? "success" : isError ? "error" : !channel.enabled ? "disabled" : "running"}`;
    badge.textContent = channel.enabled ? (channel.last_status || "未测试") : "已停用";
    statusTd.append(badge);
    tr.append(statusTd);

    // 5. Models count
    tr.append(cell(`${channelModels(channel, false).length} 个`));

    // 6. Actions: Direct clear developer controls (测试, 编辑, 启用/停用, 删除)
    const actionsTd = document.createElement("td");
    actionsTd.className = "align-right";

    const actionsWrap = document.createElement("div");
    actionsWrap.className = "row-actions";

    const testBtn = actionButton("测试", testChannel, channel, "action-btn-test");
    const editBtn = actionButton("编辑", editChannel, channel, "action-btn-edit");
    const toggleBtn = actionButton(channel.enabled ? "停用" : "启用", toggleChannel, channel, "action-btn-toggle");
    const deleteBtn = actionButton("删除", deleteChannel, channel, "action-btn-danger");

    actionsWrap.append(testBtn, editBtn, toggleBtn, deleteBtn);
    actionsTd.append(actionsWrap);
    tr.append(actionsTd);
    tbody.append(tr);

    if (playgroundSelect && channel.enabled) {
      const option = document.createElement("option");
      option.value = channel.id;
      option.textContent = channel.name || channel.id;
      playgroundSelect.append(option);
    }
  });

  if (playgroundSelect && channels.some((channel) => channel.id === selectedChannel && channel.enabled)) {
    playgroundSelect.value = selectedChannel;
  }
  byId("stat-channels").textContent = String(channels.length);
  byId("stat-healthy").textContent = String(healthy);
  byId("stat-models").textContent = String(models.size);

  const healthyBar = byId("stat-healthy-bar");
  if (healthyBar) {
    const pct = channels.length > 0 ? Math.round((healthy / channels.length) * 100) : 0;
    healthyBar.style.width = `${pct}%`;
  }

  updatePlaygroundModels();
}

async function loadChannels() {
  const data = await request("/api/channels");
  channels = data.channels || [];
  renderChannels();
}

async function loadAdapterTypes() {
  const data = await request("/api/adapter-types");
  adapterTypes = data.types || [];
  const select = document.querySelector("#channel-form select[name=type]");
  if (select) {
    select.replaceChildren();
    adapterTypes.forEach((meta) => {
      const option = document.createElement("option");
      option.value = meta.type;
      option.textContent = meta.name;
      select.append(option);
    });
    select.addEventListener("change", () => {
      const meta = adapterTypes.find((item) => item.type === select.value);
      const url = document.querySelector("#channel-form input[name=base_url]");
      if (meta && !url.value) url.value = meta.default_url;
    });
  }
  renderUnifiedProtocolPicker();
}

function renderUnifiedProtocolPicker() {
  const picker = byId("channel-protocol-picker");
  if (!picker) return;

  const currentVal = picker.value;
  picker.replaceChildren();

  // 1. 官方内置协议分组 (仅保留 OpenAI 与 Anthropic 官方规范)
  const builtinGroup = document.createElement("optgroup");
  builtinGroup.label = "官方内置协议";
  const standardBuiltinTypes = ["openai", "anthropic"];

  const form = byId("channel-form");
  const currentChannelType = form?.elements?.type?.value;

  adapterTypes.forEach((meta) => {
    // 仅展示官方标准协议；若当前正在编辑历史存量渠道 (如 newapi/sub2api)，允许显示以维持向下兼容
    if (standardBuiltinTypes.includes(meta.type) || (currentChannelType && meta.type === currentChannelType)) {
      const opt = document.createElement("option");
      opt.value = `builtin:${meta.type}`;
      opt.textContent = meta.name;
      opt.dataset.type = meta.type;
      opt.dataset.defaultUrl = meta.default_url || "";
      builtinGroup.append(opt);
    }
  });
  picker.append(builtinGroup);

  // 2. 已发布自定义 Profile 分组 (完整保留凡人生图等所有自定义 Profile)
  if (channelProfiles && channelProfiles.length > 0) {
    const customGroup = document.createElement("optgroup");
    customGroup.label = "已发布自定义 Profile";
    channelProfiles.forEach((profile) => {
      const opt = document.createElement("option");
      opt.value = `profile:${profile.id}`;
      opt.textContent = `${profile.name} · Revision ${profile.revision}`;
      opt.dataset.profileId = profile.id;
      opt.dataset.revision = String(profile.revision);
      customGroup.append(opt);
    });
    picker.append(customGroup);
  }

  if (currentVal && Array.from(picker.options).some((o) => o.value === currentVal)) {
    picker.value = currentVal;
  }

  picker._rebuildCustomSelect?.();
  picker._updateCustomSelectLabel?.();
}

function syncProtocolPickerToForm() {
  const picker = byId("channel-protocol-picker");
  if (!picker) return;
  const selectedOpt = picker.selectedOptions && picker.selectedOptions[0];
  if (!selectedOpt) return;

  const form = byId("channel-form");
  if (!form) return;
  const typeSelect = form.elements.type;
  const profileSelect = form.elements.profile_id;
  const urlInput = form.elements.base_url;

  if (selectedOpt.value.startsWith("builtin:")) {
    const type = selectedOpt.dataset.type;
    if (typeSelect) {
      typeSelect.value = type;
      typeSelect.dispatchEvent(new Event("change"));
    }
    if (profileSelect) {
      profileSelect.value = "";
      profileSelect.dispatchEvent(new Event("change"));
    }
    if (urlInput && !urlInput.value && selectedOpt.dataset.defaultUrl) {
      urlInput.value = selectedOpt.dataset.defaultUrl;
    }
  } else if (selectedOpt.value.startsWith("profile:")) {
    const profileId = selectedOpt.dataset.profileId;
    const revision = selectedOpt.dataset.revision;
    if (profileSelect) {
      let opt = Array.from(profileSelect.options).find((o) => o.value === profileId);
      if (!opt) {
        opt = document.createElement("option");
        opt.value = profileId;
        profileSelect.append(opt);
      }
      opt.dataset.revision = String(revision);
      profileSelect.value = profileId;
      profileSelect.dispatchEvent(new Event("change"));
    }
    if (typeSelect && !typeSelect.value) {
      typeSelect.value = "openai";
    }
  }
}

function setupProtocolPicker() {
  const picker = byId("channel-protocol-picker");
  if (!picker || picker.dataset.boundProtocolPicker === "true") return;
  picker.dataset.boundProtocolPicker = "true";
  picker.addEventListener("change", syncProtocolPickerToForm);
}

// The channel adapter and a custom Profile are separate concerns. Adapters
// provide credentials/model discovery; a selected published Profile supplies
// the operation request and polling rules. Keep only published custom
// revisions in this picker so a channel can never bind an editable draft.
async function loadChannelProfiles() {
  const data = await request("/api/profiles");
  const sourceProfiles = Array.isArray(data.data) ? data.data : [];
  const resolved = await Promise.all(sourceProfiles.map(async (profile) => {
    if (profile.source !== "custom") return null;
    const latest = Math.max(Number(profile.latest_revision || 0), 0);
    const first = Math.max(1, latest - 99);
    for (let revision = latest; revision >= first; revision -= 1) {
      try {
        const item = await request(`/api/profiles/${encodeURIComponent(profile.id)}?revision=${revision}`);
        if (item.revision?.state === "published") {
          return { id: profile.id, name: profile.name || profile.id, revision: item.revision.revision };
        }
      } catch (err) {
        if (err.status !== 404) throw err;
      }
    }
    return null;
  }));
  channelProfiles = resolved.filter(Boolean).sort((left, right) => String(left.name).localeCompare(String(right.name), "zh-CN"));
  renderChannelProfileOptions();
  renderUnifiedProtocolPicker();
}

function renderChannelProfileOptions(selectedID = "", selectedRevision = 0) {
  const select = document.querySelector("#channel-form select[name=profile_id]");
  if (!select) return;
  select.replaceChildren();
  appendOption(select, "", channelProfiles.length ? "不绑定自定义 Profile（使用内置协议默认）" : "暂无已发布自定义 Profile", !selectedID);
  channelProfiles.forEach((profile) => {
    const option = appendOption(select, profile.id, `${profile.name} · Revision ${profile.revision}`, profile.id === selectedID && Number(profile.revision) === Number(selectedRevision));
    option.dataset.revision = String(profile.revision);
  });
  if (selectedID && !channelProfiles.some((profile) => profile.id === selectedID && Number(profile.revision) === Number(selectedRevision))) {
    select.value = "";
  }
}

async function restoreChannelProfileSelection(channelID) {
  if (!channelID) return;
  try {
    const data = await request(`/api/profile-bindings?channel_id=${encodeURIComponent(channelID)}`);
    const binding = (Array.isArray(data.data) ? data.data : []).find((item) => item.model_pattern === "*" && Number(item.precedence) === 0 && channelProfiles.some((profile) => profile.id === item.profile_id));
    if (!binding || !byId("channel-dialog")?.open) return;
    const current = channelProfiles.find((profile) => profile.id === binding.profile_id);
    renderChannelProfileOptions(binding.profile_id, current?.revision || binding.profile_revision);
    byId("channel-form").elements.profile_id.dispatchEvent(new Event("change"));

    const picker = byId("channel-protocol-picker");
    if (picker) {
      picker.value = `profile:${binding.profile_id}`;
      picker._updateCustomSelectLabel?.();
    }
  } catch (_) {
    // The binding list is supplementary to editing a channel; keep the form
    // usable when an old deployment does not expose the lookup endpoint.
  }
}

function setChannelModelOptions(models) {
  channelModelOptions = uniqueStrings(models);
  const catalog = byId("model-catalog");
  catalog.replaceChildren();
  const datalist = byId("channel-model-options");
  datalist.replaceChildren();
  channelModelOptions.forEach((model) => {
    const chip = document.createElement("span");
    chip.className = "model-chip";
    chip.textContent = model;
    catalog.append(chip);
    const option = document.createElement("option");
    option.value = model;
    datalist.append(option);
  });
  catalog.classList.toggle("empty-catalog", channelModelOptions.length === 0);
}

function addModelMappingRow(source = "", target = "") {
  if (!source && !target && channelModelOptions.length) {
    const used = new Set(Array.from(byId("mapping-list").querySelectorAll(".mapping-target"), (input) => input.value.trim()));
    const defaultModel = channelModelOptions.find((model) => !used.has(model)) || channelModelOptions[0];
    source = defaultModel;
    target = defaultModel;
  }
  const row = document.createElement("div");
  row.className = "mapping-row";
  const sourceInput = document.createElement("input");
  sourceInput.type = "text";
  sourceInput.className = "mapping-source";
  sourceInput.placeholder = "对外模型名";
  sourceInput.value = source;
  const targetInput = document.createElement("input");
  targetInput.type = "text";
  targetInput.className = "mapping-target";
  targetInput.setAttribute("list", "channel-model-options");
  targetInput.placeholder = "选择或输入上游模型";
  targetInput.value = target || channelModelOptions[0] || "";
  const remove = document.createElement("button");
  remove.type = "button";
  remove.className = "icon-button mapping-remove";
  remove.setAttribute("aria-label", "删除映射");
  remove.textContent = "×";
  remove.addEventListener("click", () => {
    row.remove();
    byId("mapping-empty").classList.toggle("hidden", byId("mapping-list").children.length !== 0);
  });
  row.append(sourceInput, targetInput, remove);
  byId("mapping-list").append(row);
  byId("mapping-empty").classList.add("hidden");
  sourceInput.focus();
}

function renderModelMappings(raw) {
  const list = byId("mapping-list");
  list.replaceChildren();
  const mapping = parseJSON(raw, {});
  if (mapping && typeof mapping === "object" && !Array.isArray(mapping)) {
    Object.entries(mapping).forEach(([source, target]) => addModelMappingRow(source, String(target || "")));
  }
  byId("mapping-empty").classList.toggle("hidden", list.children.length !== 0);
}

function serializeModelMappings() {
  const mapping = {};
  const seen = new Set();
  byId("mapping-list").querySelectorAll(".mapping-row").forEach((row) => {
    const source = row.querySelector(".mapping-source").value.trim();
    const target = row.querySelector(".mapping-target").value.trim();
    if (!source && !target) return;
    if (!source || !target) throw new Error("模型映射的对外名称和上游模型都不能为空");
    if (seen.has(source)) throw new Error(`对外模型名重复：${source}`);
    seen.add(source);
    mapping[source] = target;
  });
  return JSON.stringify(mapping);
}

function openChannelDialog(channel = null) {
  const form = byId("channel-form");
  form.reset();
  if (form.elements.profile_id) {
    form.elements.profile_id.value = "";
    form.elements.profile_id.dispatchEvent(new Event("change"));
  }
  byId("channel-dialog-title").textContent = channel ? "编辑渠道" : "添加渠道";
  if (channel && channel.type && !Array.from(form.elements.type.options).some((o) => o.value === channel.type)) {
    const opt = document.createElement("option");
    opt.value = channel.type;
    opt.textContent = `${channel.type} (已停用)`;
    form.elements.type.append(opt);
  }
  ["id", "name", "type", "base_url", "api_keys_raw", "models_raw", "headers_raw", "priority", "weight"].forEach((name) => {
    const input = form.elements[name];
    if (input) {
      input.value = channel ? (channel[name] ?? "") : "";
      input.dispatchEvent(new Event("change"));
    }
  });
  byId("channel-id-row").classList.toggle("hidden", !channel);
  form.elements.priority.value = channel?.priority || 1;
  form.elements.weight.value = channel?.weight || 1;
  form.elements.enabled.checked = channel ? Boolean(channel.enabled) : true;
  form.elements.fetch_models.checked = channel ? Boolean(channel.fetch_models) : true;
  const picker = byId("channel-protocol-picker");
  if (channel) {
    renderUnifiedProtocolPicker();
    if (picker && channel.type) {
      picker.value = `builtin:${channel.type}`;
      picker._updateCustomSelectLabel?.();
    }
  } else if (adapterTypes[0]) {
    form.elements.type.value = adapterTypes[0].type;
    form.elements.type.dispatchEvent(new Event("change"));
    form.elements.base_url.value = adapterTypes[0].default_url;
    renderUnifiedProtocolPicker();
    if (picker) {
      picker.value = `builtin:${adapterTypes[0].type}`;
      picker._updateCustomSelectLabel?.();
    }
  }
  const models = channel ? channelModels(channel, false) : [];
  setChannelModelOptions(models);
  renderModelMappings(channel?.model_map_raw || "{}");
  byId("model-fetch-status").textContent = models.length ? `已加载 ${models.length} 个已保存模型` : "尚未获取";
  form.scrollTop = 0;
  byId("channel-dialog").showModal();
  if (channel) void restoreChannelProfileSelection(channel.id);
}

function editChannel(channel) {
  openChannelDialog(channel);
}

async function fetchChannelModels(button) {
  const form = byId("channel-form");
  const data = formObject(form);
  const status = byId("model-fetch-status");
  if (!data.base_url) {
    status.textContent = "请先填写 Base URL";
    form.elements.base_url.focus();
    return;
  }
  setBusy(button, true);
  status.textContent = "正在连接上游并获取模型…";
  try {
    const result = await request("/api/channels/probe-models", {
      method: "POST",
      body: {
        channel_id: data.id || "",
        type: data.type,
        base_url: data.base_url,
        api_keys_raw: data.api_keys_raw
      }
    });
    if (result.status !== "ok") throw new Error(result.error || "模型获取失败");
    setChannelModelOptions(result.models || []);
    status.textContent = `已获取 ${result.count || 0} 个模型 · ${result.latency || 0} ms`;
    toast("模型列表已更新");
  } catch (err) {
    status.textContent = `获取失败：${err.message}`;
  } finally {
    setBusy(button, false);
  }
}

async function testChannel(channel, button) {
  setBusy(button, true);
  try {
    const result = await request(`/api/channels/${encodeURIComponent(channel.id)}/test`, { method: "POST" });
    toast(result.status === "ok" ? `渠道正常，发现 ${result.models?.length || 0} 个模型` : result.message);
    await loadChannels();
  } catch (err) {
    toast(err.message);
  } finally {
    setBusy(button, false);
  }
}

async function toggleChannel(channel, button) {
  setBusy(button, true);
  try {
    await request(`/api/channels/${encodeURIComponent(channel.id)}/toggle`, { method: "PATCH" });
    await loadChannels();
  } catch (err) {
    toast(err.message);
  } finally {
    setBusy(button, false);
  }
}

async function deleteChannel(channel) {
  if (!confirm(`确定删除渠道“${channel.name || channel.id}”吗？`)) return;
  try {
    await request(`/api/channels/${encodeURIComponent(channel.id)}`, { method: "DELETE" });
    toast("渠道已删除");
    await loadChannels();
  } catch (err) {
    toast(err.message);
  }
}

function bindChannelForm() {
  setupProtocolPicker();
  const openAdd = () => {
    openChannelDialog();
    void loadChannelProfiles().catch(() => {});
  };
  byId("add-channel")?.addEventListener("click", openAdd);
  byId("empty-add-channel")?.addEventListener("click", openAdd);
  ["cancel-channel", "cancel-channel-bottom"].forEach((id) => byId(id)?.addEventListener("click", () => byId("channel-dialog")?.close()));
  byId("fetch-channel-models")?.addEventListener("click", (event) => fetchChannelModels(event.currentTarget));
  byId("add-model-mapping")?.addEventListener("click", () => addModelMappingRow());
  byId("channel-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const button = form.querySelector("button[type=submit]");
    let modelMapRaw = "{}";
    try {
      modelMapRaw = serializeModelMappings();
    } catch (err) {
      toast(err.message);
      return;
    }
    const data = formObject(form);
    const payload = {
      ...data,
      model_map_raw: modelMapRaw,
      priority: Number(data.priority || 1),
      weight: Number(data.weight || 1),
      enabled: form.elements.enabled.checked,
      fetch_models: form.elements.fetch_models.checked
    };
    const profileID = String(data.profile_id || "").trim();
    const profileOption = profileID
      ? Array.from(form.elements.profile_id?.options || []).find((option) => option.value === profileID)
      : null;
    const profileRevision = Number(profileOption?.dataset?.revision || 0);
    delete payload.profile_id;
    if (!payload.id) delete payload.id;
    setBusy(button, true);
    try {
      const saved = await request("/api/channels", { method: "POST", body: payload });
      const id = saved.channel?.id;
      let profileBindingError = null;
      if (id) {
        try {
          if (profileID) {
            const bound = await request(`/api/channels/${encodeURIComponent(id)}/bind-profile`, {
              method: "POST",
              body: { profile_id: profileID, profile_revision: profileRevision }
            });
            toast(`已应用 Profile：${bound.bindings?.length || 0} 个操作`);
          } else {
            await request(`/api/channels/${encodeURIComponent(id)}/bind-profile`, {
              method: "POST",
              body: { profile_id: "" }
            });
          }
        } catch (err) {
          profileBindingError = err;
        }
      }
      byId("channel-dialog").close();
      toast(profileBindingError
        ? `渠道已保存，但 Profile 绑定失败：${profileBindingError.message}`
        : (payload.fetch_models ? "渠道已保存，正在同步模型" : "渠道已保存"));
      if (payload.fetch_models && id) {
        try {
          const result = await request(`/api/channels/${encodeURIComponent(id)}/models?refresh=true`);
          toast(`模型同步完成，共 ${result.models?.length || 0} 个`);
        } catch (err) {
          toast(`渠道已保存，但模型同步失败：${err.message}`);
        }
      }
      await loadChannels();
    } catch (err) {
      toast(err.message);
    } finally {
      setBusy(button, false);
    }
  });
}

async function loadSettings() {
  const settings = await request("/api/settings");
  const form = byId("settings-form");
  form.elements.port.value = settings.port;
  form.elements.audit_retention_days.value = settings.audit_retention_days;
  const info = settings.gateway_token || {};
  const isConfigured = Boolean(info.configured);
  byId("token-prefix").textContent = isConfigured ? `${info.prefix}…` : "未配置";
  const statToken = byId("stat-token");
  if (statToken) {
    statToken.textContent = isConfigured ? `${info.prefix}…` : "未配置";
  }
  byId("stat-token-container")?.classList.toggle("configured", isConfigured);
  byId("token-updated-at").textContent = info.updated_at ? `更新于 ${new Date(info.updated_at).toLocaleString()}` : "尚未生成";
}

let profiles = [];
let profileRevisions = new Map();
let profileBindings = [];
let selectedProfileID = "";
let selectedProfileRevision = 0;
let profileJSONPending = false;

const profileStateLabels = { draft: "草稿", published: "已发布", retired: "已停用" };

function defaultProfileContent(name = "", kind = "chat", mode = "direct") {
  const endpoints = {
    chat: { operation: "chat.completions", directPath: "/chat/completions", result: "choices" },
    image: { operation: "images.create", directPath: "/images/generations", asyncPath: "/images/jobs", result: "data" },
    video: { operation: "video.create", directPath: "/videos/generations", asyncPath: "/videos/generations", result: "url" }
  };
  const endpoint = endpoints[kind] || endpoints.chat;
  const async = mode === "async";
  const path = async ? (endpoint.asyncPath || endpoint.directPath) : endpoint.directPath;
  const imageJobResultURLPaths = [
    "job.assets", "assets",
    "job.assets.0.proxy_url", "job.assets.1.proxy_url", "job.assets.2.proxy_url", "job.assets.3.proxy_url",
    "job.assets.0.url", "job.assets.1.url", "job.assets.2.url", "job.assets.3.url",
    "assets.0.proxy_url", "assets.1.proxy_url", "assets.2.proxy_url", "assets.3.proxy_url",
    "data.0.url", "data.1.url", "data.2.url", "data.3.url",
    "data", "url", "image_url", "output", "images"
  ];
  return {
    schema_version: 1,
    name,
    operations: [{
      operation: endpoint.operation,
      execution_mode: async ? "async" : "direct",
      polling_mode: async ? "background" : "off",
      media_retention: "disabled",
      submit: { method: "POST", path, body_encoding: "json", body: {} },
      response: async
        ? { task_id_paths: kind === "image" ? ["job.id", "task_id", "id"] : ["id", "task_id"] }
        : { result_paths: [endpoint.result] },
      ...(async ? {
        poll: {
          method: "GET",
          path: `${path}/{task_id}`,
          interval_ms: 5000,
          max_attempts: 360,
          max_duration_ms: 1800000,
          status_path: kind === "image" ? "job.status" : "status",
          success_values: kind === "image" ? ["succeeded"] : ["completed", "succeeded"],
          failure_values: ["failed", "cancelled"],
          result_url_paths: kind === "image" ? imageJobResultURLPaths : ["url", "video_url", "data.0.url"]
        },
        ...(kind === "video" ? { content: { method: "GET", path: `${path}/{task_id}/content` } } : {})
      } : {}),
      policy: ""
    }]
  };
}

function defaultMultiOperationProfileContent(name = "", capabilities = ["chat", "image", "video"]) {
  const selected = new Set(capabilities);
  return {
    schema_version: 1,
    name,
    operations: [
      ["chat", "direct"],
      ["image", "async"],
      ["video", "async"]
    ].filter(([kind]) => selected.has(kind)).map(([kind, mode]) => defaultProfileContent("", kind, mode).operations[0])
  };
}

function appendOption(select, value, label, selected = false) {
  const option = document.createElement("option");
  option.value = value;
  option.textContent = label;
  option.selected = selected;
  select.append(option);
  return option;
}

function profileField(labelText, className, value = "", tagName = "input") {
  const label = document.createElement("label");
  label.textContent = labelText;
  const field = document.createElement(tagName);
  field.className = className;
  field.value = value == null ? "" : String(value);
  if (tagName === "textarea") field.rows = 3;
  label.append(field);
  return label;
}

function profileSelect(labelText, className, values, selected) {
  const label = document.createElement("label");
  label.textContent = labelText;
  const select = document.createElement("select");
  select.className = className;
  const labels = { direct: "同步：直接返回结果", async: "异步：返回任务 ID", off: "关闭轮询", client: "客户端查询", gateway_wait: "网关等待结果", background: "后台轮询", disabled: "不托管", best_effort: "尽力托管", required: "必须托管" };
  values.forEach((value) => appendOption(select, value, labels[value] || value, value === selected));
  select.value = values.includes(selected) ? selected : values[0];
  label.append(select);
  return label;
}

function profileJSON(value) {
  if (value == null || String(value).trim() === "") return {};
  let parsed;
  try { parsed = JSON.parse(String(value)); } catch { throw new Error("JSON 格式错误，请检查双引号、逗号和括号"); }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("JSON 字段必须是对象");
  return parsed;
}

function profileLines(value) {
  return parseLines(value);
}

const profileOperationLabels = {
  // 对话 / 响应 / 文本
  "chat.completions": "聊天",
  "responses.create": "Responses 响应",
  "messages.create": "Anthropic 消息",
  "messages.count_tokens": "Token 计数",

  // 向量嵌入
  "embeddings.create": "向量嵌入",

  // 音频 / 语音
  "audio.speech": "语音合成",
  "audio.transcriptions": "语音转文字",
  "audio.translations": "语音翻译",

  // 图像
  "images.create": "图片生成",
  "images.generations": "图片生成（兼容）",
  "images.edits": "图片编辑",
  "images.variations": "图片变体",

  // 内容安全与审核
  "moderations.create": "内容审核",
  "moderation.create": "内容审核",

  // 视频
  "video.create": "视频生成",
  "videos.generations": "视频生成",

  // 模型与管理
  "models.list": "模型列表",
  "models.retrieve": "模型详情",
  "batches.create": "批处理任务",
  "files.create": "文件上传",
  "files.content": "文件下载"
};

function profileOperationLabel(operationName) {
  const name = String(operationName || "").trim();
  if (!name) return "未命名操作";
  if (profileOperationLabels[name]) return profileOperationLabels[name];

  const lower = name.toLowerCase();
  if (lower.includes("moderation")) return "内容审核";
  if (lower.includes("transcription") || lower.includes("transcribe")) return "语音转文字";
  if (lower.includes("translation")) return "语音翻译";
  if (lower.includes("speech") || lower.includes("tts")) return "语音合成";
  if (lower.includes("embedding")) return "向量嵌入";
  if (lower.includes("video")) return "视频生成";
  if (lower.includes("image") || lower.includes("photo") || lower.includes("picture")) {
    if (lower.includes("edit")) return "图片编辑";
    if (lower.includes("variation")) return "图片变体";
    return "图片生成";
  }
  if (lower.includes("rerank")) return "文本重排";
  if (lower.includes("count_token") || lower.includes("token")) return "Token 计数";
  if (lower.includes("chat") || lower.includes("conversation")) return "聊天";
  if (lower.includes("message")) return "消息接口";
  if (lower.includes("completion")) return "文本补全";
  if (lower.includes("response")) return "Responses 响应";
  if (lower.includes("model")) return "模型查询";
  if (lower.includes("file")) return "文件接口";
  if (lower.includes("batch")) return "批处理";

  return name;
}

function renderProfileOperations(profile) {
  const container = byId("profile-operation-editor");
  container.replaceChildren();
  const operations = Array.isArray(profile?.operations) && profile.operations.length ? profile.operations : defaultProfileContent().operations;

  // Render operations summary and bulk collapse/expand toolbar if operations exist
  if (operations.length > 0) {
    const toolbar = document.createElement("div");
    toolbar.className = "profile-operations-toolbar";

    const summary = document.createElement("div");
    summary.className = "profile-operations-summary";
    const countBadge = document.createElement("span");
    countBadge.className = "badge";
    countBadge.textContent = `${operations.length} 个接口操作`;
    const hintSpan = document.createElement("span");
    hintSpan.className = "muted";
    hintSpan.textContent = "点击卡片标题可展开/折叠";
    summary.append(countBadge, hintSpan);

    const actions = document.createElement("div");
    actions.className = "profile-operations-actions";

    const collapseAllBtn = document.createElement("button");
    collapseAllBtn.type = "button";
    collapseAllBtn.id = "profile-collapse-all";
    collapseAllBtn.className = "button ghost compact-btn profile-collapse-all";
    collapseAllBtn.textContent = "全部折叠";
    collapseAllBtn.addEventListener("click", () => {
      container.querySelectorAll(".profile-operation-card").forEach((c) => {
        c.classList.add("collapsed");
        const h = c.querySelector(".profile-operation-header");
        if (h) h.setAttribute("aria-expanded", "false");
      });
    });

    const expandAllBtn = document.createElement("button");
    expandAllBtn.type = "button";
    expandAllBtn.id = "profile-expand-all";
    expandAllBtn.className = "button ghost compact-btn profile-expand-all";
    expandAllBtn.textContent = "全部展开";
    expandAllBtn.addEventListener("click", () => {
      container.querySelectorAll(".profile-operation-card").forEach((c) => {
        c.classList.remove("collapsed");
        const h = c.querySelector(".profile-operation-header");
        if (h) h.setAttribute("aria-expanded", "true");
      });
    });

    actions.append(collapseAllBtn, expandAllBtn);
    toolbar.append(summary, actions);
    container.append(toolbar);
  }

  operations.forEach((operation, index) => {
    const card = document.createElement("article");
    card.className = "profile-operation-card";
    // Smart default: when multiple operations exist, keep 1st expanded and collapse the rest
    if (operations.length > 1 && index > 0) {
      card.classList.add("collapsed");
    }

    const head = document.createElement("div");
    head.className = "profile-subhead profile-operation-header";
    head.title = "点击展开/折叠配置";
    head.setAttribute("role", "button");
    head.setAttribute("tabindex", "0");
    const isCollapsed = card.classList.contains("collapsed");
    head.setAttribute("aria-expanded", String(!isCollapsed));

    const identity = document.createElement("div");
    identity.className = "profile-operation-title profile-op-identity";

    const toggleIcon = document.createElement("span");
    toggleIcon.className = "profile-op-toggle-icon";
    toggleIcon.textContent = "▼";

    const opLabel = profileOperationLabel(operation.operation);
    const opRaw = operation.operation || "未命名操作";

    const title = document.createElement("strong");
    title.textContent = opLabel;

    const operationKey = document.createElement("code");
    operationKey.className = "profile-operation-key";
    operationKey.textContent = opRaw;
    if (opLabel === opRaw) {
      operationKey.style.display = "none";
    }

    const badges = document.createElement("span");
    badges.className = "profile-op-badges";

    const initialMethod = (operation.submit?.method || "POST").toUpperCase();
    const methodBadge = document.createElement("span");
    methodBadge.className = `badge method-badge ${initialMethod === "GET" ? "method-get" : initialMethod === "DELETE" ? "method-delete" : ""}`;
    methodBadge.textContent = initialMethod;

    const pathCode = document.createElement("code");
    pathCode.className = "profile-op-path";
    pathCode.textContent = operation.submit?.path || "/";

    const modeBadge = document.createElement("span");
    modeBadge.className = "badge mode-badge";
    modeBadge.textContent = operation.execution_mode === "async" ? "异步" : "同步";

    badges.append(methodBadge, pathCode, modeBadge);
    identity.append(toggleIcon, title, operationKey, badges);

    const actionsRight = document.createElement("div");
    actionsRight.className = "profile-op-actions";

    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "button ghost profile-remove-operation";
    remove.textContent = "移除";
    remove.addEventListener("click", (e) => {
      e.stopPropagation();
      card.remove();
      syncProfileJSONFromEditor();
      const toolbarCount = container.querySelector(".profile-operations-summary .badge");
      if (toolbarCount) {
        toolbarCount.textContent = `${container.querySelectorAll(".profile-operation-card").length} 个接口操作`;
      }
    });
    actionsRight.append(remove);

    head.append(identity, actionsRight);
    const toggleCollapse = () => {
      const collapsed = card.classList.toggle("collapsed");
      head.setAttribute("aria-expanded", String(!collapsed));
    };
    head.addEventListener("click", toggleCollapse);
    head.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " " || e.key === "Spacebar") {
        e.preventDefault();
        toggleCollapse();
      }
    });
    card.append(head);

    const body = document.createElement("div");
    body.className = "profile-operation-body";

    const basic = document.createElement("div");
    basic.className = "form-grid compact";
    basic.append(
      profileField("操作名称", "profile-op-name", operation.operation),
      profileField("执行模式", "profile-op-execution", operation.execution_mode || "direct"),
      profileField("轮询模式", "profile-op-polling", operation.polling_mode || "off"),
      profileSelect("媒体保留", "profile-op-retention", ["disabled", "best_effort", "required"], operation.media_retention || "disabled"),
      profileField("策略 Policy", "profile-op-policy", operation.policy || "")
    );
    body.append(basic);
    basic.querySelector(".profile-op-execution").type = "hidden";
    basic.querySelector(".profile-op-polling").type = "hidden";
    basic.querySelector(".profile-op-execution").parentElement?.classList.add("hidden");
    basic.querySelector(".profile-op-polling").parentElement?.classList.add("hidden");
    const modeHelp = document.createElement("p");
    modeHelp.className = "muted";
    modeHelp.textContent = operation.execution_mode === "async"
      ? (operation.polling_mode === "background" ? "自定义异步轮询：提交后返回任务 ID，由网关后台查询结果。" : "兼容已有配置：保留原查询策略；新建自定义协议统一使用后台轮询。")
      : "同步操作：上游直接返回结果，不需要轮询。";
    body.append(modeHelp);

    const submit = document.createElement("fieldset");
    submit.className = "profile-op-section";
    const submitLegend = document.createElement("legend"); submitLegend.textContent = "Submit"; submit.append(submitLegend);
    const submitGrid = document.createElement("div"); submitGrid.className = "form-grid compact";
    submitGrid.append(
      profileSelect("Method", "profile-op-submit-method", ["GET", "POST", "PUT", "PATCH", "DELETE"], operation.submit?.method || "POST"),
      profileField("Path", "profile-op-submit-path", operation.submit?.path || "/"),
      profileSelect("Body Encoding", "profile-op-submit-encoding", ["json", "form", "raw"], operation.submit?.body_encoding || "json"),
      profileField("Headers JSON", "profile-op-submit-headers", JSON.stringify(operation.submit?.headers || {}, null, 2), "textarea"),
      profileField("Body JSON", "profile-op-submit-body", JSON.stringify(operation.submit?.body || {}, null, 2), "textarea")
    );
    submit.append(submitGrid); body.append(submit);

    // Live update badges from submit inputs
    const submitMethodEl = submitGrid.querySelector(".profile-op-submit-method");
    if (submitMethodEl) {
      submitMethodEl.addEventListener("change", () => {
        const m = (submitMethodEl.value || "POST").toUpperCase();
        methodBadge.textContent = m;
        methodBadge.className = `badge method-badge ${m === "GET" ? "method-get" : m === "DELETE" ? "method-delete" : ""}`;
      });
    }
    const submitPathEl = submitGrid.querySelector(".profile-op-submit-path");
    if (submitPathEl) {
      submitPathEl.addEventListener("input", () => {
        pathCode.textContent = submitPathEl.value.trim() || "/";
      });
    }

    const opNameInput = basic.querySelector(".profile-op-name");
    if (opNameInput) {
      opNameInput.addEventListener("input", () => {
        const nextVal = opNameInput.value.trim();
        const nextLabel = profileOperationLabel(nextVal);
        title.textContent = nextLabel;
        operationKey.textContent = nextVal || "未命名操作";
        if (nextLabel === (nextVal || "未命名操作")) {
          operationKey.style.display = "none";
        } else {
          operationKey.style.display = "";
        }
      });
    }

    const response = document.createElement("fieldset");
    response.className = "profile-op-section";
    const responseLegend = document.createElement("legend"); responseLegend.textContent = "Response"; response.append(responseLegend);
    const responseGrid = document.createElement("div"); responseGrid.className = "form-grid compact";
    responseGrid.append(
      profileField("Task ID paths（每行一个）", "profile-op-response-task-paths", (operation.response?.task_id_paths || []).join("\n"), "textarea"),
      profileField("Result paths（每行一个）", "profile-op-response-result-paths", (operation.response?.result_paths || []).join("\n"), "textarea")
    );
    response.append(responseGrid); body.append(response);

    const poll = operation.poll || {};
    const pollSection = document.createElement("fieldset");
    pollSection.className = "profile-op-section profile-op-poll-section";
    const pollLegend = document.createElement("legend"); pollLegend.textContent = "Poll（异步操作）"; pollSection.append(pollLegend);
    const pollGrid = document.createElement("div"); pollGrid.className = "form-grid compact";
    pollGrid.append(
      profileSelect("Method", "profile-op-poll-method", ["GET", "POST", "PUT", "PATCH", "DELETE"], poll.method || "GET"),
      profileField("Path", "profile-op-poll-path", poll.path || "/tasks/{task_id}"),
      profileField("Interval ms", "profile-op-poll-interval", poll.interval_ms || 3000),
      profileSelect("Backoff mode", "profile-op-poll-backoff-mode", ["", "fixed", "linear", "exponential"], poll.backoff_mode || ""),
      profileField("Backoff base ms", "profile-op-poll-backoff-base", poll.backoff_base_ms || 0),
      profileField("Backoff max ms", "profile-op-poll-backoff-max", poll.backoff_max_ms || 0),
      profileField("Jitter ms", "profile-op-poll-jitter", poll.jitter_ms || 0),
      profileField("Max attempts", "profile-op-poll-attempts", poll.max_attempts || 200),
      profileField("Max duration ms", "profile-op-poll-duration", poll.max_duration_ms || 600000),
      profileField("Status path", "profile-op-poll-status", poll.status_path || "status"),
      profileField("成功状态（每行一个）", "profile-op-poll-success", (poll.success_values || ["completed", "succeeded"]).join("\n"), "textarea"),
      profileField("失败状态（每行一个）", "profile-op-poll-failure", (poll.failure_values || ["failed", "cancelled"]).join("\n"), "textarea"),
      profileField("Progress path", "profile-op-poll-progress", poll.progress_path || ""),
      profileField("结果 URL 路径（每行一个）", "profile-op-poll-result", (poll.result_url_paths || ["url", "video_url", "data.0.url"]).join("\n"), "textarea"),
      profileField("Headers JSON", "profile-op-poll-headers", JSON.stringify(poll.headers || {}, null, 2), "textarea")
    );
    pollSection.append(pollGrid); body.append(pollSection);

    const content = operation.content || {};
    const contentSection = document.createElement("fieldset");
    contentSection.className = "profile-op-section";
    const contentLegend = document.createElement("legend"); contentLegend.textContent = "Content（可选）"; contentSection.append(contentLegend);
    const contentGrid = document.createElement("div"); contentGrid.className = "form-grid compact";
    contentGrid.append(
      profileSelect("Method", "profile-op-content-method", ["GET", "POST", "PUT", "PATCH", "DELETE"], content.method || "GET"),
      profileField("Path", "profile-op-content-path", content.path || ""),
      profileField("Headers JSON", "profile-op-content-headers", JSON.stringify(content.headers || {}, null, 2), "textarea")
    );
    contentSection.append(contentGrid); body.append(contentSection);
    card.append(body);
    container.append(card);

    const execution = basic.querySelector(".profile-op-execution");
    const polling = basic.querySelector(".profile-op-polling");
    const updatePollState = () => {
      const enabled = execution.value === "async" && polling.value !== "off";
      pollSection.classList.toggle("hidden", !enabled);
      polling.disabled = execution.value !== "async";
      modeBadge.textContent = execution.value === "async" ? "异步" : "同步";
    };
    updatePollState();
  });
  if (typeof window !== "undefined" && window.location?.search && new URLSearchParams(window.location.search).get("collapse") === "1") {
    container.querySelectorAll(".profile-operation-card").forEach((c) => c.classList.add("collapsed"));
  }
  const addSection = document.createElement("div");
  addSection.className = "profile-add-operation-section";
  const kind = profileSelect("新增操作类型", "profile-add-kind", ["chat", "image", "video"], "chat");
  addSection.append(kind);
  const add = document.createElement("button");
  add.type = "button";
  add.className = "button profile-add-operation";
  add.textContent = "添加操作";
  add.addEventListener("click", () => {
    const current = collectProfileFromEditor();
    const selectedKind = kind.querySelector("select").value;
    const next = defaultProfileContent("", selectedKind, selectedKind === "chat" ? "direct" : "async").operations[0];
    if (current.operations.some((op) => op.operation === next.operation)) {
      toast("该操作已存在，请编辑现有操作；同一操作的不同协议请新建 Profile 后分别绑定。");
      return;
    }
    current.operations.push(next);
    renderProfileOperations(current);
    syncProfileJSONFromEditor();
  });
  addSection.append(add);
  container.append(addSection);
  if (typeof enhanceAllSelects === "function") {
    enhanceAllSelects(container);
  }
}

function collectProfileFromEditor() {
  if (profileJSONPending) throw new Error("JSON 尚未应用，请先点击“应用 JSON”，再保存或发布");
  let profile;
  try { profile = JSON.parse(byId("profile-json").value); } catch { throw new Error("配置 JSON 格式错误，请检查后再应用到表单"); }
  if (!profile || typeof profile !== "object" || Array.isArray(profile)) throw new Error("Profile JSON 必须是对象");
  profile.schema_version = Number(profile.schema_version || 1);
  profile.operations = [];
  byId("profile-operation-editor").querySelectorAll(".profile-operation-card").forEach((card) => {
    const value = (selector) => card.querySelector(selector)?.value || "";
    const execution = value(".profile-op-execution") || "direct";
    const polling = execution === "direct" ? "off" : value(".profile-op-polling") || "off";
    const operation = {
      operation: value(".profile-op-name").trim(),
      execution_mode: execution,
      polling_mode: polling,
      media_retention: value(".profile-op-retention") || "disabled",
      submit: {
        method: value(".profile-op-submit-method") || "POST",
        path: value(".profile-op-submit-path") || "/",
        body_encoding: value(".profile-op-submit-encoding") || "json",
        headers: profileJSON(value(".profile-op-submit-headers")),
        body: profileJSON(value(".profile-op-submit-body"))
      },
      response: {
        task_id_paths: profileLines(value(".profile-op-response-task-paths")),
        result_paths: profileLines(value(".profile-op-response-result-paths"))
      },
      policy: value(".profile-op-policy").trim()
    };
    if (operation.operation === "chat.completions" && execution === "async") throw new Error("聊天入口目前仅支持同步协议；异步请求请选择图片或视频");
    const pollPath = value(".profile-op-poll-path").trim();
    if (execution === "async" && polling !== "off") {
      operation.poll = {
        method: value(".profile-op-poll-method") || "GET",
        path: pollPath || "/tasks/{task_id}",
        headers: profileJSON(value(".profile-op-poll-headers")),
        interval_ms: Number(value(".profile-op-poll-interval") || 0),
        backoff_mode: value(".profile-op-poll-backoff-mode"),
        backoff_base_ms: Number(value(".profile-op-poll-backoff-base") || 0),
        backoff_max_ms: Number(value(".profile-op-poll-backoff-max") || 0),
        jitter_ms: Number(value(".profile-op-poll-jitter") || 0),
        max_attempts: Number(value(".profile-op-poll-attempts") || 1),
        max_duration_ms: Number(value(".profile-op-poll-duration") || 1),
        status_path: value(".profile-op-poll-status"),
        success_values: profileLines(value(".profile-op-poll-success")),
        failure_values: profileLines(value(".profile-op-poll-failure")),
        progress_path: value(".profile-op-poll-progress"),
        result_url_paths: profileLines(value(".profile-op-poll-result"))
      };
    }
    const contentPath = value(".profile-op-content-path").trim();
    if (contentPath) operation.content = { method: value(".profile-op-content-method") || "GET", path: contentPath, headers: profileJSON(value(".profile-op-content-headers")) };
    profile.operations.push(operation);
  });
  return profile;
}

function syncProfileJSONFromEditor() {
  try {
    byId("profile-json").value = JSON.stringify(collectProfileFromEditor(), null, 2);
    byId("profile-json").removeAttribute("aria-invalid");
  } catch (err) {
    byId("profile-json").setAttribute("aria-invalid", "true");
  }
}

function applyProfileJSON() {
  const profile = parseJSON(byId("profile-json").value, null);
  if (!profile || !Array.isArray(profile.operations) || !profile.operations.length || profile.operations.some((operation) => !operation || typeof operation !== "object" || Array.isArray(operation))) {
    toast("Profile JSON 必须包含非空的 operations 对象数组");
    return;
  }
  try {
    for (const operation of profile.operations) {
      for (const [section, fields] of [[operation.response, ["task_id_paths", "result_paths"]], [operation.poll, ["success_values", "failure_values", "result_url_paths"]]]) {
        for (const field of fields) {
          if (section?.[field] != null && (!Array.isArray(section[field]) || section[field].some((item) => typeof item !== "string"))) throw new Error(`${field} 必须是字符串数组`);
        }
      }
    }
    renderProfileOperations(profile);
  } catch (err) {
    toast(`配置格式错误：${err.message}`);
    return;
  }
  profileJSONPending = false;
  byId("profile-json").removeAttribute("aria-invalid");
}

function setProfileEditorReadOnly(readOnly) {
  const editor = byId("profile-operation-editor");
  ["input", "textarea", "select"].forEach((selector) => editor.querySelectorAll(selector).forEach((field) => {
    field.disabled = readOnly;
    if (typeof field._syncCustomSelectDisabled === "function") {
      field._syncCustomSelectDisabled();
    }
  }));
  editor.querySelectorAll(".profile-remove-operation").forEach((button) => {
    button.disabled = readOnly;
    button.classList.toggle("hidden", readOnly);
  });
  editor.querySelectorAll(".profile-add-operation").forEach((button) => {
    button.disabled = readOnly;
    button.classList.toggle("hidden", readOnly);
  });
  const addSection = editor.querySelector(".profile-add-operation-section");
  if (addSection) {
    addSection.classList.toggle("hidden", readOnly);
  }
  const addKind = editor.querySelector(".profile-add-kind");
  if (addKind && addKind.parentElement && addKind.parentElement.tagName === "LABEL") {
    addKind.parentElement.classList.toggle("hidden", readOnly);
  }
  byId("profile-json").readOnly = readOnly;
  byId("apply-profile-json").disabled = readOnly;
  byId("save-profile-revision").disabled = readOnly;
  if (!readOnly) editor.querySelectorAll(".profile-operation-card").forEach((card) => {
    card.querySelector(".profile-op-polling").disabled = card.querySelector(".profile-op-execution").value !== "async";
  });
  const state = currentProfileRevision()?.state;
  const isPublished = state === "published";
  const isRetired = state === "retired";
  const newRevisionBtn = byId("new-profile-revision");
  if (newRevisionBtn) newRevisionBtn.textContent = isPublished || isRetired ? "基于此版本修改" : "新建 Revision";
  const publishBtn = byId("publish-profile-revision");
  if (publishBtn) {
    publishBtn.disabled = isPublished;
    publishBtn.textContent = isRetired ? "重新发布" : "发布";
  }
  const retireBtn = byId("retire-profile-revision");
  if (retireBtn) {
    retireBtn.disabled = !isPublished;
  }
  const deleteRevisionBtn = byId("delete-profile-revision");
  if (deleteRevisionBtn) {
    deleteRevisionBtn.textContent = "删除版本";
  }
  updateProfileRevisionDeleteState();
}

function updateProfileRevisionDeleteState() {
  const button = byId("delete-profile-revision");
  if (!button) return;
  const revision = currentProfileRevision();
  const profile = profiles.find((item) => item.id === selectedProfileID);
  const hasBindings = Boolean(revision && profileBindings.some((binding) =>
    binding.profile_id === selectedProfileID && Number(binding.profile_revision) === Number(revision.revision)
  ));
  const isBuiltin = profile?.source === "builtin";
  button.disabled = !revision || isBuiltin || hasBindings;
  button.title = !revision
    ? ""
    : isBuiltin
      ? "内置 Profile 版本不能删除"
      : hasBindings
        ? "该版本已有渠道绑定，不能删除"
        : "删除当前 Revision";
}

function renderProfileList() {
  const list = byId("profile-list");
  if (!list) return;
  list.replaceChildren();
  const count = byId("profile-count");
  if (count) count.textContent = String(profiles.length);
  byId("profiles-empty")?.classList.toggle("hidden", profiles.length !== 0);
  profiles.forEach((profile) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `profile-list-item${profile.id === selectedProfileID ? " active" : ""}`;
    const name = document.createElement("strong"); name.textContent = profile.name || profile.id;
    const isBuiltin = profile.source === "builtin";
    const tag = isBuiltin ? "官方标准" : "自定义";
    const meta = document.createElement("small"); meta.textContent = `${tag} · ${profile.id} · v${profile.latest_revision || 0}`;
    button.append(name, meta);
    button.addEventListener("click", () => selectProfile(profile.id));
    list.append(button);
  });
}

function renderProfileRevisionSelect() {
  const select = byId("profile-revision-select");
  if (!select) return;
  select.replaceChildren();
  const revisions = profileRevisions.get(selectedProfileID) || [];
  revisions.slice().sort((a, b) => b.revision - a.revision).forEach((revision) => appendOption(select, String(revision.revision), `Revision ${revision.revision} · ${profileStateLabels[revision.state] || revision.state}`, revision.revision === selectedProfileRevision));
}

function currentProfileRevision() {
  return (profileRevisions.get(selectedProfileID) || []).find((revision) => revision.revision === Number(selectedProfileRevision));
}

function setProfileEditorNoRevisionState(profile) {
  const newRevBtn = byId("new-profile-revision");
  if (newRevBtn) {
    newRevBtn.disabled = false;
    newRevBtn.textContent = "新建 Revision";
  }
  const deleteProfileBtn = byId("delete-profile");
  if (deleteProfileBtn) {
    deleteProfileBtn.disabled = false;
    deleteProfileBtn.classList.toggle("hidden", profile.source === "builtin");
  }
  const saveBtn = byId("save-profile-revision");
  if (saveBtn) saveBtn.disabled = true;
  const publishBtn = byId("publish-profile-revision");
  if (publishBtn) publishBtn.disabled = true;
  const retireBtn = byId("retire-profile-revision");
  if (retireBtn) retireBtn.disabled = true;
  const copyBtn = byId("copy-profile");
  if (copyBtn) copyBtn.disabled = true;
  const applyJsonBtn = byId("apply-profile-json");
  if (applyJsonBtn) applyJsonBtn.disabled = true;
  const deleteRevBtn = byId("delete-profile-revision");
  if (deleteRevBtn) deleteRevBtn.disabled = true;
}

function renderProfileEditor() {
  profileJSONPending = false;
  const editor = byId("profile-editor");
  if (!editor) return;
  const profile = profiles.find((item) => item.id === selectedProfileID);
  const revision = currentProfileRevision();
  const empty = byId("profile-editor-empty");
  if (!profile) {
    if (empty) empty.classList.remove("hidden");
    editor.classList.add("hidden");
    updateProfileRevisionDeleteState();
    return;
  }
  if (empty) empty.classList.add("hidden");
  editor.classList.remove("hidden");
  byId("delete-profile")?.classList.toggle("hidden", profile.source === "builtin");
  const idEl = byId("profile-editor-id");
  if (idEl) idEl.textContent = profile.id;
  const titleEl = byId("profile-editor-title");
  if (titleEl) titleEl.textContent = profile.name || profile.id;

  if (!revision) {
    const metaEl = byId("profile-editor-meta");
    if (metaEl) metaEl.textContent = `${profile.source === "builtin" ? "官方标准" : "自定义"} · 暂无版本`;
    const stateEl = byId("profile-revision-state");
    if (stateEl) {
      stateEl.textContent = "暂无版本";
      stateEl.className = "badge";
    }
    const digestEl = byId("profile-revision-digest");
    if (digestEl) digestEl.textContent = "—";
    const opEditor = byId("profile-operation-editor");
    if (opEditor) {
      opEditor.replaceChildren();
      const emptyDiv = document.createElement("div");
      emptyDiv.className = "empty";
      emptyDiv.style.padding = "48px 16px";
      emptyDiv.style.textAlign = "center";
      const titleP = document.createElement("p");
      titleP.style.fontSize = "15px";
      titleP.style.fontWeight = "600";
      titleP.style.marginBottom = "8px";
      titleP.textContent = "当前 Profile 暂无版本";
      const hintP = document.createElement("p");
      hintP.className = "muted";
      hintP.style.margin = "0";
      hintP.textContent = "您可以点击右上角「新建 Revision」开始配置，或直接点击「删除 Profile」彻底移除。";
      emptyDiv.append(titleP, hintP);
      opEditor.append(emptyDiv);
    }
    const jsonEl = byId("profile-json");
    if (jsonEl) jsonEl.value = "";
    renderProfileRevisionSelect();
    setProfileEditorNoRevisionState(profile);
    resetProfileRevisionInspector();
    updateProfileRevisionDeleteState();
    return;
  }

  const metaEl = byId("profile-editor-meta");
  if (metaEl) metaEl.textContent = `${profile.source || "custom"} · Revision ${revision.revision} · ${new Date(revision.updated_at || revision.created_at || Date.now()).toLocaleString()}`;
  const state = profileStateLabels[revision.state] || revision.state || "unknown";
  const stateEl = byId("profile-revision-state");
  if (stateEl) {
    stateEl.textContent = state;
    stateEl.className = `badge ${revision.state === "published" ? "success" : revision.state === "retired" ? "error" : "running"}`;
  }
  const digestEl = byId("profile-revision-digest");
  if (digestEl) digestEl.textContent = revision.content_digest ? revision.content_digest.slice(0, 16) : "—";
  const content = parseJSON(revision.content_json, defaultProfileContent(profile.name));
  renderProfileOperations(content);
  const jsonEl = byId("profile-json");
  if (jsonEl) jsonEl.value = JSON.stringify(content, null, 2);
  const bindingForm = byId("profile-binding-form");
  if (bindingForm?.elements?.profile_revision) bindingForm.elements.profile_revision.value = revision.revision;
  renderProfileRevisionSelect();
  setProfileEditorReadOnly(revision.state !== "draft");
  resetProfileRevisionInspector();
}

function resetProfileRevisionInspector() {
  const inspector = byId("profile-revision-inspector");
  if (inspector) {
    inspector.classList.add("hidden");
    delete inspector.dataset.loadedRevision;
  }
  const summary = byId("profile-revision-info-summary");
  const output = byId("profile-revision-info-output");
  if (!summary || !output) return;
  summary.textContent = "选择“查看引用与 Diff”加载实时引用和相邻 Revision 差异。";
  output.textContent = "";
  output.classList.add("hidden");
}

function formatProfileDiffValue(value) {
  if (value === undefined) return "<不存在>";
  try { return JSON.stringify(value); } catch (err) { return String(value); }
}

async function inspectProfileRevision() {
  if (!selectedProfileID || !currentProfileRevision()) return;
  const inspector = byId("profile-revision-inspector");
  if (inspector && !inspector.classList.contains("hidden") && inspector.dataset.loadedRevision === String(selectedProfileRevision)) {
    inspector.classList.add("hidden");
    return;
  }
  if (inspector) {
    inspector.classList.remove("hidden");
    inspector.dataset.loadedRevision = String(selectedProfileRevision);
  }
  const profileID = selectedProfileID;
  const revision = Number(selectedProfileRevision);
  const summary = byId("profile-revision-info-summary");
  const output = byId("profile-revision-info-output");
  if (!summary || !output) return;
  summary.textContent = "正在读取引用和差异…";
  try {
    const [referencesData, diffData] = await Promise.all([
      request(`/api/profiles/${encodeURIComponent(selectedProfileID)}/revisions/${revision}/references`),
      request(`/api/profiles/${encodeURIComponent(selectedProfileID)}/revisions/${revision}/diff`)
    ]);
    if (profileID !== selectedProfileID || revision !== Number(selectedProfileRevision)) return;
    const references = referencesData.references || {};
    summary.textContent = `绑定 ${references.bindings || 0}（启用 ${references.enabled_bindings || 0}） · TaskRun ${references.task_runs || 0}（未终态 ${references.non_terminal_task_runs || 0}） · 对比 Revision ${diffData.from_revision || "—"}`;
    const changes = Array.isArray(diffData.changes) ? diffData.changes : [];
    output.textContent = changes.length === 0
      ? "与基准 Revision 没有检测到差异。"
      : changes.map((change) => `${change.path}\n  - ${formatProfileDiffValue(change.before)}\n  + ${formatProfileDiffValue(change.after)}`).join("\n");
    output.classList.remove("hidden");
  } catch (err) {
    if (profileID !== selectedProfileID || revision !== Number(selectedProfileRevision)) return;
    summary.textContent = `读取失败：${err.message}`;
    output.textContent = "";
    output.classList.add("hidden");
  }
}

async function loadProfileRevisions(profile) {
  const revisions = [];
  const latest = Math.min(Math.max(Number(profile.latest_revision || 0), 0), 100);
  const latestNumber = Math.max(Number(profile.latest_revision || 0), 0);
  for (let number = Math.max(1, latestNumber - latest + 1); number <= latestNumber; number += 1) {
    try {
      const data = await request(`/api/profiles/${encodeURIComponent(profile.id)}?revision=${number}`);
      if (data.revision) revisions.push(data.revision);
    } catch (err) {
      if (err.status !== 404) throw err;
    }
  }
  profileRevisions.set(profile.id, revisions);
}

async function loadProfiles() {
  const data = await request("/api/profiles");
  const rawProfiles = Array.isArray(data.data) ? data.data : [];
  // 仅保留两大官方标准协议（OpenAI 与 Anthropic）及所有用户自定义 Profile，剔除中间件杂项
  profiles = rawProfiles.filter((profile) =>
    profile.source === "custom" || profile.id === "builtin-openai" || profile.id === "builtin-anthropic"
  );
  await Promise.all(profiles.map(loadProfileRevisions));
  const urlProfile = new URLSearchParams(window.location.search).get("profile");
  if (urlProfile && profiles.some((p) => p.id === urlProfile)) {
    selectedProfileID = urlProfile;
  } else if (!selectedProfileID || !profiles.some((profile) => profile.id === selectedProfileID)) {
    selectedProfileID = profiles[0]?.id || "";
  }
  const revisions = profileRevisions.get(selectedProfileID) || [];
  if (!revisions.some((revision) => revision.revision === selectedProfileRevision)) selectedProfileRevision = revisions.slice().sort((a, b) => b.revision - a.revision)[0]?.revision || 0;
  renderProfileList();
  renderProfileEditor();
  if (selectedProfileID) await loadProfileBindings();
}

async function selectProfile(profileID) {
  selectedProfileID = profileID;
  const revisions = profileRevisions.get(profileID) || [];
  selectedProfileRevision = revisions.slice().sort((a, b) => b.revision - a.revision)[0]?.revision || 0;
  renderProfileList(); renderProfileEditor();
  await loadProfileBindings();
}

async function loadProfileBindings() {
  if (!selectedProfileID) return;
  const profileID = selectedProfileID;
  const data = await request(`/api/profile-bindings?profile_id=${encodeURIComponent(profileID)}`);
  if (profileID !== selectedProfileID) return;
  profileBindings = Array.isArray(data.data) ? data.data : [];
  renderProfileBindings();
}

function renderProfileBindings() {
  const list = byId("profile-binding-list");
  if (!list) {
    updateProfileRevisionDeleteState();
    return;
  }
  list.replaceChildren();
  byId("profile-bindings-empty")?.classList.toggle("hidden", profileBindings.length !== 0);
  profileBindings.forEach((binding) => {
    const row = document.createElement("tr");
    const channel = channels.find((item) => item.id === binding.channel_id);
    const opLabel = profileOperationLabel(binding.operation);
    const opDisplay = opLabel !== binding.operation ? `${opLabel} (${binding.operation})` : binding.operation;
    row.append(cell(channel?.name || binding.channel_id), cell(opDisplay), cell(binding.model_pattern), cell(String(binding.profile_revision)), cell(String(binding.precedence)), cell(binding.enabled ? "启用" : "停用"));
    const actions = document.createElement("td"); actions.className = "row-actions";
    actions.append(actionButton("编辑", () => editProfileBinding(binding), binding), actionButton(binding.enabled ? "停用" : "启用", () => toggleProfileBinding(binding), binding), actionButton("删除", () => deleteProfileBinding(binding), binding, "danger"));
    row.append(actions); list.append(row);
  });
  updateProfileRevisionDeleteState();
}

function populateProfileBindingForm(binding = null) {
  const form = byId("profile-binding-form");
  if (!form) return;
  form.reset();
  form.elements.id.value = binding?.id || "";
  form.elements.channel_id.replaceChildren();
  channels.forEach((channel) => appendOption(form.elements.channel_id, channel.id, channel.name || channel.id, channel.id === binding?.channel_id));
  const profile = profiles.find((item) => item.id === selectedProfileID);
  const revisions = profileRevisions.get(selectedProfileID) || [];
  const revision = binding?.profile_revision || selectedProfileRevision || profile?.latest_revision || 1;
  form.elements.profile_revision.value = revision;
  form.elements.operation.replaceChildren();
  const source = revisions.find((item) => item.revision === Number(revision));
  const content = parseJSON(source?.content_json, defaultProfileContent());
  (content.operations || []).forEach((operation) => {
    const opLabel = profileOperationLabel(operation.operation);
    const opDisplay = opLabel !== operation.operation ? `${opLabel} (${operation.operation})` : operation.operation;
    appendOption(form.elements.operation, operation.operation, opDisplay, operation.operation === binding?.operation);
  });
  form.elements.model_pattern.value = binding?.model_pattern || "*";
  form.elements.precedence.value = binding?.precedence ?? 0;
  form.elements.enabled.checked = binding ? Boolean(binding.enabled) : true;
}

function editProfileBinding(binding) { populateProfileBindingForm(binding); }

async function toggleProfileBinding(binding) {
  try { await request(`/api/profile-bindings/${binding.id}/toggle`, { method: "PATCH" }); await loadProfileBindings(); } catch (err) { toast(err.message); }
}

async function deleteProfileBinding(binding) {
  if (!confirm("确定删除此 Profile 绑定吗？")) return;
  try { await request(`/api/profile-bindings/${binding.id}`, { method: "DELETE" }); await loadProfileBindings(); } catch (err) { toast(err.message); }
}

async function createProfile() {
  const form = byId("profile-form");
  const data = formObject(form);
  const name = data.name.trim();
  const capabilities = ["chat", "image", "video"].filter((kind) => byId(`profile-create-${kind}`).checked);
  if (!capabilities.length) throw new Error("请至少选择一种功能");
  const profile = defaultMultiOperationProfileContent(name, capabilities);
  const result = await request("/api/profiles", { method: "POST", body: { name, source: "custom", revision: 1, profile } });
  const profileID = result.profile?.id;
  if (!profileID) throw new Error("Profile 已创建，但服务端未返回标识；请刷新列表检查");
  byId("profile-dialog").close();
  selectedProfileID = profileID; selectedProfileRevision = result.revision?.revision || 1;
  await loadProfiles();
  toast("Profile 已创建");
}

async function copySelectedProfile() {
  const source = profiles.find((item) => item.id === selectedProfileID);
  const revision = currentProfileRevision();
  if (!source || !revision) {
    toast("请先选择要复制的 Profile Revision");
    return;
  }
  const defaultID = `${source.id}-copy`;
  const id = window.prompt("新 Profile ID", defaultID);
  if (id == null || !id.trim()) return;
  const name = window.prompt("新 Profile 显示名称", `${source.name || source.id} Copy`);
  if (name == null || !name.trim()) return;
  let profile;
  try {
    profile = JSON.parse(revision.content_json);
  } catch (err) {
    toast(`源 Revision JSON 无效：${err.message}`);
    return;
  }
  try {
    await request("/api/profiles", {
      method: "POST",
      body: { id: id.trim(), name: name.trim(), source: "custom", revision: 1, profile }
    });
    selectedProfileID = id.trim();
    selectedProfileRevision = 1;
    await loadProfiles();
    toast("Profile 已复制为自定义草稿");
  } catch (err) {
    toast(err.message);
  }
}

async function saveProfileRevision() {
  const revision = currentProfileRevision();
  if (!revision || revision.state !== "draft") return;
  let profile;
  try { profile = collectProfileFromEditor(); } catch (err) { toast(`Profile 内容无效：${err.message}`); return; }
  try {
    await request(`/api/profiles/${encodeURIComponent(selectedProfileID)}/revisions/${revision.revision}`, { method: "PUT", body: { revision: revision.revision, profile } });
    await loadProfiles(); toast("草稿已保存");
  } catch (err) { toast(err.detail || err.message); }
}

async function createProfileRevision() {
  const profile = profiles.find((item) => item.id === selectedProfileID);
  if (!profile) return;
  const current = currentProfileRevision();
  let content;
  if (!current) {
    content = defaultMultiOperationProfileContent(profile.name || "自定义 Profile", ["chat"]);
  } else {
    try { content = collectProfileFromEditor(); } catch (err) { toast(`Profile 内容无效：${err.message}`); return; }
  }
  try {
    const result = await request(`/api/profiles/${encodeURIComponent(selectedProfileID)}/revisions`, { method: "POST", body: { profile: content } });
    selectedProfileRevision = result.revision?.revision || (current?.revision || 0) + 1;
    await loadProfiles(); toast(current?.state === "published" || current?.state === "retired" ? "已基于当前版本创建可编辑草稿" : "新草稿版本已创建");
  } catch (err) { toast(err.message); }
}

async function deleteSelectedProfileRevision() {
  const profile = profiles.find((item) => item.id === selectedProfileID);
  const revision = currentProfileRevision();
  if (!profile || !revision || profile.source === "builtin") return;
  const revs = profileRevisions.get(selectedProfileID) || [];
  const isLast = revs.length <= 1;
  const confirmMsg = isLast
    ? `Revision ${revision.revision} 是该 Profile 的最后一个版本。\n删除后该 Profile 将暂无版本，可随时新建版本或彻底删除该 Profile。\n\n确定删除吗？`
    : `确定删除 ${profile.name || profile.id} 的 Revision ${revision.revision} 吗？此操作不可撤销。`;
  if (!confirm(confirmMsg)) return;
  try {
    await request(`/api/profiles/${encodeURIComponent(selectedProfileID)}/revisions/${revision.revision}`, { method: "DELETE" });
    selectedProfileRevision = 0;
    await loadProfiles();
    toast(isLast ? "最后一个版本已删除；可随时点击「新建 Revision」或「删除 Profile」" : "Profile Revision 已删除");
  } catch (err) {
    toast(err.detail || err.message);
  }
}

async function changeProfileRevisionState(action) {
  const revision = currentProfileRevision();
  if (!revision) return;
  if (action === "retire" && !confirm(`确定停用版本 ${revision.revision} 吗？请先禁用或迁移该版本的渠道绑定；已接收的异步任务会继续执行。`)) return;
  if (action === "publish" && revision.state === "retired" && !confirm(`确定重新发布 Revision ${revision.revision} 吗？`)) return;
  try {
    if (action === "publish" && revision.state === "draft") {
      const profile = collectProfileFromEditor();
      await request(`/api/profiles/${encodeURIComponent(selectedProfileID)}/revisions/${revision.revision}`, { method: "PUT", body: { revision: revision.revision, profile } });
    }
    await request(`/api/profiles/${encodeURIComponent(selectedProfileID)}/revisions/${revision.revision}/${action}`, { method: "POST" });
    await loadProfiles();
    toast(action === "publish" ? "版本已发布，启用渠道绑定后可接收新请求" : "版本已停用");
  } catch (err) { toast(err.message); }
}

async function deleteSelectedProfile() {
  const profile = profiles.find((item) => item.id === selectedProfileID);
  if (!profile) return;
  if (profile.source === "builtin") {
    toast("内置 Profile 不能删除");
    return;
  }
  if (!confirm(`确定彻底删除自定义 Profile "${profile.name || profile.id}" 吗？此操作不可撤销。`)) return;
  try {
    await request(`/api/profiles/${encodeURIComponent(profile.id)}`, { method: "DELETE" });
    selectedProfileID = profiles.find((item) => item.id !== profile.id)?.id || "";
    selectedProfileRevision = 1;
    await loadProfiles();
    toast("自定义 Profile 已删除");
  } catch (err) { toast(err.detail || err.message); }
}

function bindProfileManager() {
  byId("profile-json")?.addEventListener("input", () => { profileJSONPending = true; });
  byId("profile-operation-editor")?.addEventListener("input", syncProfileJSONFromEditor);
  byId("profile-operation-editor")?.addEventListener("change", syncProfileJSONFromEditor);
  byId("add-profile")?.addEventListener("click", () => {
    byId("profile-form").reset();
    ["chat", "image", "video"].forEach((kind) => {
      const input = byId(`profile-create-${kind}`);
      const card = input?.closest?.(".profile-capability-card") || input?.parentElement;
      if (card && input) card.classList.toggle("active", input.checked);
    });
    byId("profile-dialog").showModal();
  });
  ["cancel-profile", "cancel-profile-bottom"].forEach((id) => byId(id)?.addEventListener("click", () => byId("profile-dialog").close()));
  byId("close-revision-inspector")?.addEventListener("click", () => {
    byId("profile-revision-inspector")?.classList.add("hidden");
  });
  ["chat", "image", "video"].forEach((kind) => {
    const input = byId(`profile-create-${kind}`);
    input?.addEventListener("change", () => {
      const card = input.closest?.(".profile-capability-card") || input.parentElement;
      card?.classList.toggle("active", input.checked);
    });
  });
  byId("profile-form")?.addEventListener("submit", async (event) => { event.preventDefault(); try { await createProfile(); } catch (err) { toast(err.message); } });
  byId("profile-revision-select")?.addEventListener("change", async (event) => { selectedProfileRevision = Number(event.currentTarget.value); renderProfileEditor(); await loadProfileBindings(); });
  byId("profile-revision-info")?.addEventListener("click", inspectProfileRevision);
  byId("copy-profile")?.addEventListener("click", copySelectedProfile);
  byId("save-profile-revision")?.addEventListener("click", saveProfileRevision);
  byId("new-profile-revision")?.addEventListener("click", createProfileRevision);
  byId("publish-profile-revision")?.addEventListener("click", () => changeProfileRevisionState("publish"));
  byId("retire-profile-revision")?.addEventListener("click", () => changeProfileRevisionState("retire"));
  byId("delete-profile-revision")?.addEventListener("click", deleteSelectedProfileRevision);
  byId("delete-profile")?.addEventListener("click", deleteSelectedProfile);
  byId("apply-profile-json")?.addEventListener("click", applyProfileJSON);
  byId("reset-profile-binding")?.addEventListener("click", () => populateProfileBindingForm());
  byId("profile-binding-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const data = formObject(form);
    const payload = { channel_id: data.channel_id, operation: data.operation, model_pattern: data.model_pattern.trim(), profile_id: selectedProfileID, profile_revision: Number(data.profile_revision), precedence: Number(data.precedence || 0), enabled: form.elements.enabled.checked };
    try {
      const path = data.id ? `/api/profile-bindings/${data.id}` : "/api/profile-bindings";
      await request(path, { method: data.id ? "PUT" : "POST", body: payload });
      populateProfileBindingForm(); await loadProfileBindings(); toast("Profile 绑定已保存");
    } catch (err) { toast(err.message); }
  });
  populateProfileBindingForm();
}

function clearOneTimeToken() {
  byId("new-token").textContent = "";
  byId("copy-token").dataset.value = "";
  byId("token-output").classList.add("hidden");
}

function setSettingsTab(name) {
  document.querySelectorAll(".settings-tab").forEach((tab) => {
    const active = tab.dataset.settingsTab === name;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  });
  document.querySelectorAll("[data-settings-panel]").forEach((panel) => {
    panel.classList.toggle("hidden", panel.dataset.settingsPanel !== name);
  });
}

function openSettings(name) {
  setSettingsTab(name);
  byId("user-menu-popover").classList.add("hidden");
  byId("user-menu-button").setAttribute("aria-expanded", "false");
  byId("settings-dialog").showModal();
}

function bindUserMenu() {
  const button = byId("user-menu-button");
  const popover = byId("user-menu-popover");
  button.addEventListener("click", () => {
    const open = popover.classList.toggle("hidden") === false;
    button.setAttribute("aria-expanded", String(open));
  });
  document.addEventListener("click", (event) => {
    if (!event.target.closest(".user-menu")) {
      popover.classList.add("hidden");
      button.setAttribute("aria-expanded", "false");
    }
  });
  popover.querySelectorAll("[data-settings-tab]").forEach((item) => {
    item.addEventListener("click", () => openSettings(item.dataset.settingsTab));
  });
}

function bindSettings() {
  document.querySelectorAll(".settings-tab").forEach((button) => {
    button.addEventListener("click", () => setSettingsTab(button.dataset.settingsTab));
  });
  byId("close-settings").addEventListener("click", () => byId("settings-dialog").close());
  byId("settings-dialog").addEventListener("close", clearOneTimeToken);
  byId("settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = formObject(event.currentTarget);
    try {
      await request("/api/settings", {
        method: "POST",
        body: { port: Number(data.port), audit_retention_days: Number(data.audit_retention_days) }
      });
      toast("设置已保存；端口修改将在重启后生效");
    } catch (err) {
      toast(err.message);
    }
  });
  byId("account-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    try {
      const result = await request("/api/account/credentials", { method: "PUT", body: formObject(form) });
      csrfToken = result.csrf_token;
      byId("current-user").textContent = result.username;
      form.reset();
      toast("账号信息已更新，其他会话已退出");
    } catch (err) {
      toast(err.message);
    }
  });
  byId("rotate-token").addEventListener("click", async () => {
    if (!confirm("重置后，旧网关 Key 会立即失效。继续吗？")) return;
    try {
      const result = await request("/api/gateway-token/rotate", { method: "POST" });
      byId("new-token").textContent = result.gateway_api_key;
      byId("copy-token").dataset.value = result.gateway_api_key;
      byId("token-output").classList.remove("hidden");
      await loadSettings();
      toast("网关 Key 已重置");
    } catch (err) {
      toast(err.message);
    }
  });
  byId("copy-token").addEventListener("click", (event) => {
    copyText(event.currentTarget.dataset.value || "").catch((err) => toast(err.message));
  });
}

function isVideoModelName(model) {
  const value = String(model || "").toLowerCase();
  return value.includes("video") || value.includes("kling") || value.includes("runway") || value.includes("sora") || value.includes("cogvideo") || value.includes("luma") || value.includes("hailuo") || value.includes("pika") || value.includes("minimax-video") || value.includes("wanx-video");
}

function isImageModelName(model) {
  const value = String(model || "").toLowerCase();
  if (isVideoModelName(value)) return false;
  return value.includes("image") || value.includes("imagine") || value.includes("dall-e") || value.includes("flux") || value.includes("midjourney") || value.includes("stable-diffusion") || value.startsWith("sdxl") || value.includes("recraft") || value.includes("ideogram") || value.includes("cogview");
}

function updatePlaygroundModels() {
  const form = byId("playground-form");
  if (!form) return;
  const kind = form.elements.kind.value;
  const channelID = form.elements.channel_id.value;
  const selected = channels.find((channel) => channel.id === channelID);
  let models = selected ? channelModels(selected) : uniqueStrings(channels.filter((channel) => channel.enabled).flatMap((channel) => channelModels(channel)));
  const filtered = models.filter((model) => kind === "image" ? isImageModelName(model) : kind === "video" ? isVideoModelName(model) : (!isImageModelName(model) && !isVideoModelName(model)));
  if (filtered.length) models = filtered;

  const select = byId("playground-model-select");
  const input = byId("playground-model-input");
  if (!select || !input) return;

  const prevValue = (select.value && select.value !== "__custom__") ? select.value : input.value;
  select.replaceChildren();

  if (models.length === 0) {
    const emptyOpt = document.createElement("option");
    emptyOpt.value = "__custom__";
    emptyOpt.textContent = "当前分类暂无预设模型 (点击手动输入)";
    select.append(emptyOpt);
    select.value = "__custom__";
    input.classList.remove("hidden");
    input.value = "";
    input.placeholder = kind === "image" ? "输入生图模型名，例如：gpt-image-2" : kind === "video" ? "输入视频模型名，例如：grok-imagine-video-1.5" : "输入对话模型名，例如：gpt-4o";
    return;
  }

  let matched = false;
  models.forEach((model) => {
    const option = document.createElement("option");
    option.value = model;
    option.textContent = model;
    if (model === prevValue) {
      option.selected = true;
      matched = true;
    }
    select.append(option);
  });

  const customOpt = document.createElement("option");
  customOpt.value = "__custom__";
  customOpt.textContent = "➕ 手动输入自定义模型...";
  select.append(customOpt);

  if (matched) {
    select.value = prevValue;
    input.value = prevValue;
    input.classList.add("hidden");
  } else {
    select.selectedIndex = 0;
    input.value = select.value;
    input.classList.add("hidden");
  }
}

function setPlaygroundKind(kind) {
  const form = byId("playground-form");
  form.elements.kind.value = kind;
  document.querySelectorAll("[data-playground-kind]").forEach((button) => {
    const active = button.dataset.playgroundKind === kind;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", String(active));
  });
  document.querySelectorAll("[data-playground-options]").forEach((panel) => {
    panel.classList.toggle("hidden", panel.dataset.playgroundOptions !== kind);
  });
  const prompts = { chat: "输入对话测试内容", image: "描述希望生成的图片", video: "描述希望生成的视频" };
  form.elements.prompt.placeholder = prompts[kind];
  updatePlaygroundModels();
}

function clearPlaygroundMedia() {
  const media = byId("playground-media");
  if (media) {
    media.replaceChildren();
    media.classList.add("hidden");
  }
  const chat = byId("playground-chat");
  if (chat) {
    chat.replaceChildren();
    chat.classList.add("hidden");
  }
}

// Only use URLs that are safe for a media element or an interactive link.
// Provider responses are untrusted: assigning javascript:, data:text/html, or
// an SVG data URL to href/src would otherwise give the response control over
// the administrator's browser. Same-origin relative URLs are retained for
// gateway-generated media endpoints; remote media is limited to HTTP(S).
const SAFE_DATA_IMAGE_TYPES = new Set(["image/png", "image/jpeg", "image/webp", "image/gif"]);
function normalizeMediaURL(value, type = "image") {
  if (typeof value !== "string") return null;
  const raw = value.trim();
  if (!raw || /[\u0000-\u001f\u007f]/.test(raw)) return null;

  if (/^data:/i.test(raw)) {
    if (type !== "image") return null;
    const comma = raw.indexOf(",");
    if (comma < 0) return null;
    const mimeType = raw.slice(5, comma).split(";", 1)[0].trim().toLowerCase();
    return SAFE_DATA_IMAGE_TYPES.has(mimeType) ? raw : null;
  }

  // Reject every non-HTTP URL scheme, including protocol-relative URLs. URL()
  // normalizes backslashes, so parse first and then enforce same-origin for
  // non-absolute URLs to avoid //host and /\\host bypasses.
  if (/^[a-z][a-z0-9+.-]*:/i.test(raw) && !/^https?:\/\//i.test(raw)) return null;
  if (raw.startsWith("//")) return null;
  try {
    const currentLocation = window.location || (typeof location !== "undefined" ? location : null);
    const origin = currentLocation?.origin || "";
    const baseURL = currentLocation?.href || (origin ? `${origin}/` : "");
    if (!baseURL) return null;
    const parsed = new URL(raw, baseURL);
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;
    const isAbsoluteHTTP = /^https?:\/\//i.test(raw);
    if (!isAbsoluteHTTP && origin && parsed.origin !== origin) return null;
    return raw;
  } catch (_err) {
    return null;
  }
}

function createMediaFigure(url, title, type = "image", options = {}) {
  const figure = document.createElement("figure");
  figure.className = "media-card";
  url = normalizeMediaURL(url, type);
  if (!url) {
    const notice = document.createElement("div");
    notice.className = "media-load-notice";
    notice.textContent = "⚠️ 媒体地址不受支持或已被安全策略拦截";
    figure.append(notice);
    return figure;
  }
  let reloadVideo = null;

  if (type === "video") {
    const container = document.createElement("div");
    container.className = "media-video-container";

    const video = document.createElement("video");
    video.controls = true;
    video.preload = "metadata";
    video.setAttribute("preload", "metadata");
    video.playsInline = true;
    video.setAttribute("playsinline", "true");
    video.setAttribute("webkit-playsinline", "true");
    video.setAttribute("x5-playsinline", "true");
    video.setAttribute("x5-video-player-type", "h5");
    video.disablePictureInPicture = true;
    video.setAttribute("disablePictureInPicture", "true");
    video.setAttribute("controlsList", "nodownload");
    video.setAttribute("translate", "no");
    video.classList.add("notranslate");

    const mediaSrc = url;
    video.src = mediaSrc;

    reloadVideo = () => {
      video.pause();
      video.removeAttribute("src");
      video.load();
      video.preload = "auto";
      video.setAttribute("preload", "auto");
      video.src = mediaSrc;
      video.load();
      video.play().catch(() => {});
    };

    video.addEventListener("loadedmetadata", () => {
      if (video.videoWidth && video.videoHeight) {
        let metaEl = caption.querySelector(".media-meta-badge");
        if (!metaEl) {
          metaEl = document.createElement("span");
          metaEl.className = "media-meta-badge";
          label.after(metaEl);
        }
        const dur = Math.round(video.duration);
        const durText = dur > 0 ? ` · ${dur}s` : "";
        metaEl.textContent = `${video.videoWidth}×${video.videoHeight}${durText}`;
      }
    });

    video.addEventListener("error", () => {
      if (!figure.querySelector(".media-load-notice")) {
        const notice = document.createElement("div");
        notice.className = "media-load-notice";
        notice.textContent = "⚠️ 视频流加载中或上游生成尚未就绪，可点击下方「播放视频」在新窗口播放或下载。";
        figure.prepend(notice);
      }
    });
    container.append(video);
    figure.append(container);
  } else {
    const img = document.createElement("img");
    img.src = url;
    img.alt = title;
    img.loading = "lazy";
    img.referrerPolicy = "no-referrer";

    const isDataUri = /^data:/i.test(url);
    const isInternalMedia = (() => {
      try {
        if (url.startsWith("/")) return true;
        const origin = typeof window !== "undefined" && window.location ? window.location.origin : "http://localhost:8000";
        const parsed = new URL(url, origin);
        return parsed.pathname.startsWith("/v1/media/") ||
               parsed.pathname.startsWith("/api/media-assets/") ||
               (typeof window !== "undefined" && window.location && parsed.origin === window.location.origin);
      } catch {
        return false;
      }
    })();

    img.addEventListener("error", () => {
      if (!isDataUri && !img.dataset.proxied && !isInternalMedia) {
        img.dataset.proxied = "1";
        img.src = `/api/media-proxy?url=${encodeURIComponent(url)}`;
      } else {
        img.classList.add("hidden");
        if (!figure.querySelector(".media-load-notice")) {
          const notice = document.createElement("div");
          notice.className = "media-load-notice";
          notice.textContent = "⚠️ 图片加载失败，可尝试点击下方按钮查看或下载";
          figure.prepend(notice);
        }
      }
    });
    figure.append(img);
  }

  const caption = document.createElement("figcaption");
  caption.className = "media-caption-bar";

  const label = document.createElement("span");
  label.className = "media-label";
  label.textContent = title;
  caption.append(label);

  const actions = document.createElement("div");
  actions.className = "media-actions";

  // A logged-in preview may use an admin-only URL.  Do not expose that URL
  // through controls which users reasonably expect to be shareable.
  const publicURL = Object.prototype.hasOwnProperty.call(options || {}, "publicURL")
    ? normalizeMediaURL(options.publicURL, type)
    : url;
  const hasPublicURL = Boolean(publicURL);
  const actionURL = publicURL || url;
  const isDataUri = /^data:/i.test(actionURL);
  if (hasPublicURL) {
  const openLink = document.createElement("a");
  openLink.href = actionURL;
  openLink.target = "_blank";
  openLink.rel = "noreferrer noopener";
  openLink.className = "btn-media-action";
  openLink.title = type === "video" ? "在浏览器新标签页播放完整视频" : "在浏览器新标签页打开原始文件";
  openLink.textContent = type === "video" ? "播放视频 ↗" : "打开原图 ↗";
  if (type === "image" && isDataUri) {
    openLink.addEventListener("click", (e) => {
      e.preventDefault();
      const win = window.open("", "_blank");
      if (win && win.document) {
        win.document.title = title || "Image Preview";
        if (win.document.body) {
          win.document.body.style.margin = "0";
          win.document.body.style.background = "#090d16";
          win.document.body.style.display = "flex";
          win.document.body.style.alignItems = "center";
          win.document.body.style.justifyContent = "center";
          win.document.body.style.height = "100vh";
          const previewImg = win.document.createElement("img");
          previewImg.src = actionURL;
          previewImg.alt = title || "Image Preview";
          previewImg.style.maxWidth = "96%";
          previewImg.style.maxHeight = "96%";
          previewImg.style.objectFit = "contain";
          previewImg.style.borderRadius = "8px";
          previewImg.style.boxShadow = "0 12px 36px rgba(0,0,0,0.6)";
          win.document.body.append(previewImg);
        }
      }
    });
  }
  actions.append(openLink);
  }

  if (type === "video") {
    const reloadBtn = document.createElement("button");
    reloadBtn.type = "button";
    reloadBtn.className = "btn-media-action";
    reloadBtn.title = "重新加载视频播放";
    reloadBtn.textContent = "重载 🔄";
    reloadBtn.addEventListener("click", () => {
      const vid = figure.querySelector("video");
      if (vid && reloadVideo) {
        reloadVideo();
      }
    });
    actions.append(reloadBtn);
  }

  if (hasPublicURL && type === "image" && !isDataUri && (actionURL.startsWith("http://") || actionURL.startsWith("https://"))) {
    const isInternalMedia = (() => {
      try {
        if (actionURL.startsWith("/")) return true;
        const origin = typeof window !== "undefined" && window.location ? window.location.origin : "http://localhost:8000";
        const parsed = new URL(actionURL, origin);
        return parsed.pathname.startsWith("/v1/media/") ||
               parsed.pathname.startsWith("/api/media-assets/") ||
               (typeof window !== "undefined" && window.location && parsed.origin === window.location.origin);
      } catch {
        return false;
      }
    })();
    const proxyLink = document.createElement("a");
    proxyLink.href = isInternalMedia ? actionURL : `/api/media-proxy?url=${encodeURIComponent(actionURL)}`;
    proxyLink.target = "_blank";
    proxyLink.rel = "noreferrer noopener";
    proxyLink.className = "btn-media-action";
    proxyLink.title = isInternalMedia ? "直接打开网关托管图片" : "通过网关代理服务器打开";
    proxyLink.textContent = "代理直连 ↗";
    actions.append(proxyLink);
  }

  if (hasPublicURL) {
  const dlLink = document.createElement("a");
  dlLink.href = actionURL;
  let ext = type === "video" ? "mp4" : "png";
  if (type === "image") {
    if (actionURL.includes("image/jpeg") || actionURL.includes(".jpg") || actionURL.includes(".jpeg")) ext = "jpg";
    else if (actionURL.includes("image/webp") || actionURL.includes(".webp")) ext = "webp";
  }
  let downloadFilename = `${title.replace(/\s+/g, "_")}.${ext}`;
  if (type === "video") {
    const match = actionURL.match(/(?:video-content|videos|media-assets)\/([^/?#]+)/i);
    if (match && match[1]) {
      const cleanID = match[1].replace(/\.mp4$/i, "").replace(/\/content$/i, "");
      if (cleanID && cleanID !== "content") {
        downloadFilename = `${cleanID}.mp4`;
      }
    }
  }
  dlLink.download = downloadFilename;
  dlLink.className = "btn-media-action";
  dlLink.title = "下载媒体文件到本地";
  dlLink.textContent = "下载 💾";
  actions.append(dlLink);

  const copyBtn = document.createElement("button");
  copyBtn.type = "button";
  copyBtn.className = "btn-media-action";
  copyBtn.title = "复制直链到剪贴板";
  copyBtn.textContent = "复制链接";
  copyBtn.addEventListener("click", () => {
    let resolvedURL = actionURL;
    try {
      resolvedURL = new URL(actionURL, window.location.href).href;
    } catch (_) {}
    navigator.clipboard.writeText(resolvedURL).then(() => {
      copyBtn.textContent = "已复制 ✓";
      setTimeout(() => { copyBtn.textContent = "复制链接"; }, 2000);
    }).catch(() => {
      copyBtn.textContent = "复制失败";
    });
  });
  actions.append(copyBtn);
  }

  caption.append(actions);
  figure.append(caption);
  return figure;
}

function renderPlaygroundImages(urls) {
  const media = byId("playground-media");
  if (!media) return;
  media.replaceChildren();
  urls.forEach((url, index) => {
    media.append(createMediaFigure(url, `图片 ${index + 1}`, "image"));
  });
  media.classList.toggle("hidden", urls.length === 0);
}

function renderPlaygroundVideo(url) {
  const media = byId("playground-media");
  if (!media) return;
  media.replaceChildren();
  media.append(createMediaFigure(url, "生成视频播放预览", "video"));
  media.classList.remove("hidden");
}

function extractChatContent(resp) {
  if (!resp) return "";
  let data = resp;
  if (typeof resp === "string") {
    try {
      data = JSON.parse(resp);
    } catch (_) {
      return resp.trim();
    }
  }
  if (!data || typeof data !== "object") return String(data || "").trim();

  // If wrapped in choices (OpenAI)
  if (Array.isArray(data.choices) && data.choices.length > 0) {
    const choice = data.choices[0];
    if (choice.message) {
      let text = "";
      if (choice.message.reasoning_content) {
        text += `💭 思考过程：\n${choice.message.reasoning_content.trim()}\n\n💬 回答：\n`;
      }
      if (typeof choice.message.content === "string") {
        text += choice.message.content.trim();
        return text.trim();
      }
      if (Array.isArray(choice.message.content)) {
        text += choice.message.content.map((part) => part.text || (part.image_url ? `[图片: ${part.image_url.url}]` : "")).join("\n").trim();
        return text.trim();
      }
    }
    if (choice.text) return choice.text.trim();
    if (choice.delta?.content) return choice.delta.content.trim();
  }

  // If Anthropic
  if (Array.isArray(data.content) && data.content.length > 0) {
    return data.content.map((part) => part.text || "").join("\n").trim();
  }

  // If nested response
  if (data.response != null) {
    return extractChatContent(data.response);
  }

  // If error
  if (data.error) {
    if (typeof data.error === "string") return data.error.trim();
    if (data.error.message) return data.error.message.trim();
  }

  if (data.message && typeof data.message === "string") {
    return data.message.trim();
  }

  return typeof resp === "string" ? resp.trim() : JSON.stringify(data, null, 2);
}

function renderPlaygroundChat(userPrompt, result) {
  const container = byId("playground-chat");
  if (!container) return;
  container.replaceChildren();

  // User bubble
  const userBubble = document.createElement("div");
  userBubble.className = "chat-bubble user";
  const userMeta = document.createElement("div");
  userMeta.className = "bubble-meta";
  userMeta.textContent = "用户 Prompt";
  const userBody = document.createElement("div");
  userBody.className = "bubble-body";
  userBody.textContent = userPrompt;
  userBubble.append(userMeta, userBody);

  // Assistant bubble
  const aiBubble = document.createElement("div");
  aiBubble.className = "chat-bubble assistant";
  const aiMeta = document.createElement("div");
  aiMeta.className = "bubble-meta";
  const aiLabel = document.createElement("span");
  aiLabel.textContent = `${result.model || "AI 响应"} · ${result.latency || 0} ms`;
  const copyBtn = document.createElement("button");
  copyBtn.type = "button";
  copyBtn.className = "button ghost";
  copyBtn.style.padding = "2px 8px";
  copyBtn.style.fontSize = "0.74rem";
  copyBtn.textContent = "复制回答";

  const contentText = extractChatContent(result.response);
  copyBtn.addEventListener("click", () => copyText(contentText || ""));
  aiMeta.append(aiLabel, copyBtn);

  const aiBody = document.createElement("div");
  aiBody.className = "bubble-body";
  aiBody.textContent = contentText || (typeof result.response === "string" ? result.response : JSON.stringify(result.response, null, 2));

  aiBubble.append(aiMeta, aiBody);
  container.append(userBubble, aiBubble);
  container.classList.remove("hidden");
}

const delay = (milliseconds) => new Promise((resolve) => setTimeout(resolve, milliseconds));

async function pollPlaygroundImage(initial, pollToken) {
  const taskID = initial.task_id;
  const channelID = initial.channel;
  if (!taskID) throw new Error("图片任务未返回任务 ID");
  for (let attempt = 0; attempt < 225; attempt += 1) {
    if (pollToken !== playgroundPollToken) return;
    const query = new URLSearchParams({ task_id: taskID, channel_id: channelID || "" });
    const result = await request(`/api/playground/image-status?${query}`);
    if (pollToken !== playgroundPollToken) return;
    if (result.status !== "ok") throw new Error(result.error || "图片状态查询失败");
    const taskStatus = result.task_status || "queued";
    byId("playground-status").textContent = taskStatus === "processing" ? "图片生成中…" : `图片任务 ${taskStatus}`;
    byId("playground-result").textContent = JSON.stringify(result, null, 2);
    const images = Array.isArray(result.images) ? result.images.filter(Boolean) : [];
    if (images.length > 0) {
      renderPlaygroundImages(images);
      if (taskStatus !== "completed") {
        byId("playground-status").textContent = "图片已生成，本地托管处理中…";
      }
    }
    if (taskStatus === "completed") {
      if (images.length === 0) throw new Error("图片任务已完成，但上游没有返回可用图片");
      byId("playground-status").textContent = "图片生成完成";
      return;
    }
    if (taskStatus === "failed") throw new Error(result.error || "图片生成失败");
    await delay(4000);
  }
  throw new Error("图片任务等待超时");
}

async function pollPlaygroundVideo(initial, pollToken) {
  const taskID = initial.task_id;
  const channelID = initial.channel;
  if (!taskID) throw new Error("视频任务未返回任务 ID");
  for (let attempt = 0; attempt < 225; attempt += 1) {
    if (pollToken !== playgroundPollToken) return;
    await delay(4000);
    const query = new URLSearchParams({ task_id: taskID, channel_id: channelID || "" });
    const result = await request(`/api/playground/video-status?${query}`);
    if (result.status !== "ok") throw new Error(result.error || "视频状态查询失败");
    const progress = result.progress == null ? "" : ` · ${Math.round(Number(result.progress))}%`;
    byId("playground-status").textContent = `视频任务 ${result.task_status}${progress}`;
    byId("playground-result").textContent = JSON.stringify(result, null, 2);
    if (result.task_status === "completed") {
      const videoURL = result.video_url || result.url || (Array.isArray(result.data) && result.data[0]?.url);
      if (videoURL) renderPlaygroundVideo(videoURL);
      byId("playground-status").textContent = "视频生成完成";
      return;
    }
    if (result.task_status === "failed") {
      const failMsg = typeof result.error === "string" ? result.error : (result.error?.message || result.upstream_body || "视频生成失败");
      throw new Error(failMsg);
    }
  }
  throw new Error("视频任务等待超时");
}

function bindPlayground() {
  document.querySelectorAll("[data-playground-kind]").forEach((button) => {
    button.addEventListener("click", () => setPlaygroundKind(button.dataset.playgroundKind));
  });
  byId("playground-form").elements.channel_id.addEventListener("change", updatePlaygroundModels);

  const previewTab = byId("canvas-toggle-preview");
  const jsonTab = byId("canvas-toggle-json");
  const previewPane = byId("playground-preview-pane");
  const jsonPane = byId("playground-json-pane");
  const placeholder = byId("playground-placeholder");

  let activeOutputView = "preview";

  const setOutputView = (view) => {
    activeOutputView = view;
    const isPreview = view === "preview";
    previewTab?.classList.toggle("active", isPreview);
    previewTab?.setAttribute("aria-selected", String(isPreview));
    jsonTab?.classList.toggle("active", !isPreview);
    jsonTab?.setAttribute("aria-selected", String(!isPreview));

    const isPlaceholderVisible = placeholder && !placeholder.classList.contains("hidden");

    if (isPreview) {
      jsonPane?.classList.add("hidden");
      if (!isPlaceholderVisible) {
        previewPane?.classList.remove("hidden");
      }
    } else {
      placeholder?.classList.add("hidden");
      previewPane?.classList.add("hidden");
      jsonPane?.classList.remove("hidden");
    }
  };

  previewTab?.addEventListener("click", () => setOutputView("preview"));
  jsonTab?.addEventListener("click", () => setOutputView("json"));

  const setCanvasStatus = (state) => {
    const line = byId("canvas-status-line");
    if (line) line.className = `canvas-status-line ${state}`;
    const statusEl = byId("playground-status");
    if (statusEl) {
      statusEl.className = `run-status ${state}`;
    }
  };

  const modelSelect = byId("playground-model-select");
  const modelInput = byId("playground-model-input");
  if (modelSelect && modelInput) {
    modelSelect.addEventListener("change", () => {
      if (modelSelect.value === "__custom__") {
        modelInput.classList.remove("hidden");
        modelInput.value = "";
        modelInput.placeholder = "输入自定义模型名";
        modelInput.focus();
      } else {
        modelInput.classList.add("hidden");
        modelInput.value = modelSelect.value;
      }
    });
  }
  const sizeSelect = byId("playground-image-size-select");
  const sizeInput = byId("playground-image-size-input");
  if (sizeSelect && sizeInput) {
    sizeSelect.addEventListener("change", () => {
      if (sizeSelect.value === "__custom__") {
        sizeInput.classList.remove("hidden");
        sizeInput.value = "";
        sizeInput.focus();
      } else {
        sizeInput.classList.add("hidden");
        sizeInput.value = sizeSelect.value;
      }
    });
    sizeInput.addEventListener("input", () => {
      const match = Array.from(sizeSelect.options).find((opt) => opt.value === sizeInput.value);
      if (match) {
        sizeSelect.value = match.value;
      } else {
        sizeSelect.value = "__custom__";
      }
    });
  }
  byId("playground-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const button = form.querySelector("button[type=submit]");
    const data = formObject(form);
    const payload = {
      kind: data.kind,
      channel_id: data.channel_id,
      model: data.model,
      prompt: data.prompt
    };
    if (data.kind === "image") {
      Object.assign(payload, {
        n: Number(data.image_n || 1),
        size: data.image_size,
        quality: data.image_quality,
        output_format: data.image_output_format,
        reference_urls: parseLines(data.image_reference_urls)
      });
    }
    if (data.kind === "video") {
      Object.assign(payload, {
        duration: Number(data.video_duration || 6),
        aspect_ratio: data.video_aspect_ratio,
        resolution: data.video_resolution,
        reference_image: data.video_reference_image
      });
    }
    const currentPoll = ++playgroundPollToken;
    clearPlaygroundMedia();
    setBusy(button, true);
    setCanvasStatus("running");
    placeholder?.classList.add("hidden");
    if (activeOutputView === "preview") {
      previewPane?.classList.remove("hidden");
      jsonPane?.classList.add("hidden");
    } else {
      previewPane?.classList.add("hidden");
      jsonPane?.classList.remove("hidden");
    }
    byId("playground-status").textContent = data.kind === "video" ? "正在提交视频任务…" : data.kind === "image" ? "正在提交图片任务…" : "请求处理中…";
    byId("playground-result").textContent = "请求中…";
    try {
      const result = await request("/api/playground/run", { method: "POST", body: payload });
      if (result.status !== "ok") {
        const error = new Error(result.error || result.message || "测试失败");
        error.providerMessage = result.provider_message;
        throw error;
      }
      byId("playground-result").textContent = JSON.stringify(result, null, 2);
      if (result.type === "image_task") {
        await pollPlaygroundImage(result, currentPoll);
        setCanvasStatus("success");
      } else if (result.type === "image") {
        renderPlaygroundImages(result.images || []);
        byId("playground-status").textContent = `图片生成完成 · ${result.latency || 0} ms`;
        setCanvasStatus("success");
      } else if (result.type === "video_task") {
        await pollPlaygroundVideo(result, currentPoll);
        setCanvasStatus("success");
      } else {
        renderPlaygroundChat(data.prompt, result);
        byId("playground-status").textContent = `请求完成 · ${result.latency || 0} ms`;
        setCanvasStatus("success");
      }
    } catch (err) {
      if (currentPoll === playgroundPollToken) {
        setCanvasStatus("error");
        byId("playground-status").textContent = err.providerMessage ? `测试失败：${err.providerMessage}` : "测试失败";
        byId("playground-result").textContent = err.message;
        placeholder?.classList.add("hidden");
        setOutputView("json");
      }
    } finally {
      if (currentPoll === playgroundPollToken) setBusy(button, false);
    }
  });
  setPlaygroundKind("chat");
}

async function initDashboard() {
  const me = await loadIdentity();
  bindLogout();
  bindUserMenu();
  await Promise.all([loadAdapterTypes(), loadChannelProfiles(), loadChannels(), loadSettings()]);
  if (me.gateway_token?.configured) {
    byId("stat-token").textContent = `${me.gateway_token.prefix}…`;
    byId("stat-token-container")?.classList.add("configured");
  }
  bindChannelForm();
  bindSettings();
  bindPlayground();
  enhanceAllSelects();
  bindMobileNav();

  document.addEventListener("click", () => {
    document.querySelectorAll(".channel-menu-popover").forEach((m) => m.classList.add("hidden"));
  });
}

function bindMobileNav() {
  const mobileToggle = byId("mobile-nav-toggle");
  const sidebar = byId("app-sidebar");
  const backdrop = byId("sidebar-backdrop");
  if (mobileToggle && sidebar && backdrop) {
    mobileToggle.addEventListener("click", () => {
      const open = sidebar.classList.toggle("mobile-open");
      backdrop.classList.toggle("active", open);
    });
    backdrop.addEventListener("click", () => {
      sidebar.classList.remove("mobile-open");
      backdrop.classList.remove("active");
    });
  }
}

async function initProfiles() {
  await loadIdentity();
  bindLogout();
  bindMobileNav();
  await Promise.all([loadChannels(), loadProfiles()]);
  bindProfileManager();
  enhanceAllSelects();
}

let logPage = 1;
let logTotal = 0;
const logPageSize = 50;

function outcomeLabel(value) {
  return ({
    success: "成功",
    error: "错误",
    cancelled: "已取消",
    interrupted: "已中断",
    running: "处理中"
  })[value] || value;
}

let activeLogRecord = null;
let activeLogID = "";
let activeLogRequestToken = 0;
let activeLogRefreshTimer = null;
let activeLogRefreshFailures = 0;
let activeLogRefreshDisabled = false;
const logDetailRefreshMs = 4000;
const logDetailMaxRetryMs = 30000;

function formatJSON(value) {
  if (!value) return "无";
  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    const formatted = JSON.stringify(parsed, null, 2);
    if (formatted.length > 200000) {
      return formatted.slice(0, 200000) + "\n\n/* 内容过长（已截取前 200 KB 显示），已自动截断以保护浏览器流畅运行。完整内容可通过接口获取。 */";
    }
    return formatted;
  } catch (_) {
    const str = String(value);
    if (str.length > 200000) {
      return str.slice(0, 200000) + "\n\n/* 内容过长，已自动截断 */";
    }
    return str;
  }
}

function extractPromptSnippet(bodyStr) {
  if (!bodyStr) return "";
  try {
    const data = typeof bodyStr === "string" ? JSON.parse(bodyStr) : bodyStr;
    let snippet = "";
    if (data.prompt && typeof data.prompt === "string") snippet = data.prompt.trim();
    else if (Array.isArray(data.messages) && data.messages.length > 0) {
      for (let i = data.messages.length - 1; i >= 0; i--) {
        const msg = data.messages[i];
        if (msg && msg.content) {
          if (typeof msg.content === "string") { snippet = msg.content.trim(); break; }
          if (Array.isArray(msg.content)) {
            const textPart = msg.content.find((p) => p.text);
            if (textPart) { snippet = textPart.text.trim(); break; }
          }
        }
      }
    } else if (data.input) {
      snippet = typeof data.input === "string" ? data.input.trim() : JSON.stringify(data.input);
    }
    if (snippet && snippet.length > 200) {
      return snippet.slice(0, 200) + "…";
    }
    return snippet;
  } catch (_) {}
  return "";
}

function extractFullPrompt(bodyStr) {
  if (!bodyStr) return "";
  try {
    const data = typeof bodyStr === "string" ? JSON.parse(bodyStr) : bodyStr;
    if (data.prompt && typeof data.prompt === "string") return data.prompt.trim();
    if (Array.isArray(data.messages) && data.messages.length > 0) {
      if (data.messages.length === 1) {
        const m = data.messages[0];
        if (typeof m.content === "string") return m.content.trim();
        if (Array.isArray(m.content)) return m.content.map((p) => p.text || "").join("\n").trim();
      }
      return data.messages
        .map((m) => {
          const role = (m.role || "user").toUpperCase();
          const content = typeof m.content === "string" ? m.content.trim() : Array.isArray(m.content) ? m.content.map((p) => p.text || "").join("\n").trim() : JSON.stringify(m.content, null, 2);
          return `【${role}】\n${content}`;
        })
        .join("\n\n");
    }
    if (data.input) {
      return typeof data.input === "string" ? data.input.trim() : JSON.stringify(data.input, null, 2);
    }
  } catch (_) {}
  return String(bodyStr || "");
}

function extractInnerResponseContent(data) {
  if (!data) return "";
  if (typeof data === "string") {
    try {
      const parsed = JSON.parse(data);
      const inner = extractInnerResponseContent(parsed);
      if (inner) return inner;
    } catch (_) {
      return data.trim();
    }
  }

  // 1. If wrapped in playground format or upstream response field
  if (data.response != null) {
    if (typeof data.response === "string" && data.response.trim()) {
      try {
        const parsedResp = JSON.parse(data.response);
        const inner = extractInnerResponseContent(parsedResp);
        if (inner) return inner;
      } catch (_) {
        return data.response.trim();
      }
    }
    if (typeof data.response === "object") {
      const inner = extractInnerResponseContent(data.response);
      if (inner) return inner;
    }
  }

  // 2. OpenAI format (choices)
  if (Array.isArray(data.choices) && data.choices.length > 0) {
    const choice = data.choices[0];
    if (choice.message) {
      let resultText = "";
      if (choice.message.reasoning_content) {
        resultText += `💭 思考过程：\n${choice.message.reasoning_content.trim()}\n\n💬 回答：\n`;
      }
      if (typeof choice.message.content === "string") {
        resultText += choice.message.content.trim();
        return resultText.trim();
      }
      if (Array.isArray(choice.message.content)) {
        resultText += choice.message.content.map((p) => p.text || (p.image_url ? `[图片: ${p.image_url.url}]` : "")).join("\n").trim();
        return resultText.trim();
      }
    }
    if (choice.text) return choice.text.trim();
    if (choice.delta?.content) return choice.delta.content.trim();
  }

  // 3. Anthropic format (content)
  if (Array.isArray(data.content) && data.content.length > 0) {
    return data.content.map((p) => p.text || "").join("\n").trim();
  }

  // 4. Video task status
  if (data.type === "video_task" || data.task_id || (data.id && (data.object === "video" || data.status === "queued" || data.status === "processing" || data.status === "completed" || data.status === "succeeded"))) {
    const tid = data.task_id || data.id;
    const parts = [];
    parts.push(`🎬 视频生成任务: ${data.task_status || data.status || "处理中"}`);
    if (tid) parts.push(`任务 ID: ${tid}`);
    if (data.progress != null) parts.push(`进度: ${Math.round(data.progress)}%`);
    if (data.video_url) parts.push(`播放地址: ${data.video_url}`);
    return parts.join(" · ");
  }

  // 5. Image generation
  const imgCount = Array.isArray(data.data) ? data.data.length : Array.isArray(data.images) ? data.images.length : 0;
  if (imgCount > 0) {
    return `图片生成成功，共 ${imgCount} 张（已在下方直观画廊中呈现）`;
  }

  // 6. Error object
  if (data.error) {
    if (typeof data.error === "string") return data.error.trim();
    if (data.error.message) return data.error.message.trim();
    return JSON.stringify(data.error, null, 2);
  }

  // 7. Generic message
  if (data.message && typeof data.message === "string") {
    return data.message.trim();
  }

  return "";
}

function extractResponseText(row) {
  const responseBody = row?.async_result_body || row?.response_body || "";
  // Priority 1: Reconstructed complete stream text
  if (row.stream_text && row.stream_text.trim()) {
    return row.stream_text.trim();
  }

  // Priority 2: Structured response body
  if (responseBody && responseBody.trim()) {
    try {
      const data = JSON.parse(responseBody);
      const extracted = extractInnerResponseContent(data);
      if (extracted) return extracted;
    } catch (_) {
      return responseBody.trim();
    }
  }

  // Priority 3: Error message
  if (row.error_message && row.error_message.trim()) {
    return `错误: ${row.error_message.trim()}`;
  }

  return "(上游未返回任何文本内容)";
}

function formatBase64DataUrl(b64) {
  if (!b64 || typeof b64 !== "string") return "";
  if (b64.startsWith("data:image/")) return b64;
  if (b64.startsWith("/9j/")) return `data:image/jpeg;base64,${b64}`;
  if (b64.startsWith("iVBORw0KGgo")) return `data:image/png;base64,${b64}`;
  if (b64.startsWith("UklGR")) return `data:image/webp;base64,${b64}`;
  if (b64.startsWith("R0lGOD")) return `data:image/gif;base64,${b64}`;
  return `data:image/png;base64,${b64}`;
}

function isVideoResponsePayload(data, row) {
  const taskKind = String(row?.async_task_kind || "").trim().toLowerCase();
  if (taskKind === "video") return true;

  const path = String(row?.path || "").trim().toLowerCase();
  if (path.includes("/video")) return true;

  const type = String(data?.type || data?.object || "").trim().toLowerCase();
  if (type.includes("video")) return true;
  return typeof data?.video_url === "string" && data.video_url.trim() !== "";
}

function logResponsePayloads(responseBody) {
  if (!responseBody) return [];
  let root = responseBody;
  if (typeof root === "string") {
    try {
      root = JSON.parse(root);
    } catch (_) {
      return [];
    }
  }
  const payloads = [];
  const pending = [{ value: root, depth: 0 }];
  const seen = new Set();
  while (pending.length > 0) {
    const { value, depth } = pending.shift();
    if (!value || typeof value !== "object" || seen.has(value)) continue;
    seen.add(value);
    payloads.push(value);
    if (depth >= 4 || Array.isArray(value)) continue;
    ["response", "raw", "raw_payload"].forEach((key) => {
      let nested = value[key];
      if (typeof nested === "string" && /^[\s]*[\[{]/.test(nested)) {
        try {
          nested = JSON.parse(nested);
        } catch (_) {
          nested = null;
        }
      }
      if (nested && typeof nested === "object") {
        pending.push({ value: nested, depth: depth + 1 });
      }
    });
  }
  return payloads;
}

function isUsableLogMediaSource(raw) {
  if (typeof raw !== "string" || !raw.trim()) return false;
  const value = raw.trim();
  return !/\[REDACTED\]|%5BREDACTED%5D/i.test(value);
}

function extractImagesFromResponse(responseBody, row = null) {
  const payloads = logResponsePayloads(responseBody);
  // Video responses commonly expose their playable URL in data[].url as
  // well as video_url. Do not render that same MP4 endpoint as a failed
  // image card before the dedicated video renderer handles it.
  if (payloads.some((data) => isVideoResponsePayload(data, row))) return [];
  const urls = [];
  const seen = new Set();
  const appendURL = (raw) => {
    if (!isUsableLogMediaSource(raw)) return;
    const value = raw.trim();
    if (/\.(?:mp4|webm|mov|m4v)(?:[?#]|$)/i.test(value) || value.includes("/video-content/")) return;
    if (seen.has(value)) return;
    seen.add(value);
    urls.push(value);
  };
  payloads.forEach((data) => {
    if (!data || Array.isArray(data)) return;
    if (Array.isArray(data.data)) {
      data.data.forEach((item) => {
        if (typeof item === "string") {
          appendURL(item);
        } else if (item?.url) {
          appendURL(item.url);
        } else if (item.b64_json && !item.b64_json.startsWith("[BASE64")) {
          appendURL(formatBase64DataUrl(item.b64_json));
        }
      });
    }
    if (Array.isArray(data.images)) {
      data.images.forEach((item) => {
        if (typeof item === "string") appendURL(item);
        else if (item?.url) appendURL(item.url);
      });
    }
    const assets = Array.isArray(data.assets) ? data.assets :
      (Array.isArray(data.job?.assets) ? data.job.assets : []);
    assets.forEach((asset) => {
      if (!asset || typeof asset !== "object") return;
      const url = asset.url || asset.proxy_url || asset.thumbnail_url;
      appendURL(url);
    });
    appendURL(data.image_url);
  });
  return urls;
}

function extractVideoFromResponse(responseBody, row) {
  const payloads = logResponsePayloads(responseBody);
  if (payloads.length === 0) return null;
  const isVideo = payloads.some((data) => isVideoResponsePayload(data, row)) ||
                  (row && ((row.target_model || "") + (row.requested_model || "")).toLowerCase().includes("video")) ||
                  (row && row.request_body && row.request_body.includes('"video"'));
  const candidates = [];
  payloads.forEach((data) => {
    if (!data || Array.isArray(data)) return;
    // Providers use several equivalent shapes for a completed video. Check
    // nested data/assets entries as well as the top-level URL so Profile and
    // legacy logs render the same playable resource.
    candidates.push(data.video_url, data.url);
    const arrays = [data.data, data.assets, data.job?.assets];
    arrays.forEach((items) => {
      if (!Array.isArray(items)) return;
      items.forEach((item) => {
        if (!item || typeof item !== "object") return;
        candidates.push(item.video_url, item.url, item.proxy_url);
      });
    });
  });
  for (const candidate of candidates) {
    if (!isUsableLogMediaSource(candidate)) continue;
    const value = candidate.trim();
    if (isVideo || /\.(?:mp4|webm|mov|m4v)(?:[?#]|$)/i.test(value) || value.includes("/video-content/") || value.endsWith("/content")) {
      return normalizeLogMediaURL(value);
    }
  }

  if (isVideo) {
    // Support async video tasks where task_id or id is returned.
    const taskPayload = payloads.find((data) => !Array.isArray(data) && (data.task_id || data.id));
    const taskId = taskPayload?.task_id || taskPayload?.id;
    if (taskId) {
      // Keep this as a same-origin gateway URL. Older audit rows stored an
      // absolute localhost URL, which breaks when the console is opened via a
      // different host or reverse proxy.
      return `/api/playground/video-content/${encodeURIComponent(taskId)}.mp4`;
    }
  }
  return null;
}

// Normalize gateway-generated media links kept in historical audit rows to a
// relative URL. Provider CDN URLs remain untouched; only known gateway media
// endpoints are rewritten so browser playback follows the current console
// origin and session.
function normalizeLogMediaURL(value) {
  if (typeof value !== "string") return value;
  const raw = value.trim();
  if (!raw) return raw;
  try {
    const parsed = new URL(raw, "http://gateway.invalid/");
    const path = parsed.pathname || "";
    const isGatewayMediaPath = path.startsWith("/api/playground/video-content/") ||
      (path.startsWith("/api/media-assets/") && (path.endsWith("/content") || path.endsWith("/content.mp4"))) ||
      (path.startsWith("/v1/videos/") && (path.endsWith("/content") || path.endsWith("/content.mp4")));
    if (!isGatewayMediaPath) return raw;
    return `${path}${parsed.search || ""}${parsed.hash || ""}`;
  } catch (_) {
    return raw;
  }
}

function managedLogMediaURLs(assets, kind) {
  if (!Array.isArray(assets)) return [];
  return assets
    .filter((asset) => String(asset?.kind || "").toLowerCase() === kind &&
      String(asset?.status || "").toLowerCase() === "available" &&
      Number(asset?.id) > 0)
    .sort((a, b) => (Number(a.ordinal) || 0) - (Number(b.ordinal) || 0) || (Number(a.id) || 0) - (Number(b.id) || 0))
    .map((asset) => normalizeMediaURL(asset.public_url || asset.publicUrl || asset.public_link || asset.publicLink, kind))
    .filter(Boolean);
}

function managedLogMediaAssets(assets, kind) {
  if (!Array.isArray(assets)) return [];
  return assets
    .filter((asset) => String(asset?.kind || "").toLowerCase() === kind &&
      String(asset?.status || "").toLowerCase() === "available" &&
      Number(asset?.id) > 0)
    .sort((a, b) => (Number(a.ordinal) || 0) - (Number(b.ordinal) || 0) || (Number(a.id) || 0) - (Number(b.id) || 0));
}

function isMediaResultLog(row) {
  const taskKind = String(row?.async_task_kind || "").trim().toLowerCase();
  if (taskKind === "image" || taskKind === "video") return true;
  const path = String(row?.path || "").trim().toLowerCase();
  return path.includes("/images") || path.includes("/videos") || path === "/api/playground/run";
}

function generateCurlCommand(row) {
  if (!row) return "";
  const lines = [`curl -X ${row.method || "POST"} "${location.origin}${row.path || "/v1/chat/completions"}"`];
  lines.push(`  -H "Authorization: Bearer <YOUR_GATEWAY_KEY>"`);
  lines.push(`  -H "Content-Type: application/json"`);
  if (row.request_body) {
    lines.push(`  -d '${row.request_body.replace(/'/g, "'\\''")}'`);
  }
  return lines.join(" \\\n");
}

function setLogInspectorTab(name) {
  document.querySelectorAll("[data-log-tab]").forEach((tab) => {
    const active = tab.dataset.logTab === name;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  });
  document.querySelectorAll("[data-log-panel]").forEach((panel) => {
    panel.classList.toggle("hidden", panel.dataset.logPanel !== name);
  });
}

function getDisplayModel(row) {
  if (!row) return "无模型";

  const req = (row.requested_model || "").trim();
  const tgt = (row.target_model || "").trim();

  if (req && tgt && req !== tgt) {
    return `${req} → ${tgt}`;
  }
  if (tgt) return tgt;
  if (req) return req;

  // Fallback: Check request_body for model
  if (row.request_body) {
    try {
      const b = typeof row.request_body === "string" ? JSON.parse(row.request_body) : row.request_body;
      if (b && typeof b.model === "string" && b.model.trim()) {
        return b.model.trim();
      }
    } catch (_) {}
  }

  // Fallback: Check response_body for model
  if (row.response_body) {
    try {
      const resp = typeof row.response_body === "string" ? JSON.parse(row.response_body) : row.response_body;
      if (resp && typeof resp.model === "string" && resp.model.trim()) {
        return resp.model.trim();
      }
    } catch (_) {}
  }

  if (row.path) {
    if (row.path.includes("/content")) return "媒体内容流";
    if (row.path.startsWith("/api/")) return "控制台操作";
  }

  return "无模型";
}

function asyncTaskKindLabel(kind) {
  switch (String(kind || "").toLowerCase()) {
    case "image": return "异步图片任务";
    case "video": return "异步视频任务";
    default: return "异步任务";
  }
}

function asyncTaskStatusLabel(status) {
  switch (String(status || "").toLowerCase()) {
    case "queued": case "pending": return "排队中";
    case "running": case "processing": case "in_progress": return "处理中";
    case "completed": case "succeeded": case "success": return "已完成";
    case "failed": case "error": return "失败";
    case "cancelled": case "canceled": return "已取消";
    default: return status || "处理中";
  }
}

function normalizedAsyncTaskStatus(status) {
  const value = String(status || "").trim().toLowerCase();
  if (["success", "succeeded", "complete", "completed", "done"].includes(value)) return "completed";
  if (["error", "errored", "failed", "cancelled", "canceled", "rejected", "expired"].includes(value)) return "failed";
  if (["pending", "waiting", "in_progress", "in-progress", "running"].includes(value)) return "processing";
  return value;
}

function asyncTaskProgress(row) {
  if (!row?.async_result_body) return null;
  try {
    const data = typeof row.async_result_body === "string" ? JSON.parse(row.async_result_body) : row.async_result_body;
    const value = data?.progress ?? data?.response?.progress ?? data?.job?.progress;
    const progress = Number(value);
    return Number.isFinite(progress) ? Math.max(0, Math.min(100, Math.round(progress))) : null;
  } catch (_) {
    return null;
  }
}

function asyncTaskSummaryText(row) {
  const progress = asyncTaskProgress(row);
  const progressText = progress == null ? "" : ` · ${progress}%`;
  return `⏳ ${asyncTaskKindLabel(row.async_task_kind)} · ${asyncTaskStatusLabel(row.async_task_status)}${progressText} · 轮询 ${Number(row.async_poll_count) || 0} 次`;
}

function shouldRefreshAsyncTask(row, detail) {
  if (!row?.async_task_kind) return false;
  if (detail?.task_run && ["completed", "failed", "cancelled", "canceled"].includes(normalizedAsyncTaskStatus(detail.task_run.task_status))) {
    return false;
  }
  if (Array.isArray(detail?.media_assets) && detail.media_assets.some((a) => a.kind === row.async_task_kind && a.status === "available")) {
    return false;
  }
  if (["cancelled", "canceled"].includes(normalizedAsyncTaskStatus(row.async_task_status))) {
    return false;
  }
  return !["completed", "failed"].includes(normalizedAsyncTaskStatus(row.async_task_status));
}

function stopActiveLogRefresh() {
  if (activeLogRefreshTimer != null) {
    clearTimeout(activeLogRefreshTimer);
    activeLogRefreshTimer = null;
  }
}

// Log inspection follows status already persisted by explicit task-status
// requests. Merely viewing a log must never contact an upstream provider.
function scheduleActiveLogRefresh(id, token, delayMs = logDetailRefreshMs) {
  stopActiveLogRefresh();
  if (activeLogRefreshDisabled || document.visibilityState === "hidden") return;
  activeLogRefreshTimer = setTimeout(() => {
    activeLogRefreshTimer = null;
    openLog(id, { background: true, token });
  }, delayMs);
}

function isPermanentLogRefreshError(error) {
  return [401, 403, 404, 410].includes(Number(error?.status));
}

function handleActiveLogVisibilityChange() {
  if (document.visibilityState === "hidden") {
    stopActiveLogRefresh();
    return;
  }
  if (!activeLogRefreshDisabled && activeLogRecord && shouldRefreshAsyncTask(activeLogRecord)) {
    scheduleActiveLogRefresh(activeLogID, activeLogRequestToken, 0);
  }
}

function refreshActiveLogListItem(row) {
  const item = Array.from(document.querySelectorAll("#log-list .log-item-card"))
    .find((candidate) => candidate.dataset.logId === row?.id);
  const taskTag = item?.querySelector("[data-async-task-summary]");
  if (taskTag && row?.async_task_kind) {
    taskTag.textContent = asyncTaskSummaryText(row);
  }
}

function asyncResultBody(row) {
  return row?.async_result_body || row?.response_body || "";
}

function renderLogs(rows) {
  const container = byId("log-list");
  container.replaceChildren();
  byId("logs-empty").classList.toggle("hidden", rows.length !== 0);

  rows.forEach((row) => {
    const item = document.createElement("div");
    item.className = "log-item-card";
    item.tabIndex = 0;
    item.dataset.logId = row.id;

    item.addEventListener("click", () => {
      document.querySelectorAll("#log-list .log-item-card").forEach((r) => r.classList.remove("active-row"));
      item.classList.add("active-row");
      openLog(row.id);
    });
    item.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        document.querySelectorAll("#log-list .log-item-card").forEach((r) => r.classList.remove("active-row"));
        item.classList.add("active-row");
        openLog(row.id);
      }
    });

    // Line 1: Header (Status + Method + Path + Duration + Time)
    const header = document.createElement("div");
    header.className = "log-item-header";

    const leftHeader = document.createElement("div");
    leftHeader.className = "log-item-left";

    const statusBadge = document.createElement("span");
    const outcomeCls = (row.outcome || "unknown").toLowerCase();
    statusBadge.className = `log-status-pill ${outcomeCls}`;
    statusBadge.textContent = `${row.status_code || (row.outcome === "success" ? "200" : "ERR")} ${outcomeLabel(row.outcome)}`;

    const methodSpan = document.createElement("span");
    const m = (row.method || "POST").toLowerCase();
    methodSpan.className = `log-method-pill ${m}`;
    methodSpan.textContent = row.method || "POST";

    const pathSpan = document.createElement("span");
    pathSpan.className = "log-path-text";
    pathSpan.textContent = row.path || "/";
    pathSpan.title = row.path || "/";

    leftHeader.append(statusBadge, methodSpan, pathSpan);

    const rightMeta = document.createElement("div");
    rightMeta.className = "log-item-meta-right";

    const durSpan = document.createElement("span");
    durSpan.className = "log-duration";
    durSpan.textContent = `${row.duration_ms || 0} ms`;

    const timeSpan = document.createElement("span");
    timeSpan.className = "log-time";
    timeSpan.textContent = new Date(row.started_at).toLocaleTimeString();

    rightMeta.append(durSpan, timeSpan);
    header.append(leftHeader, rightMeta);
    item.append(header);

    // Line 2: Tags (Model + Channel + Tokens + Stream)
    const tagsRow = document.createElement("div");
    tagsRow.className = "log-item-tags";

    const modelName = getDisplayModel(row);
    const modelTag = document.createElement("span");
    modelTag.className = "log-tag model-tag";
    modelTag.textContent = `🤖 ${modelName}`;
    tagsRow.append(modelTag);

    const chanName = row.channel_name || row.channel_id;
    if (chanName) {
      const chanTag = document.createElement("span");
      chanTag.className = "log-tag channel-tag";
      chanTag.textContent = `🏷️ ${chanName}`;
      tagsRow.append(chanTag);
    }

    if (row.input_tokens || row.output_tokens) {
      const tokensTag = document.createElement("span");
      tokensTag.className = "log-tag tokens-tag";
      tokensTag.textContent = `⚡ ▲ ${row.input_tokens || 0} · ▼ ${row.output_tokens || 0}`;
      tagsRow.append(tokensTag);
    }

    if (row.is_stream) {
      const streamTag = document.createElement("span");
      streamTag.className = "log-tag stream-tag";
      streamTag.textContent = "🌊 流式";
      tagsRow.append(streamTag);
    }

    if (row.async_task_kind) {
      const taskTag = document.createElement("span");
      taskTag.className = "log-tag tokens-tag";
      taskTag.dataset.asyncTaskSummary = "true";
      taskTag.textContent = asyncTaskSummaryText(row);
      tagsRow.append(taskTag);
    }

    item.append(tagsRow);

    // Line 3: Prompt preview or Error
    const promptSnippet = extractPromptSnippet(row.request_body);
    if (promptSnippet) {
      const snippet = document.createElement("div");
      snippet.className = "log-item-snippet";
      snippet.textContent = `💬 ${promptSnippet}`;
      item.append(snippet);
    }

    if (row.error_message) {
      const errHint = document.createElement("div");
      errHint.className = "log-item-error";
      errHint.textContent = `⚠️ ${row.error_message}`;
      item.append(errHint);
    }

    container.append(item);
  });

  // Dynamically ensure all models & channels present in the rows exist in the filter options
  let hasNewChannel = false;
  rows.forEach((r) => {
    [r.target_model, r.requested_model].forEach((m) => {
      if (m && m.trim() && !allAvailableModels.has(m.trim())) {
        allAvailableModels.add(m.trim());
      }
    });
    if (r.channel_id && !allAvailableChannels.has(r.channel_id)) {
      const name = r.channel_name ? `🏷️ ${r.channel_name}` : `🏷️ ${r.channel_id}`;
      allAvailableChannels.set(r.channel_id, name);
      hasNewChannel = true;
    }
  });
  if (hasNewChannel) {
    renderChannelSelectOptions();
  }

  const pages = Math.max(1, Math.ceil(logTotal / logPageSize));
  byId("page-info").textContent = `第 ${logPage} / ${pages} 页 · 共 ${logTotal} 条`;
  byId("prev-page").disabled = logPage <= 1;
  byId("next-page").disabled = logPage >= pages;

  // Auto-select first row if available
  if (rows.length > 0 && container.firstElementChild) {
    container.firstElementChild.classList.add("active-row");
    openLog(rows[0].id);
  } else {
    activeLogID = "";
    activeLogRequestToken += 1;
    activeLogRecord = null;
    activeLogRefreshFailures = 0;
    activeLogRefreshDisabled = true;
    stopActiveLogRefresh();
    const empty = byId("inspector-empty");
    const content = byId("inspector-content");
    if (empty && content) {
      empty.classList.remove("hidden");
      content.classList.add("hidden");
    }
  }
}

async function loadLogs() {
  const form = byId("log-filters");
  const data = form ? formObject(form) : {};
  const params = new URLSearchParams({ page: String(logPage), page_size: String(logPageSize) });

  // Handle date preset
  const datePreset = byId("date-preset")?.value || "all";
  let fromISO = "";
  let toISO = "";

  if (datePreset === "today") {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date();
    end.setHours(23, 59, 59, 999);
    fromISO = start.toISOString();
    toISO = end.toISOString();
  } else if (datePreset === "yesterday") {
    const start = new Date();
    start.setDate(start.getDate() - 1);
    start.setHours(0, 0, 0, 0);
    const end = new Date();
    end.setDate(end.getDate() - 1);
    end.setHours(23, 59, 59, 999);
    fromISO = start.toISOString();
    toISO = end.toISOString();
  } else if (datePreset === "7d") {
    const start = new Date(Date.now() - 7 * 86400000);
    fromISO = start.toISOString();
  } else if (datePreset === "30d") {
    const start = new Date(Date.now() - 30 * 86400000);
    fromISO = start.toISOString();
  } else if (datePreset === "custom") {
    const f = byId("filter-from-date")?.value;
    const t = byId("filter-to-date")?.value;
    if (f) {
      const d = new Date(f + "T00:00:00");
      if (!Number.isNaN(d.getTime())) fromISO = d.toISOString();
    }
    if (t) {
      const d = new Date(t + "T23:59:59.999");
      if (!Number.isNaN(d.getTime())) toISO = d.toISOString();
    }
  }

  if (fromISO) params.set("from", fromISO);
  if (toISO) params.set("to", toISO);

  // Other curated filters: q, channel_id, model, outcome
  if (data.q && data.q.trim()) params.set("q", data.q.trim());
  if (data.channel_id && data.channel_id.trim()) params.set("channel_id", data.channel_id.trim());
  if (data.model && data.model.trim()) params.set("model", data.model.trim());
  if (data.outcome && data.outcome.trim()) params.set("outcome", data.outcome.trim());

  const result = await request(`/api/logs?${params}`);
  logTotal = result.total || 0;
  renderLogs(result.data || []);
}

function summaryBadge(text) {
  const span = document.createElement("span");
  span.className = "badge";
  span.textContent = text;
  return span;
}

function setLogDetailText(element, value) {
  if (!element) return;
  const text = String(value ?? "");
  if (element.textContent !== text) element.textContent = text;
}

function logDetailRenderKey(value) {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value ?? "");
  }
}

async function openLog(id, options = {}) {
  const background = Boolean(options.background);
  let requestToken = options.token;
  if (background) {
    if (id !== activeLogID || requestToken !== activeLogRequestToken) return;
  } else {
    stopActiveLogRefresh();
    activeLogID = id;
    requestToken = ++activeLogRequestToken;
    activeLogRefreshFailures = 0;
    activeLogRefreshDisabled = false;
  }
  try {
    const detail = await request(`/api/logs/${encodeURIComponent(id)}`);
    if (id !== activeLogID || requestToken !== activeLogRequestToken) return;
    const row = detail.log;
    activeLogRecord = row;
    activeLogRefreshFailures = 0;
    activeLogRefreshDisabled = false;

    const empty = byId("inspector-empty");
    const content = byId("inspector-content");
    if (empty) empty.classList.add("hidden");
    if (content) content.classList.remove("hidden");
    const inspectorPane = byId("logs-inspector");
    if (inspectorPane) inspectorPane.classList.add("mobile-active");

    // Title / ID
    const titleEl = byId("detail-title");
    setLogDetailText(titleEl, row.id);

    // Summary badges
    const summary = byId("detail-summary");
    if (summary) {
      const modelName = getDisplayModel(row);
      const badges = [
        summaryBadge(`${row.method} ${row.path}`),
        summaryBadge(outcomeLabel(row.outcome)),
        summaryBadge(`HTTP ${row.status_code || 0}`),
        summaryBadge(`${row.duration_ms} ms`),
        summaryBadge(row.channel_name || row.channel_id || "未分发"),
        summaryBadge(modelName !== "无模型" ? `模型: ${modelName}` : "无模型")
      ];
      if (row.target_model && row.requested_model && row.target_model !== row.requested_model) {
        badges.push(summaryBadge(`映射目标: ${row.target_model}`));
      }
      if (row.input_tokens || row.output_tokens) {
        badges.push(summaryBadge(`Tokens: 输入 ${row.input_tokens} / 输出 ${row.output_tokens}`));
      }
      if (row.is_stream) {
        badges.push(summaryBadge("SSE 流式"));
      }
      if (row.async_task_kind) {
        const progress = asyncTaskProgress(row);
        badges.push(summaryBadge(asyncTaskKindLabel(row.async_task_kind)));
        badges.push(summaryBadge(`状态: ${asyncTaskStatusLabel(row.async_task_status)}${progress == null ? "" : ` · ${progress}%`}`));
        badges.push(summaryBadge(`轮询: ${Number(row.async_poll_count) || 0} 次`));
        if (row.async_task_id) badges.push(summaryBadge(`任务: ${row.async_task_id}`));
      }
      const summaryKey = logDetailRenderKey(badges.map((badge) => badge.textContent));
      if (summary.dataset.renderKey !== summaryKey) {
        summary.replaceChildren(...badges);
        summary.dataset.renderKey = summaryKey;
      }
    }

    // Tab 1: Visual View
    const errorCard = byId("visual-error-card");
    if (errorCard) {
      const taskError = row.async_task_error || "";
      if (row.outcome === "error" || row.error_message || taskError) {
        errorCard.replaceChildren();
        const errTitle = document.createElement("strong");
        errTitle.textContent = taskError ? "🚨 异步任务失败" : `🚨 请求失败 · HTTP ${row.status_code || "Error"}`;
        const errMsg = document.createElement("div");
        errMsg.textContent = taskError || row.error_message || "上游未返回具体错误文本";
        errorCard.append(errTitle, errMsg);
        errorCard.classList.remove("hidden");
      } else {
        errorCard.classList.add("hidden");
      }
    }

    // Prompt & Response
    const promptBody = byId("visual-prompt-body");
    if (promptBody) {
      const fullPrompt = extractFullPrompt(row.request_body);
      setLogDetailText(promptBody, fullPrompt || "(无文本 Prompt)");
    }

    const responseBody = byId("visual-response-body");
    if (responseBody) {
      const fullResp = extractResponseText(row);
      const taskError = row.async_task_error || "";
      setLogDetailText(responseBody, fullResp || (taskError || row.error_message ? `(错误: ${taskError || row.error_message})` : "(无文本返回)"));
    }

    // Media (Images & Videos)
    const mediaView = byId("visual-media-view");
    if (mediaView) {
      const resultBody = asyncResultBody(row);
      // Prefer gateway-managed objects when the task has required retention.
      // The admin content endpoint supports byte ranges and keeps playback
      // independent of expired provider URLs or provider-specific auth.
      const managedImageAssets = managedLogMediaAssets(detail.media_assets, "image");
      const managedVideoAssets = managedLogMediaAssets(detail.media_assets, "video");
      const managedImages = managedImageAssets.map((asset) => `/api/media-assets/${encodeURIComponent(asset.id)}/content`);
      const managedVideos = managedVideoAssets.map((asset) => `/api/media-assets/${encodeURIComponent(asset.id)}/content`);
      const images = managedImages.length > 0 ? managedImages : extractImagesFromResponse(resultBody, row);
      const videoUrl = managedVideos[0] || (normalizedAsyncTaskStatus(row.async_task_status) === "completed"
        ? extractVideoFromResponse(resultBody, row)
        : null);

      const imageItems = images.map((url, idx) => {
        const asset = managedImageAssets[idx];
        return {
          url,
          title: `图片 ${idx + 1}`,
          options: asset ? {
            publicURL: asset.public_url || asset.publicUrl || asset.public_link || asset.publicLink || null,
          } : {},
        };
      });
      const videoItems = managedVideoAssets.length > 0
        ? managedVideoAssets.map((asset, idx) => ({
            url: `/api/media-assets/${encodeURIComponent(asset.id)}/content`,
            title: managedVideoAssets.length > 1 ? `生成视频 ${idx + 1}` : "生成视频播放预览",
            options: {
              publicURL: asset.public_url || asset.publicUrl || asset.public_link || asset.publicLink || null,
            },
          }))
        : (videoUrl ? [{ url: videoUrl, title: "生成视频播放预览", options: {} }] : []);
      const hasLegacyBase64Notice = imageItems.length === 0 && resultBody.includes("[BASE64");
      const hasMedia = imageItems.length > 0 || videoItems.length > 0 || hasLegacyBase64Notice;
      const collapseStructuredResponse = hasMedia && isMediaResultLog(row) && !String(row.stream_text || "").trim();
      responseBody?.classList.toggle("hidden", collapseStructuredResponse);
      byId("copy-visual-response")?.classList.toggle("hidden", collapseStructuredResponse);
      mediaView.classList.toggle("hidden", !hasMedia);

      const mediaRenderKey = logDetailRenderKey({
        logID: row.id,
        images: imageItems,
        videos: videoItems,
        legacyBase64: hasLegacyBase64Notice,
      });
      if (mediaView.dataset.renderKey !== mediaRenderKey) {
        mediaView.replaceChildren();
        imageItems.forEach((item) => {
          mediaView.append(createMediaFigure(item.url, item.title, "image", item.options));
        });
        if (hasLegacyBase64Notice) {
          const notice = document.createElement("div");
          notice.className = "media-load-notice";
          notice.textContent = "ℹ️ 该历史日志产生时，图像 Base64 数据曾被旧版审计规则转为摘要（[BASE64 ...]）。系统现已升级保留图像数据，后续新发起的生图请求将在此直接呈现完整画廊。";
          mediaView.append(notice);
        }
        videoItems.forEach((item) => {
          mediaView.append(createMediaFigure(item.url, item.title, "video", item.options));
        });
        mediaView.dataset.renderKey = mediaRenderKey;
      }
    }

    // Tab 2: Full Request
    const headersEl = byId("detail-headers");
    setLogDetailText(headersEl, formatJSON(row.request_headers));
    const reqBodyEl = byId("detail-request");
    setLogDetailText(reqBodyEl, formatJSON(row.request_body));

    // Tab 3: Full Response
    const respBodyEl = byId("detail-response");
    if (respBodyEl) {
      const initial = formatJSON(row.response_body);
      const terminal = row.async_result_body ? formatJSON(row.async_result_body) : "";
      setLogDetailText(respBodyEl, terminal ? `创建响应:\n${initial}\n\n异步任务最新结果:\n${terminal}` : initial);
    }
    const streamEl = byId("detail-stream");
    setLogDetailText(streamEl, row.stream_text || "无流式文本");
    const errEl = byId("detail-error");
    setLogDetailText(errEl, row.async_task_error || row.error_message || "无错误");

    // Tab 4: Upstream Trace & Lifecycle
    const events = byId("detail-events");
    if (events) {
      const upstreamPhases = new Set([
        "candidate_channels",
        "attempt_started",
        "upstream_attempt",
        "upstream_headers",
        "upstream_failed",
        "failover",
        "stream_started"
      ]);
      const upstream = [];
      const eventRenderKey = logDetailRenderKey(detail.events || []);
      const shouldRenderEvents = events.dataset.renderKey !== eventRenderKey;
      if (shouldRenderEvents) events.replaceChildren();
      (detail.events || []).forEach((event) => {
        if (shouldRenderEvents) {
          const li = document.createElement("li");
          const title = document.createElement("strong");
          const meta = document.createElement("small");
          title.textContent = event.phase;
          meta.textContent = `+${event.elapsed_ms} ms${event.channel_id ? ` · ${event.channel_id}` : ""}${event.status_code ? ` · HTTP ${event.status_code}` : ""}${event.message ? ` · ${event.message}` : ""}`;
          li.append(title, meta);
          if (event.data) {
            const pre = document.createElement("pre");
            pre.textContent = event.data;
            li.append(pre);
          }
          events.append(li);
        }
        if (upstreamPhases.has(event.phase)) {
          upstream.push(`${event.phase} +${event.elapsed_ms} ms${event.channel_id ? ` · ${event.channel_id}` : ""}${event.target_url ? ` · ${event.target_url}` : ""}${event.status_code ? ` · HTTP ${event.status_code}` : ""}${event.message ? ` · ${event.message}` : ""}${event.data ? `\n${event.data}` : ""}`);
        }
      });
      const upstreamEl = byId("detail-upstream");
      if (upstreamEl) {
        setLogDetailText(upstreamEl, upstream.length ? upstream.join("\n\n") : "无上游尝试事件");
      }
      if (shouldRenderEvents) events.dataset.renderKey = eventRenderKey;
    }

    // Durable task lifecycle details are returned with the log detail. These
    // are local projections only; rendering them must never trigger polling.
    const taskRunEl = byId("detail-task-run");
    if (taskRunEl) {
      const taskRun = detail.task_run;
      if (!taskRun) {
        setLogDetailText(taskRunEl, "无持久化任务聚合（旧日志或同步请求）");
      } else {
        setLogDetailText(taskRunEl, formatJSON({
          id: taskRun.id,
          task_kind: taskRun.task_kind,
          operation: taskRun.operation,
          engine: taskRun.engine,
          channel_id: taskRun.channel_id,
          profile_id: taskRun.profile_id,
          profile_revision: taskRun.profile_revision,
          profile_digest: taskRun.profile_digest,
          polling_mode: taskRun.polling_mode,
          provider_task_id: taskRun.provider_task_id,
          task_status: taskRun.task_status,
          task_outcome: taskRun.task_outcome,
          poll_count: taskRun.poll_count,
          poll_success_count: taskRun.poll_success_count,
          poll_failure_count: taskRun.poll_failure_count,
          last_http_status: taskRun.last_http_status,
          state_version: taskRun.state_version,
          created_at: taskRun.created_at,
          updated_at: taskRun.updated_at,
        }));
      }
    }

    const renderTaskTimeline = (elementID, items, renderItem) => {
      const container = byId(elementID);
      if (!container) return;
      const list = Array.isArray(items) ? items : [];
      const renderKey = logDetailRenderKey(list);
      if (container.dataset.renderKey === renderKey) return;
      container.replaceChildren();
      list.forEach((item) => container.append(renderItem(item)));
      if (!container.children.length) {
        const empty = document.createElement("li");
        empty.textContent = "无记录";
        container.append(empty);
      }
      container.dataset.renderKey = renderKey;
    };
    renderTaskTimeline("detail-task-events", detail.task_events, (event) => {
      const li = document.createElement("li");
      const title = document.createElement("strong");
      title.textContent = `${event.sequence ?? "-"} · ${event.type || "event"}`;
      const meta = document.createElement("small");
      meta.textContent = event.created_at ? new Date(event.created_at).toLocaleString() : "";
      li.append(title, meta);
      if (event.data) {
        const pre = document.createElement("pre");
        pre.textContent = formatJSON(event.data);
        li.append(pre);
      }
      return li;
    });
    renderTaskTimeline("detail-attempts", detail.attempts, (attempt) => {
      const li = document.createElement("li");
      const title = document.createElement("strong");
      title.textContent = `${attempt.attempt_type || "attempt"} · ${attempt.outcome || "unknown"}`;
      const meta = document.createElement("small");
      meta.textContent = `${attempt.http_status ? `HTTP ${attempt.http_status} · ` : ""}${attempt.started_at || ""}${attempt.finished_at ? ` → ${attempt.finished_at}` : ""}`;
      li.append(title, meta);
      if (attempt.error || attempt.response_meta) {
        const pre = document.createElement("pre");
        pre.textContent = attempt.error || formatJSON(attempt.response_meta);
        li.append(pre);
      }
      return li;
    });
    renderTaskTimeline("detail-media-assets", detail.media_assets, (asset) => {
      const li = document.createElement("li");
      const title = document.createElement("strong");
      title.textContent = `${asset.kind || "media"} · ${asset.status || "unknown"}`;
      const meta = document.createElement("small");
      meta.textContent = `${asset.public_id || asset.id || ""}${asset.content_type ? ` · ${asset.content_type}` : ""}${asset.byte_size ? ` · ${asset.byte_size} bytes` : ""}`;
      li.append(title, meta);
      return li;
    });
    refreshActiveLogListItem(row);
    if (shouldRefreshAsyncTask(row, detail)) {
      scheduleActiveLogRefresh(id, requestToken);
    } else {
      stopActiveLogRefresh();
    }
  } catch (err) {
    if (id !== activeLogID || requestToken !== activeLogRequestToken) return;
    if (background) {
      if (isPermanentLogRefreshError(err)) {
        activeLogRefreshDisabled = true;
        stopActiveLogRefresh();
        return;
      }
      activeLogRefreshFailures += 1;
      const retryDelay = Math.min(
        logDetailRefreshMs * (2 ** Math.min(activeLogRefreshFailures, 3)),
        logDetailMaxRetryMs
      );
      scheduleActiveLogRefresh(id, requestToken, retryDelay);
    } else {
      toast(err.message);
    }
  }
}

function bindLogInspector() {
  // Tab switching
  document.querySelectorAll("[data-log-tab]").forEach((button) => {
    button.addEventListener("click", () => setLogInspectorTab(button.dataset.logTab));
  });

  // Copy buttons
  byId("copy-log-id")?.addEventListener("click", () => {
    if (activeLogRecord?.id) copyText(activeLogRecord.id);
  });
  byId("copy-log-curl")?.addEventListener("click", () => {
    if (activeLogRecord) copyText(generateCurlCommand(activeLogRecord));
  });
  byId("copy-visual-prompt")?.addEventListener("click", () => {
    const text = byId("visual-prompt-body")?.textContent;
    if (text) copyText(text);
  });
  byId("copy-visual-response")?.addEventListener("click", () => {
    const text = byId("visual-response-body")?.textContent;
    if (text) copyText(text);
  });
  byId("copy-headers-btn")?.addEventListener("click", () => {
    const text = byId("detail-headers")?.textContent;
    if (text) copyText(text);
  });
  byId("copy-request-btn")?.addEventListener("click", () => {
    const text = byId("detail-request")?.textContent;
    if (text) copyText(text);
  });
  byId("copy-response-btn")?.addEventListener("click", () => {
    const text = byId("detail-response")?.textContent;
    if (text) copyText(text);
  });
  byId("copy-stream-btn")?.addEventListener("click", () => {
    const text = byId("detail-stream")?.textContent;
    if (text) copyText(text);
  });
}

const allAvailableModels = new Set();
const allAvailableChannels = new Map();
let dateSelectCtrl = null;
let channelSelectCtrl = null;
let outcomeSelectCtrl = null;

function setupCustomSelect({ wrapperId, inputId, triggerId, labelId, dropdownId, onChange }) {
  const wrapper = byId(wrapperId);
  const input = byId(inputId);
  const trigger = byId(triggerId);
  const label = byId(labelId);
  const dropdown = byId(dropdownId);
  if (!wrapper || !trigger || !dropdown) return null;

  function openDropdown() {
    document.querySelectorAll(".combobox-dropdown, .custom-select-dropdown").forEach((d) => {
      if (d !== dropdown) d.classList.add("hidden");
    });
    document.querySelectorAll(".custom-select-field, .model-combobox-field").forEach((f) => {
      if (f !== wrapper) f.classList.remove("open");
    });

    const rect = trigger.getBoundingClientRect();
    const spaceBelow = window.innerHeight - rect.bottom;
    const spaceAbove = rect.top;
    if (spaceBelow < 240 && spaceAbove > spaceBelow) {
      dropdown.classList.add("drop-up");
    } else {
      dropdown.classList.remove("drop-up");
    }

    dropdown.classList.remove("hidden");
    wrapper.classList.add("open");
    trigger.setAttribute("aria-expanded", "true");

    const highlighted = dropdown.querySelector(".combobox-item.highlighted");
    if (highlighted) {
      highlighted.scrollIntoView({ block: "nearest" });
    }
  }

  function closeDropdown() {
    dropdown.classList.add("hidden");
    wrapper.classList.remove("open");
    trigger.setAttribute("aria-expanded", "false");
  }

  function selectOption(val, text, triggerChange = true) {
    if (input) input.value = val;
    if (label) label.textContent = text;
    dropdown.querySelectorAll(".combobox-item").forEach((item) => {
      item.classList.toggle("highlighted", (item.dataset.value ?? "") === (val ?? ""));
    });
    closeDropdown();
    if (triggerChange && typeof onChange === "function") {
      onChange(val, text);
    }
  }

  trigger.addEventListener("click", (e) => {
    e.stopPropagation();
    if (dropdown.classList.contains("hidden")) {
      openDropdown();
    } else {
      closeDropdown();
    }
  });

  trigger.addEventListener("keydown", (e) => {
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      if (dropdown.classList.contains("hidden")) {
        openDropdown();
        return;
      }
      const items = Array.from(dropdown.querySelectorAll(".combobox-item"));
      if (!items.length) return;
      let idx = items.findIndex((el) => el.classList.contains("highlighted"));
      if (e.key === "ArrowDown") {
        idx = idx < 0 ? 0 : (idx + 1) % items.length;
      } else {
        idx = idx < 0 ? items.length - 1 : (idx - 1 + items.length) % items.length;
      }
      items.forEach((el) => el.classList.remove("highlighted"));
      items[idx].classList.add("highlighted");
      items[idx].scrollIntoView({ block: "nearest" });
    } else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      if (dropdown.classList.contains("hidden")) {
        openDropdown();
      } else {
        const highlighted = dropdown.querySelector(".combobox-item.highlighted");
        if (highlighted) {
          selectOption(highlighted.dataset.value ?? "", highlighted.textContent);
        } else {
          closeDropdown();
        }
      }
    } else if (e.key === "Escape") {
      closeDropdown();
    }
  });

  dropdown.querySelectorAll(".combobox-item").forEach((item) => {
    item.addEventListener("click", () => {
      selectOption(item.dataset.value ?? "", item.textContent);
    });
  });

  return { selectOption, closeDropdown, openDropdown };
}

function enhanceSelect(select) {
  if (!select || select.dataset.customSelectEnhanced === "true") return;
  if (typeof select.after !== "function" && !select.parentNode?.insertBefore) return;
  select.dataset.customSelectEnhanced = "true";
  select.classList.add("custom-select-native-hidden");

  const wrapper = document.createElement("div");
  wrapper.className = "custom-select-field";

  const trigger = document.createElement("button");
  trigger.type = "button";
  trigger.className = "custom-select-trigger";
  trigger.setAttribute("aria-haspopup", "listbox");
  trigger.setAttribute("aria-expanded", "false");

  const textSpan = document.createElement("span");
  textSpan.className = "custom-select-text";

  const arrowSpan = document.createElement("span");
  arrowSpan.className = "custom-select-arrow";
  arrowSpan.textContent = "▾";

  trigger.append(textSpan, arrowSpan);

  const dropdown = document.createElement("div");
  dropdown.className = "combobox-dropdown custom-select-dropdown hidden";
  dropdown.setAttribute("role", "listbox");

  wrapper.append(trigger, dropdown);
  if (typeof select.after === "function") {
    select.after(wrapper);
  } else if (select.parentNode?.insertBefore) {
    select.parentNode.insertBefore(wrapper, select.nextSibling);
  }

  function syncDisabled() {
    const disabled = Boolean(select.disabled);
    trigger.disabled = disabled;
    wrapper.classList.toggle("disabled", disabled);
    if (disabled) {
      closeDropdown();
    }
  }

  function updateLabel() {
    const selectedOpt = select.selectedOptions && select.selectedOptions[0];
    const text = selectedOpt ? selectedOpt.textContent : (select.options[0]?.textContent || "");
    textSpan.textContent = text || select.getAttribute("placeholder") || "请选择...";
    dropdown.querySelectorAll(".combobox-item").forEach((item) => {
      item.classList.toggle("highlighted", item.dataset.value === select.value);
    });
  }

  select._rebuildCustomSelect = rebuildDropdown;
  select._updateCustomSelectLabel = updateLabel;
  select._syncCustomSelectDisabled = syncDisabled;

  function appendComboboxItem(opt, isAll = false) {
    const item = document.createElement("div");
    item.className = "combobox-item";
    if (isAll) item.classList.add("all-option");
    if (opt.value === select.value) item.classList.add("highlighted");
    item.dataset.value = opt.value;
    item.textContent = opt.textContent;

    item.addEventListener("click", (e) => {
      e.stopPropagation();
      if (select.disabled || trigger.disabled) return;
      select.value = opt.value;
      updateLabel();
      closeDropdown();
      select.dispatchEvent(new Event("change", { bubbles: true }));
    });
    dropdown.append(item);
  }

  function rebuildDropdown() {
    dropdown.replaceChildren();
    const optgroups = select.querySelectorAll("optgroup");
    if (optgroups.length > 0) {
      optgroups.forEach((group) => {
        const header = document.createElement("div");
        header.className = "combobox-group-header";
        header.textContent = group.label;
        dropdown.append(header);

        Array.from(group.querySelectorAll("option")).forEach((opt) => {
          appendComboboxItem(opt);
        });
      });
    } else {
      Array.from(select.options).forEach((opt, idx) => {
        appendComboboxItem(opt, idx === 0 && (!opt.value || opt.value === "all"));
      });
    }
    updateLabel();
    syncDisabled();
  }

  function openDropdown() {
    if (select.disabled || trigger.disabled) return;
    document.querySelectorAll(".combobox-dropdown, .custom-select-dropdown").forEach((d) => {
      if (d !== dropdown) d.classList.add("hidden");
    });
    document.querySelectorAll(".custom-select-field, .model-combobox-field").forEach((f) => {
      if (f !== wrapper) f.classList.remove("open");
    });

    const rect = trigger.getBoundingClientRect();
    const spaceBelow = window.innerHeight - rect.bottom;
    const spaceAbove = rect.top;
    if (spaceBelow < 240 && spaceAbove > spaceBelow) {
      dropdown.classList.add("drop-up");
    } else {
      dropdown.classList.remove("drop-up");
    }

    dropdown.classList.remove("hidden");
    wrapper.classList.add("open");
    trigger.setAttribute("aria-expanded", "true");

    const highlighted = dropdown.querySelector(".combobox-item.highlighted");
    if (highlighted) {
      highlighted.scrollIntoView({ block: "nearest" });
    }
  }

  function closeDropdown() {
    dropdown.classList.add("hidden");
    wrapper.classList.remove("open");
    trigger.setAttribute("aria-expanded", "false");
  }

  trigger.addEventListener("click", (e) => {
    e.stopPropagation();
    e.preventDefault();
    if (select.disabled || trigger.disabled) return;
    if (dropdown.classList.contains("hidden")) {
      openDropdown();
    } else {
      closeDropdown();
    }
  });

  trigger.addEventListener("keydown", (e) => {
    if (select.disabled || trigger.disabled) return;
    if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      if (dropdown.classList.contains("hidden")) {
        openDropdown();
        return;
      }
      const items = Array.from(dropdown.querySelectorAll(".combobox-item"));
      if (!items.length) return;
      let idx = items.findIndex((el) => el.classList.contains("highlighted"));
      if (e.key === "ArrowDown") {
        idx = idx < 0 ? 0 : (idx + 1) % items.length;
      } else {
        idx = idx < 0 ? items.length - 1 : (idx - 1 + items.length) % items.length;
      }
      items.forEach((el) => el.classList.remove("highlighted"));
      items[idx].classList.add("highlighted");
      items[idx].scrollIntoView({ block: "nearest" });
    } else if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      if (dropdown.classList.contains("hidden")) {
        openDropdown();
      } else {
        const highlighted = dropdown.querySelector(".combobox-item.highlighted");
        if (highlighted) {
          select.value = highlighted.dataset.value ?? "";
          updateLabel();
          closeDropdown();
          select.dispatchEvent(new Event("change", { bubbles: true }));
        } else {
          closeDropdown();
        }
      }
    } else if (e.key === "Escape") {
      closeDropdown();
    }
  });

  select.addEventListener("change", () => {
    updateLabel();
  });

  if (typeof MutationObserver !== "undefined") {
    const observer = new MutationObserver(() => {
      rebuildDropdown();
      syncDisabled();
    });
    observer.observe(select, { childList: true, subtree: true, characterData: true, attributes: true, attributeFilter: ["disabled"] });
  }

  rebuildDropdown();
  syncDisabled();
}

function enhanceAllSelects(root = document) {
  root.querySelectorAll("select:not(.custom-select-native-hidden)").forEach((sel) => {
    enhanceSelect(sel);
  });
}

function renderChannelSelectOptions() {
  const dropdown = byId("channel-select-dropdown");
  const input = byId("channel-filter-select");
  if (!dropdown || !input) return;

  const currentVal = input.value || "";
  dropdown.replaceChildren();

  const allItem = document.createElement("div");
  allItem.className = "combobox-item all-option" + (currentVal === "" ? " highlighted" : "");
  allItem.dataset.value = "";
  allItem.textContent = "🏷️ 全部渠道";
  allItem.addEventListener("click", () => {
    channelSelectCtrl?.selectOption("", "🏷️ 全部渠道");
  });
  dropdown.append(allItem);

  const sorted = Array.from(allAvailableChannels.entries()).sort((a, b) => a[1].localeCompare(b[1]));
  sorted.forEach(([id, name]) => {
    const item = document.createElement("div");
    item.className = "combobox-item" + (currentVal === id ? " highlighted" : "");
    item.dataset.value = id;
    item.textContent = name;
    item.addEventListener("click", () => {
      channelSelectCtrl?.selectOption(id, name);
    });
    dropdown.append(item);
  });
}

function renderModelComboboxOptions(filterText = "") {
  const dropdown = byId("model-combobox-dropdown");
  const input = byId("model-filter-input");
  if (!dropdown || !input) return;

  dropdown.replaceChildren();

  // Option 1: All models / Clear
  const allOpt = document.createElement("div");
  allOpt.className = "combobox-item all-option";
  allOpt.textContent = "🤖 全部模型 (清空筛选)";
  allOpt.addEventListener("click", () => {
    input.value = "";
    dropdown.classList.add("hidden");
    byId("model-combobox")?.classList.remove("open");
    logPage = 1;
    loadLogs().catch((err) => toast(err.message));
  });
  dropdown.append(allOpt);

  const query = (filterText || "").trim().toLowerCase();
  const sorted = Array.from(allAvailableModels).sort();
  const matched = query ? sorted.filter((m) => m.toLowerCase().includes(query)) : sorted;

  if (matched.length === 0) {
    const empty = document.createElement("div");
    empty.className = "combobox-empty";
    empty.textContent = "无匹配模型 (按回车直接搜索)";
    dropdown.append(empty);
  } else {
    matched.forEach((m) => {
      const item = document.createElement("div");
      item.className = "combobox-item";
      item.textContent = m;
      if (input.value && input.value.trim() === m) {
        item.classList.add("highlighted");
      }
      item.addEventListener("click", () => {
        input.value = m;
        dropdown.classList.add("hidden");
        byId("model-combobox")?.classList.remove("open");
        logPage = 1;
        loadLogs().catch((err) => toast(err.message));
      });
      dropdown.append(item);
    });
  }
}

async function populateLogFilterOptions() {
  try {
    const res = await request("/api/channels");
    const channelList = (res && res.channels) || [];

    channelList.forEach((ch) => {
      const name = ch.name ? `🏷️ ${ch.name}` : `🏷️ ${ch.id}`;
      allAvailableChannels.set(ch.id, name);

      const models = channelModels(ch);
      if (Array.isArray(models)) {
        models.forEach((m) => {
          if (m && typeof m === "string" && !allAvailableModels.has(m.trim())) {
            allAvailableModels.add(m.trim());
          }
        });
      }
    });

    renderChannelSelectOptions();
    renderModelComboboxOptions(byId("model-filter-input")?.value || "");
  } catch (err) {
    console.error("Failed to populate filter options:", err);
  }
}

async function initLogs() {
  await loadIdentity();
  bindLogout();
  bindMobileNav();
  bindLogInspector();
  document.addEventListener("visibilitychange", handleActiveLogVisibilityChange);
  window.addEventListener("pagehide", stopActiveLogRefresh, { once: true });
  window.addEventListener("beforeunload", stopActiveLogRefresh, { once: true });

  dateSelectCtrl = setupCustomSelect({
    wrapperId: "date-select-field",
    inputId: "date-preset",
    triggerId: "date-select-trigger",
    labelId: "date-select-label",
    dropdownId: "date-select-dropdown",
    onChange: (val) => {
      const isCustom = val === "custom";
      byId("custom-date-row")?.classList.toggle("hidden", !isCustom);
      if (!isCustom) {
        logPage = 1;
        loadLogs().catch((err) => toast(err.message));
      }
    },
  });

  channelSelectCtrl = setupCustomSelect({
    wrapperId: "channel-select-field",
    inputId: "channel-filter-select",
    triggerId: "channel-select-trigger",
    labelId: "channel-select-label",
    dropdownId: "channel-select-dropdown",
    onChange: () => {
      logPage = 1;
      loadLogs().catch((err) => toast(err.message));
    },
  });

  outcomeSelectCtrl = setupCustomSelect({
    wrapperId: "outcome-select-field",
    inputId: "outcome-filter-input",
    triggerId: "outcome-select-trigger",
    labelId: "outcome-select-label",
    dropdownId: "outcome-select-dropdown",
    onChange: () => {
      logPage = 1;
      loadLogs().catch((err) => toast(err.message));
    },
  });

  await populateLogFilterOptions();

  byId("filter-from-date")?.addEventListener("change", () => {
    logPage = 1;
    loadLogs().catch((err) => toast(err.message));
  });
  byId("filter-to-date")?.addEventListener("change", () => {
    logPage = 1;
    loadLogs().catch((err) => toast(err.message));
  });

  const modelInput = byId("model-filter-input");
  const modelDropdown = byId("model-combobox-dropdown");
  const modelArrow = byId("model-combobox-arrow");

  function openModelDropdown() {
    document.querySelectorAll(".combobox-dropdown, .custom-select-dropdown").forEach((d) => {
      if (d !== modelDropdown) d.classList.add("hidden");
    });
    document.querySelectorAll(".custom-select-field").forEach((f) => f.classList.remove("open"));

    byId("model-combobox")?.classList.add("open");
    renderModelComboboxOptions("");
    modelDropdown.classList.remove("hidden");
    const highlighted = modelDropdown.querySelector(".combobox-item.highlighted");
    if (highlighted) {
      highlighted.scrollIntoView({ block: "nearest" });
    }
  }

  function closeModelDropdown() {
    modelDropdown?.classList.add("hidden");
    byId("model-combobox")?.classList.remove("open");
  }

  if (modelInput && modelDropdown) {
    modelInput.addEventListener("focus", () => {
      openModelDropdown();
    });

    modelInput.addEventListener("input", () => {
      byId("model-combobox")?.classList.add("open");
      renderModelComboboxOptions(modelInput.value);
      modelDropdown.classList.remove("hidden");
    });

    modelInput.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown" || e.key === "ArrowUp") {
        e.preventDefault();
        if (modelDropdown.classList.contains("hidden")) {
          openModelDropdown();
          return;
        }
        const items = Array.from(modelDropdown.querySelectorAll(".combobox-item"));
        if (!items.length) return;
        let idx = items.findIndex((el) => el.classList.contains("highlighted"));
        if (e.key === "ArrowDown") {
          idx = idx < 0 ? 0 : (idx + 1) % items.length;
        } else {
          idx = idx < 0 ? items.length - 1 : (idx - 1 + items.length) % items.length;
        }
        items.forEach((el) => el.classList.remove("highlighted"));
        items[idx].classList.add("highlighted");
        items[idx].scrollIntoView({ block: "nearest" });
      } else if (e.key === "Enter") {
        e.preventDefault();
        const highlighted = !modelDropdown.classList.contains("hidden")
          ? modelDropdown.querySelector(".combobox-item.highlighted")
          : null;
        if (highlighted && !highlighted.classList.contains("all-option")) {
          modelInput.value = highlighted.textContent || "";
        } else if (highlighted && highlighted.classList.contains("all-option")) {
          modelInput.value = "";
        }
        closeModelDropdown();
        logPage = 1;
        loadLogs().catch((err) => toast(err.message));
      } else if (e.key === "Escape") {
        closeModelDropdown();
      }
    });

    modelArrow?.addEventListener("click", (e) => {
      e.stopPropagation();
      const isHidden = modelDropdown.classList.contains("hidden");
      if (isHidden) {
        openModelDropdown();
        modelInput.focus();
      } else {
        closeModelDropdown();
      }
    });

    document.addEventListener("click", (e) => {
      if (!e.target.closest(".custom-select-field") && !e.target.closest(".model-combobox-field")) {
        document.querySelectorAll(".combobox-dropdown, .custom-select-dropdown").forEach((d) => d.classList.add("hidden"));
        document.querySelectorAll(".custom-select-field, .model-combobox-field").forEach((f) => f.classList.remove("open"));
      }
    });
  }

  byId("log-filters")?.addEventListener("submit", (event) => {
    event.preventDefault();
    logPage = 1;
    loadLogs().catch((err) => toast(err.message));
  });

  byId("reset-filters-btn")?.addEventListener("click", () => {
    const form = byId("log-filters");
    if (form) {
      form.reset();
      dateSelectCtrl?.selectOption("all", "📅 全部日期", false);
      channelSelectCtrl?.selectOption("", "🏷️ 全部渠道", false);
      outcomeSelectCtrl?.selectOption("", "🚥 全部状态", false);
      if (modelInput) modelInput.value = "";
      byId("custom-date-row")?.classList.add("hidden");
      document.querySelectorAll(".combobox-dropdown, .custom-select-dropdown").forEach((d) => d.classList.add("hidden"));
      document.querySelectorAll(".custom-select-field, .model-combobox-field").forEach((f) => f.classList.remove("open"));
      logPage = 1;
      loadLogs().catch((err) => toast(err.message));
    }
  });

  byId("prev-page").addEventListener("click", () => {
    if (logPage > 1) {
      logPage -= 1;
      loadLogs();
    }
  });
  byId("next-page").addEventListener("click", () => {
    if (logPage * logPageSize < logTotal) {
      logPage += 1;
      loadLogs();
    }
  });
  byId("close-detail")?.addEventListener("click", () => byId("log-dialog")?.close());
  byId("mobile-inspector-back")?.addEventListener("click", () => {
    byId("logs-inspector")?.classList.remove("mobile-active");
  });
  byId("clear-logs").addEventListener("click", async () => {
    if (!confirm("确定永久清空全部结构化调用日志吗？此操作无法撤销。")) return;
    try {
      await request("/api/logs?confirm=DELETE", { method: "DELETE" });
      toast("日志已清空");
      logPage = 1;
      await loadLogs();
    } catch (err) {
      toast(err.message);
    }
  });
  await loadLogs();
}

function formatMediaSize(bytes) {
  const value = Number(bytes || 0);
  if (!Number.isFinite(value)) return "—";
  if (value < 1024) return `${Math.max(0, value)} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MiB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
}

async function initMedia() {
  await loadIdentity();
  bindLogout();
  bindMobileNav();
  const list = byId("media-list");
  const grid = byId("media-grid");
  const tableWrap = byId("media-table-wrap");
  const empty = byId("media-empty");
  const btnGrid = byId("media-view-grid");
  const btnTable = byId("media-view-table");
  const pageSize = 24;
  let offset = 0;
  let mediaLoadVersion = 0;
  let previewLinkVersion = 0;
  let previewAssetId = null;
  const previewDialog = byId("media-preview-dialog");
  const previewContent = byId("media-preview-content");

  let currentViewMode = localStorage.getItem("media_view_mode") || "grid";
  const setViewMode = (mode) => {
    currentViewMode = mode;
    localStorage.setItem("media_view_mode", mode);
    btnGrid?.classList.toggle("active", mode === "grid");
    btnTable?.classList.toggle("active", mode === "table");
    grid?.classList.toggle("hidden", mode !== "grid");
    tableWrap?.classList.toggle("hidden", mode !== "table");
  };
  btnGrid?.addEventListener("click", () => setViewMode("grid"));
  btnTable?.addEventListener("click", () => setViewMode("table"));
  setViewMode(currentViewMode);

  const getMediaContentURL = (asset) => asset.kind === "video"
    ? `/api/media-assets/${encodeURIComponent(asset.id)}/content.mp4`
    : `/api/media-assets/${encodeURIComponent(asset.id)}/content`;

  const getMediaDownloadName = (asset) => asset.display_name || (asset.kind === "video" ? `video_${asset.id}.mp4` : asset.kind === "audio" ? `audio_${asset.id}.mp3` : `image_${asset.id}.png`);

  const applyVideoStandardAttrs = (v) => {
    v.disablePictureInPicture = true;
    v.setAttribute("disablePictureInPicture", "true");
    v.setAttribute("controlsList", "nodownload");
    v.setAttribute("translate", "no");
    v.classList.add("notranslate");
  };

  const pauseAllOtherMedia = (currentMedia) => {
    document.querySelectorAll(".media-gallery-card video, .media-gallery-card audio, .media-preview-content video, .media-preview-content audio").forEach((el) => {
      if (el !== currentMedia && !el.paused) {
        el.pause();
      }
    });
  };

  const MAX_CONCURRENT_VIDEO_LOADS = 2;
  let activeVideoLoads = 0;
  const videoLoadQueue = [];

  const pumpVideoLoadQueue = () => {
    while (activeVideoLoads < MAX_CONCURRENT_VIDEO_LOADS && videoLoadQueue.length > 0) {
      const task = videoLoadQueue.shift();
      if (!task.video || task.video.isConnected === false) continue;
      activeVideoLoads++;
      let finished = false;
      const onDone = () => {
        if (finished) return;
        finished = true;
        task.video.removeEventListener("loadeddata", onDone);
        task.video.removeEventListener("loadedmetadata", onDone);
        task.video.removeEventListener("error", onDone);
        activeVideoLoads--;
        pumpVideoLoadQueue();
      };
      task.video.addEventListener("loadeddata", onDone, { once: true });
      task.video.addEventListener("loadedmetadata", onDone, { once: true });
      task.video.addEventListener("error", onDone, { once: true });
      setTimeout(onDone, 3000);

      task.video.preload = "metadata";
      task.video.src = task.src;
    }
  };

  const queueVideoFirstFrame = (video, src) => {
    video.preload = "none";
    videoLoadQueue.push({ video, src });
    pumpVideoLoadQueue();
  };

  let videoObserver = null;
  if (typeof IntersectionObserver !== "undefined") {
    videoObserver = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          const video = entry.target;
          videoObserver.unobserve(video);
          const src = video.dataset.src;
          if (src && !video.hasAttribute("src")) {
            queueVideoFirstFrame(video, src);
          }
        }
      });
    }, { rootMargin: "200px" });
  }

  const registerVideoForFirstFrame = (video, src) => {
    video.dataset.src = src;
    video.preload = "none";
    if (videoObserver) {
      videoObserver.observe(video);
    } else {
      queueVideoFirstFrame(video, src);
    }
  };

  const cleanupMedia = (container) => {
    if (!container) return;
    container.querySelectorAll("video, audio").forEach((video) => {
      if (videoObserver) {
        try { videoObserver.unobserve(video); } catch (_) {}
      }
      try {
        video.pause();
        video.removeAttribute("src");
        video.load();
      } catch (_) {}
    });
    container.replaceChildren();
  };

  const closePreview = () => {
    previewLinkVersion++;
    previewAssetId = null;
    if (previewContent) {
      cleanupMedia(previewContent);
    }
    if (previewDialog?.open) previewDialog.close();
  };

  const openPreview = (asset) => {
    if (!previewDialog || !previewContent) return;
    const linkVersion = ++previewLinkVersion;
    previewAssetId = String(asset.id);
    const isVid = asset.kind === "video";
    const isAud = asset.kind === "audio";
    const type = isVid ? "video" : isAud ? "audio" : "image";
    const media = document.createElement(isVid ? "video" : isAud ? "audio" : "img");
    media.className = `media-preview-${type}`;
    media.addEventListener("error", () => {
      const failure = document.createElement("p");
      failure.className = "muted";
      failure.textContent = "媒体加载失败：文件可能已删除、登录已过期，或浏览器不支持此格式。请关闭后刷新页面重试。";
      if (previewContent.querySelector("video, audio, img") === media) previewContent.append(failure);
    }, { once: true });

    const contentURL = getMediaContentURL(asset);
    media.src = `/api/media-assets/${encodeURIComponent(asset.id)}/content${isVid ? ".mp4" : ""}`;

    if (isVid || isAud) {
      media.controls = true;
      media.preload = "metadata";
      applyVideoStandardAttrs(media);
      media.addEventListener("play", () => pauseAllOtherMedia(media));
    } else {
      media.alt = asset.display_name || "媒体图片预览";
    }

    previewContent.replaceChildren(media);
    byId("media-preview-title").textContent = asset.display_name || (isVid ? "视频预览" : isAud ? "音频预览" : "图片预览");

    const metaLabel = byId("media-preview-meta");
    if (metaLabel) {
      metaLabel.textContent = `${formatMediaSize(asset.byte_size)} · ${asset.created_at ? new Date(asset.created_at).toLocaleString() : ""}`;
    }

    const urlInput = byId("media-preview-url");
    const copyBtn = byId("media-preview-copy-btn");
    const openExtBtn = byId("media-preview-open-ext");
    if (urlInput) {
      urlInput.value = "正在加载公开链接…";
      urlInput.disabled = true;
    }
    if (copyBtn) copyBtn.disabled = true;
    if (openExtBtn) openExtBtn.removeAttribute("href");

    const setPreviewPublicURL = (url) => {
      if (linkVersion !== previewLinkVersion || previewAssetId !== String(asset.id)) return;
      const absUrl = new URL(url, window.location.origin).href;
      if (urlInput) { urlInput.value = absUrl; urlInput.disabled = false; }
      if (copyBtn) { copyBtn.disabled = false; copyBtn.onclick = () => handleCopyLink(copyBtn, absUrl); }
      if (openExtBtn) openExtBtn.href = absUrl;
    };
    request(`/api/media-assets/${encodeURIComponent(asset.id)}/link`)
      .then((result) => {
        if (!result?.url || typeof result.url !== "string") throw new Error("服务器未返回有效的公开链接");
        setPreviewPublicURL(result.url);
      })
      .catch((err) => {
        if (linkVersion !== previewLinkVersion || previewAssetId !== String(asset.id)) return;
        if (urlInput) { urlInput.value = "公开链接加载失败，请稍后重试"; urlInput.disabled = true; }
        if (copyBtn) copyBtn.disabled = true;
        if (openExtBtn) openExtBtn.removeAttribute("href");
        toast(`公开链接加载失败：${err.message}`);
      });

    const dlBtn = byId("media-preview-download-btn");
    if (dlBtn) {
      dlBtn.href = contentURL;
      dlBtn.download = getMediaDownloadName(asset);
    }

    previewDialog.showModal();
  };

  byId("media-preview-close")?.addEventListener("click", closePreview);
  previewDialog?.addEventListener("cancel", () => closePreview());
  previewDialog?.addEventListener("click", (event) => { if (event.target === previewDialog) closePreview(); });

  const statusLabels = {
    pending: "排队中",
    materializing: "固化中",
    available: "可用",
    failed: "处理失败"
  };

  const createKindBadge = (kind) => {
    const badge = document.createElement("span");
    badge.className = `badge ${kind === "video" ? "badge-kind-video" : kind === "image" ? "badge-kind-image" : "badge-kind-audio"}`;
    badge.style.fontSize = "0.72rem";
    badge.style.padding = "2px 7px";
    badge.style.borderRadius = "4px";
    badge.textContent = kind === "video" ? "视频 🎬" : kind === "image" ? "图片 🖼️" : kind === "audio" ? "音频 🎵" : (kind || "文件");
    return badge;
  };

  const createStatusBadge = (status) => {
    const span = document.createElement("span");
    span.className = status === "available" ? "badge-status-available" : ["pending", "materializing"].includes(status) ? "badge-status-processing" : status === "failed" ? "badge-status-failed" : "badge-status-muted";
    span.style.fontSize = "0.72rem";
    span.style.fontWeight = "600";
    span.textContent = statusLabels[status] || status || "—";
    return span;
  };

  const handleCopyLink = async (btn, textToCopy) => {
    try {
      await copyText(textToCopy);
      const prev = btn.textContent;
      btn.textContent = "✓ 已复制";
      btn.classList.add("btn-copied");
      setTimeout(() => {
        btn.textContent = prev;
        btn.classList.remove("btn-copied");
      }, 1500);
    } catch (err) {
      toast(err.message);
    }
  };

  const createMediaActions = (asset, isCard) => {
    const actionsWrap = document.createElement("div");
    actionsWrap.className = isCard ? "media-card-actions" : "row-actions";
    const isAvail = asset.status === "available";
    const contentURL = getMediaContentURL(asset);
    const downloadName = getMediaDownloadName(asset);

    if (["image", "video", "audio"].includes(asset.kind) && isAvail) {
      if (!isCard) {
        const preview = document.createElement("button");
        preview.type = "button";
        preview.className = "button ghost compact-btn";
        preview.textContent = "预览";
        preview.addEventListener("click", () => openPreview(asset));
        actionsWrap.append(preview);
      }

      const dl = document.createElement("a");
      dl.className = "button ghost compact-btn";
      dl.href = contentURL;
      dl.download = downloadName;
      dl.title = "下载此文件";
      dl.textContent = "下载 💾";
      actionsWrap.append(dl);

      const publicLink = document.createElement("button");
      publicLink.type = "button";
      publicLink.className = "button ghost compact-btn";
      publicLink.textContent = "复制公开链接";
      publicLink.title = "复制固定公开链接";
      publicLink.addEventListener("click", async () => {
        try {
          publicLink.disabled = true;
          const result = await request(`/api/media-assets/${encodeURIComponent(asset.id)}/link`);
          if (!result?.url) throw new Error("服务器未返回有效的公开链接");
          await handleCopyLink(publicLink, new URL(result.url, window.location.origin).href);
        } catch (err) {
          toast(err.message);
        } finally {
          publicLink.disabled = false;
        }
      });
      actionsWrap.append(publicLink);
    }

    if (["failed", "pending", "materializing"].includes(asset.status)) {
      const retry = document.createElement("button");
      retry.type = "button";
      retry.className = "button ghost compact-btn";
      retry.textContent = "重试";
      retry.addEventListener("click", async () => {
        try {
          await request(`/api/media-assets/${asset.id}/retry`, { method: "POST" });
          await load();
        } catch (err) {
          toast(err.message);
        }
      });
      actionsWrap.append(retry);
    }

    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "button button-danger-ghost compact-btn";
    remove.textContent = "删除";
    remove.addEventListener("click", async () => {
      if (!confirm("确定删除此媒体吗？本地文件和数据将被彻底删除。")) return;
      remove.disabled = true;
      try {
        await request(`/api/media-assets/${asset.id}`, { method: "DELETE" });
        toast("媒体已彻底删除");
        await load();
      } catch (err) {
        remove.disabled = false;
        toast(err.message);
      }
    });
    actionsWrap.append(remove);

    if (!isCard) {
      const td = document.createElement("td");
      td.className = "align-right";
      td.append(actionsWrap);
      return td;
    }

    return actionsWrap;
  };

  const load = async () => {
    const version = ++mediaLoadVersion;
    const kind = byId("media-kind-filter")?.value || "";
    const status = byId("media-status-filter")?.value || "";
    const query = new URLSearchParams({ limit: String(pageSize), offset: String(offset) });
    if (kind) query.set("kind", kind);
    if (status) query.set("status", status);

    const result = await request(`/api/media-assets?${query}`);
    if (version !== mediaLoadVersion) return;
    const assets = Array.isArray(result?.data) ? result.data : [];
    videoLoadQueue.length = 0;
    cleanupMedia(list);
    if (grid) cleanupMedia(grid);
    empty.classList.toggle("hidden", assets.length !== 0);

    for (const asset of assets) {
      const isVideo = asset.kind === "video";
      const isImage = asset.kind === "image";
      const isAudio = asset.kind === "audio";
      const isAvail = asset.status === "available";
      const contentURL = getMediaContentURL(asset);

      // 1. Table Row
      const row = document.createElement("tr");

      const thumbCell = document.createElement("td");
      const thumbWrap = document.createElement("div");
      thumbWrap.className = "table-thumb";
      if (isAvail) {
        thumbWrap.setAttribute("role", "button");
        thumbWrap.setAttribute("tabindex", "0");
        thumbWrap.addEventListener("keydown", (e) => {
          if (e.key === "Enter" || e.key === " " || e.key === "Spacebar") {
            e.preventDefault();
            openPreview(asset);
          }
        });
        if (isVideo) {
          thumbWrap.setAttribute("aria-label", "查看视频预览");
          const vThumb = document.createElement("video");
          vThumb.muted = true;
          applyVideoStandardAttrs(vThumb);
          registerVideoForFirstFrame(vThumb, `${contentURL}#t=0.001`);
          const playBadge = document.createElement("span");
          playBadge.className = "thumb-play-icon";
          playBadge.textContent = "▶";
          thumbWrap.append(vThumb, playBadge);
          thumbWrap.title = "点击查看视频";
          thumbWrap.addEventListener("click", () => openPreview(asset));
        } else if (isImage) {
          thumbWrap.setAttribute("aria-label", "查看图片预览");
          const iThumb = document.createElement("img");
          iThumb.loading = "lazy";
          iThumb.src = contentURL;
          iThumb.alt = "缩略图";
          thumbWrap.append(iThumb);
          thumbWrap.title = "点击查看大图";
          thumbWrap.addEventListener("click", () => openPreview(asset));
        } else if (isAudio) {
          thumbWrap.setAttribute("aria-label", "播放音频预览");
          const iconSpan = document.createElement("span");
          iconSpan.textContent = "🎵";
          thumbWrap.append(iconSpan);
          thumbWrap.title = "点击播放音频";
          thumbWrap.addEventListener("click", () => openPreview(asset));
        }
      } else {
        const iconSpan = document.createElement("span");
        iconSpan.textContent = isVideo ? "🎬" : isImage ? "🖼️" : isAudio ? "🎵" : "📁";
        thumbWrap.append(iconSpan);
      }
      thumbCell.append(thumbWrap);
      row.append(thumbCell);

      const kindCell = document.createElement("td");
      kindCell.append(createKindBadge(asset.kind));
      row.append(kindCell);

      const statusCell = document.createElement("td");
      statusCell.append(createStatusBadge(asset.status));
      row.append(statusCell);

      const sizeCell = document.createElement("td");
      sizeCell.style.fontFamily = "var(--font-mono, monospace)";
      sizeCell.textContent = formatMediaSize(asset.byte_size);
      row.append(sizeCell);

      const taskCell = document.createElement("td");
      taskCell.style.fontFamily = "var(--font-mono, monospace)";
      taskCell.style.fontSize = "0.78rem";
      taskCell.textContent = asset.task_run_id || "—";
      row.append(taskCell);

      const dateCell = document.createElement("td");
      dateCell.textContent = asset.created_at ? new Date(asset.created_at).toLocaleString() : "—";
      row.append(dateCell);

      const actionsCell = createMediaActions(asset, false);
      row.append(actionsCell);
      list.append(row);

      // 2. Gallery Card
      if (grid) {
        const card = document.createElement("div");
        card.className = "media-gallery-card";

        if (isAvail) {
          if (isVideo) {
            const mediaWrap = document.createElement("div");
            mediaWrap.className = "media-card-media-wrap";
            const vid = document.createElement("video");
            vid.controls = true;
            vid.playsInline = true;
            applyVideoStandardAttrs(vid);
            registerVideoForFirstFrame(vid, `${contentURL}#t=0.001`);
            vid.addEventListener("play", () => {
              pauseAllOtherMedia(vid);
              if (!vid.getAttribute("src")) {
                vid.src = `${contentURL}#t=0.001`;
                vid.preload = "auto";
                vid.play().catch(() => {});
              }
            });
            vid.addEventListener("error", () => {
              const errBox = document.createElement("div");
              errBox.className = "media-card-error";
              const errIcon = document.createElement("span");
              errIcon.textContent = "⚠️";
              const errText = document.createElement("span");
              errText.textContent = "视频加载失败或格式不兼容";
              errBox.append(errIcon, errText);
              mediaWrap.replaceChildren(errBox);
            }, { once: true });
            mediaWrap.append(vid);
            card.append(mediaWrap);
          } else if (isImage) {
            const mediaWrap = document.createElement("div");
            mediaWrap.className = "media-card-media-wrap";
            const img = document.createElement("img");
            img.loading = "lazy";
            img.src = contentURL;
            img.alt = asset.display_name || "图片";
            img.title = "点击查看大图预览";
            img.setAttribute("role", "button");
            img.setAttribute("tabindex", "0");
            img.setAttribute("aria-label", "查看大图预览");
            img.addEventListener("click", () => openPreview(asset));
            img.addEventListener("keydown", (e) => {
              if (e.key === "Enter" || e.key === " " || e.key === "Spacebar") {
                e.preventDefault();
                openPreview(asset);
              }
            });
            img.addEventListener("error", () => {
              const errBox = document.createElement("div");
              errBox.className = "media-card-error";
              const errIcon = document.createElement("span");
              errIcon.textContent = "⚠️";
              const errText = document.createElement("span");
              errText.textContent = "图片加载失败";
              errBox.append(errIcon, errText);
              mediaWrap.replaceChildren(errBox);
            }, { once: true });
            mediaWrap.append(img);
            card.append(mediaWrap);
          } else if (isAudio) {
            const audWrap = document.createElement("div");
            audWrap.className = "media-card-audio-wrap";
            const waveIcon = document.createElement("span");
            waveIcon.className = "media-audio-wave-icon";
            waveIcon.textContent = "🎵";
            const aud = document.createElement("audio");
            aud.controls = true;
            aud.preload = "none";
            aud.src = contentURL;
            aud.addEventListener("play", () => pauseAllOtherMedia(aud));
            audWrap.append(waveIcon, aud);
            card.append(audWrap);
          }
        } else {
          const mediaWrap = document.createElement("div");
          mediaWrap.className = "media-card-media-wrap";
          const placeholder = document.createElement("div");
          placeholder.className = "media-card-placeholder";
          const iconSpan = document.createElement("span");
          iconSpan.style.fontSize = "1.8rem";
          iconSpan.textContent = isVideo ? "🎬" : isImage ? "🖼️" : isAudio ? "🎵" : "📁";
          const textSpan = document.createElement("span");
          textSpan.textContent = statusLabels[asset.status] || asset.status;
          placeholder.append(iconSpan, textSpan);
          mediaWrap.append(placeholder);
          card.append(mediaWrap);
        }

        const cardBody = document.createElement("div");
        cardBody.className = "media-card-body";

        const titleRow = document.createElement("div");
        titleRow.className = "media-card-title-row";

        const leftDiv = document.createElement("div");
        leftDiv.style.display = "flex";
        leftDiv.style.alignItems = "center";
        leftDiv.style.gap = "6px";
        leftDiv.append(createKindBadge(asset.kind), createStatusBadge(asset.status));

        const sizeSpan = document.createElement("span");
        sizeSpan.style.fontSize = "0.74rem";
        sizeSpan.style.fontWeight = "600";
        sizeSpan.style.fontFamily = "var(--font-mono, monospace)";
        sizeSpan.textContent = formatMediaSize(asset.byte_size);

        titleRow.append(leftDiv, sizeSpan);
        cardBody.append(titleRow);

        const nameDiv = document.createElement("div");
        nameDiv.className = "media-card-name";
        nameDiv.textContent = asset.display_name || asset.task_run_id || `媒体资产 #${asset.id}`;
        nameDiv.title = nameDiv.textContent;
        cardBody.append(nameDiv);

        const dateDiv = document.createElement("div");
        dateDiv.className = "media-card-date";
        dateDiv.textContent = asset.created_at ? new Date(asset.created_at).toLocaleString() : "—";
        cardBody.append(dateDiv);

        const cardActions = createMediaActions(asset, true);
        cardBody.append(cardActions);
        card.append(cardBody);
        grid.append(card);
      }
    }

    const page = Math.floor(offset / pageSize) + 1;
    byId("media-page-label").textContent = `第 ${page} 页`;
    byId("media-prev").disabled = offset === 0;
    byId("media-next").disabled = assets.length < pageSize;
  };

  byId("media-refresh")?.addEventListener("click", () => load().catch((err) => toast(err.message)));
  ["media-kind-filter", "media-status-filter"].forEach((id) => byId(id)?.addEventListener("change", () => { offset = 0; load().catch((err) => toast(err.message)); }));
  const kindSelect = byId("media-kind-filter");
  const statusSelect = byId("media-status-filter");
  if (kindSelect) enhanceSelect(kindSelect);
  if (statusSelect) enhanceSelect(statusSelect);
  byId("media-prev")?.addEventListener("click", () => { offset = Math.max(0, offset - pageSize); load().catch((err) => toast(err.message)); });
  byId("media-next")?.addEventListener("click", () => { offset += pageSize; load().catch((err) => toast(err.message)); });

  const cleanupAllMedia = () => {
    cleanupMedia(list);
    if (grid) cleanupMedia(grid);
    if (previewContent) cleanupMedia(previewContent);
  };
  window.addEventListener("pagehide", cleanupAllMedia, { once: true });
  window.addEventListener("beforeunload", cleanupAllMedia, { once: true });

  await load();
}

document.addEventListener("DOMContentLoaded", () => {
  document.addEventListener("click", (e) => {
    if (!e.target.closest(".custom-select-field") && !e.target.closest(".model-combobox-field")) {
      document.querySelectorAll(".combobox-dropdown, .custom-select-dropdown").forEach((d) => d.classList.add("hidden"));
      document.querySelectorAll(".custom-select-field, .model-combobox-field").forEach((f) => f.classList.remove("open"));
    }
  });

  const page = document.body.dataset.page;
  const start = page === "setup" ? initSetup : page === "login" ? initLogin : page === "dashboard" ? initDashboard : page === "logs" ? initLogs : page === "media" ? initMedia : page === "profiles" ? initProfiles : null;
  if (start) Promise.resolve(start()).catch((err) => toast(err.message));
});
