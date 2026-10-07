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
  toggle(name, force = !this.contains(name)) { if (force) this.add(name); else this.remove(name); return force; }
}

class Element {
  constructor(tag = "div") {
    this.tagName = tag.toUpperCase();
    this.children = [];
    this.dataset = {};
    this.attributes = new Map();
    this.listeners = new Map();
    this.classList = new Classes();
    this.style = {};
    this._value = "";
    this.textContent = "";
  }
  set className(value) { this.classList = new Classes(); this.classList.add(...value.split(/\s+/).filter(Boolean)); }
  get className() { return [...this.classList.values].join(" "); }
  get options() { return this.querySelectorAll("option"); }
  get selectedOptions() { return this.options.filter((option) => option.value === this.value); }
  get value() { return this.tagName === "SELECT" && !this.options.some((option) => option.value === this._value) ? "" : this._value; }
  set value(value) { this._value = String(value); }
  append(...children) {
    children.forEach((child) => { child.parentElement = this; this.children.push(child); });
    if (this.tagName === "SELECT") {
      const selected = this.options.findLast((option) => option.selected);
      if (selected) this.value = selected.value;
      else if (!this.options.some((option) => option.value === this._value)) this.value = this.options[0]?.value || "";
    }
  }
  replaceChildren(...children) { this.children = []; this._value = ""; this.append(...children); }
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
  async fire(type) {
    for (const listener of this.listeners.get(type) || []) await listener({ type, currentTarget: this, target: this, preventDefault() {} });
  }
  matches(selector) {
    if (selector.startsWith(".")) return this.classList.contains(selector.slice(1));
    if (selector === "button[type=submit]") return this.tagName === "BUTTON" && this.type === "submit";
    return this.tagName === selector.toUpperCase();
  }
  querySelectorAll(selector) { return this.children.flatMap((child) => [...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector)]); }
  querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  focus() {}
  showModal() { this.open = true; }
  close() { this.open = false; this.dispatchEvent({ type: "close" }); }
  reset() {
    Object.values(this.elements).forEach((field) => { field.value = field.tagName === "SELECT" ? field.options[0]?.value || "" : ""; field.checked = false; });
  }
}

function harness() {
  const document = new Element("document");
  document.body = new Element("body");
  document.body.dataset.page = "test";
  document.elements = new Map();
  document.createElement = (tag) => new Element(tag);
  document.getElementById = (id) => document.elements.get(id) || null;
  document.querySelector = (selector) => selector === "#channel-form select[name=profile_id]" ? document.getElementById("channel-form")?.elements.profile_id : null;
  const window = new Element("window");
  window.location = { search: "", origin: "http://gateway.test" };
  const context = vm.createContext({
    document, window, location: window.location, console, URL, URLSearchParams,
    Event: class { constructor(type) { this.type = type; } },
    FormData: class {
      constructor(form) { this.form = form; }
      entries() { return Object.entries(this.form.elements).map(([name, field]) => [name, field.value]); }
    },
    confirm: () => true,
    fetch: async () => { throw new Error("unexpected fetch"); },
    setTimeout: () => 1,
    clearTimeout() {},
  });
  vm.runInContext(source, context, { filename: "app.js" });
  const add = (id, tag = "div") => {
    const element = new Element(tag);
    document.elements.set(id, element);
    return element;
  };
  const form = (id, fields) => {
    const element = add(id, "form");
    element.elements = Object.fromEntries(Object.entries(fields).map(([name, tag]) => [name, new Element(tag)]));
    const button = new Element("button"); button.type = "submit"; element.append(button);
    return element;
  };
  const run = (code) => vm.runInContext(code, context);
  run("messages = []; toast = (message) => messages.push(message)");
  return { context, document, window, add, form, run };
}

function channelHarness() {
  const h = harness();
  const form = h.form("channel-form", Object.fromEntries(["id", "name", "type", "base_url", "api_keys_raw", "models_raw", "headers_raw", "priority", "weight", "profile_id", "media_retention", "enabled", "fetch_models"].map((name) => [name, ["type", "profile_id", "media_retention"].includes(name) ? "select" : "input"])));
  for (const id of ["channel-dialog", "channel-dialog-title", "channel-id-row", "model-fetch-status"]) h.add(id);
  const picker = h.add("channel-protocol-picker", "select");
  h.run(`
    adapterTypes = [{ type: "openai", name: "OpenAI" }, { type: "anthropic", name: "Anthropic" }];
    channelProfiles = [{ id: "A", name: "Alpha", revision: 2 }];
    adapterTypes.forEach((item) => appendOption(byId("channel-form").elements.type, item.type, item.name));
    appendOption(byId("channel-form").elements.media_retention, "disabled", "disabled");
    renderChannelProfileOptions();
    setChannelModelOptions = () => {};
    renderModelMappings = () => {};
    serializeModelMappings = () => "{}";
    channelModelCandidates = () => [];
    loadChannels = async () => {};
    bindChannelForm();
  `);
  return { ...h, channelForm: form, picker, open: () => h.run('openChannelDialog({ id: "c1", type: "openai", enabled: true, fetch_models: false })') };
}

function profileHarness() {
  const h = harness();
  const form = h.form("profile-binding-form", { id: "input", channel_id: "select", operation: "select", model_pattern: "input", profile_revision: "input", precedence: "input", enabled: "input" });
  h.add("profile-binding-list", "tbody");
  h.add("profile-bindings-empty");
  h.run(`
    profiles = ["A", "B"].map((id) => ({ id, name: id, source: "custom", latest_revision: 1 }));
    profiles.forEach((profile) => profileRevisions.set(profile.id, [{ revision: 1, state: "draft", content_json: JSON.stringify({ operations: [{ operation: "images.create" }] }) }]));
    channels = [{ id: "c1", name: "Channel" }];
    selectedProfileID = "A"; selectedProfileRevision = 1;
    editorRenders = []; renderProfileEditor = () => editorRenders.push(selectedProfileID);
    renderProfileList = () => {};
    bindProfileManager();
  `);
  return { ...h, bindingForm: form };
}

function deferred() {
  let resolve, reject;
  const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
  return { promise, resolve, reject };
}
const turn = () => new Promise((resolve) => setImmediate(resolve));
const binding = (profileID = "A", id = "binding-a") => ({ id, channel_id: "c1", operation: "images.create", model_pattern: "*", precedence: 0, profile_id: profileID, profile_revision: 1, enabled: true });

test("restoring a channel preserves its bound revision through metadata refresh and picker changes", async () => {
  const h = channelHarness();
  h.context.request = async (url) => url === "/api/profiles"
    ? { data: [{ id: "A", name: "Alpha", source: "custom", latest_revision: 2 }] }
    : url.startsWith("/api/profiles/") ? { revision: { revision: 2, state: "published" } } : { data: [binding()] };
  h.open();
  await turn();
  assert.equal(h.channelForm.elements.profile_id.value, "A");
  assert.equal(h.channelForm.elements.profile_id.selectedOptions[0].dataset.revision, "1");
  assert.equal(h.picker.value, "profile:A");
  assert.equal(h.picker.selectedOptions[0].dataset.revision, "1");
  await h.run("loadChannelProfiles()");
  assert.equal(h.channelForm.elements.profile_id.selectedOptions[0].dataset.revision, "1");
  assert.equal(h.picker.selectedOptions[0].dataset.revision, "1");
  await h.picker.fire("change");
  assert.equal(h.channelForm.elements.profile_id.selectedOptions[0].dataset.revision, "1");
});

test("restoring an existing channel retains a Profile missing from the published list", async () => {
  const h = channelHarness();
  h.context.request = async () => ({ data: [binding("legacy")] });
  h.open();
  await turn();
  assert.equal(h.channelForm.elements.profile_id.value, "legacy");
  assert.equal(h.channelForm.elements.profile_id.selectedOptions[0].dataset.revision, "1");
  assert.equal(h.picker.value, "profile:legacy");
  assert.equal(h.picker.selectedOptions[0].dataset.revision, "1");
});

test("saving immediately waits for restoration and preserves unchanged existing bindings", async () => {
  const h = channelHarness();
  const lookup = deferred();
  const writes = [];
  h.context.request = async (url, options = {}) => {
    if (url.startsWith("/api/profile-bindings?")) return lookup.promise;
    writes.push({ url, options });
    return { channel: { id: "c1" } };
  };
  h.open();
  const save = h.channelForm.fire("submit");
  assert.equal(writes.length, 0, "channel saving waits for the binding lookup");
  lookup.resolve({ data: [binding()] });
  await save;
  assert.deepEqual(writes.map((item) => item.url), ["/api/channels"], "unchanged editing must not recreate or remove any binding");
  assert.equal(h.channelForm.elements.profile_id.selectedOptions[0].dataset.revision, "1");
});

test("failed lookup saves channel fields without silently removing its existing binding", async () => {
  const h = channelHarness();
  const writes = [];
  h.context.request = async (url) => {
    if (url.startsWith("/api/profile-bindings?")) throw new Error("lookup unavailable");
    writes.push(url);
    return { channel: { id: "c1" } };
  };
  h.open();
  await h.channelForm.fire("submit");
  assert.deepEqual(writes, ["/api/channels"]);
  assert.match(h.run("messages.at(-1)"), /已保留原绑定/);
});

test("an explicit protocol change during lookup wins and is saved", async () => {
  const h = channelHarness();
  const lookup = deferred();
  const writes = [];
  h.context.request = async (url, options = {}) => {
    if (url.startsWith("/api/profile-bindings?")) return lookup.promise;
    writes.push({ url, options });
    return { channel: { id: "c1" } };
  };
  h.open();
  h.picker.value = "builtin:anthropic";
  await h.picker.fire("change");
  const save = h.channelForm.fire("submit");
  lookup.resolve({ data: [binding()] });
  await save;
  assert.equal(h.picker.value, "builtin:anthropic");
  assert.equal(h.channelForm.elements.type.value, "anthropic");
  assert.deepEqual(writes.map((item) => item.url), ["/api/channels", "/api/channels/c1/bind-profile"]);
  assert.equal(writes[1].options.body.profile_id, "");
});

test("a save waiting on an old dialog cannot mutate a newly opened channel", async () => {
  const h = channelHarness();
  const oldLookup = deferred();
  const writes = [];
  let lookups = 0;
  h.context.request = async (url) => {
    if (url.startsWith("/api/profile-bindings?")) return ++lookups === 1 ? oldLookup.promise : { data: [] };
    writes.push(url);
    return { channel: { id: "c1" } };
  };
  h.open();
  const save = h.channelForm.fire("submit");
  h.document.getElementById("channel-dialog").close();
  h.run('openChannelDialog({ id: "c2", type: "anthropic", enabled: true })');
  oldLookup.resolve({ data: [binding()] });
  await save;
  assert.equal(h.channelForm.elements.id.value, "c2");
  assert.equal(h.channelForm.elements.profile_id.value, "");
  assert.deepEqual(writes, []);
});

test("switching Profiles resets binding edits before loading and preserves normal create/edit paths", async () => {
  const h = profileHarness();
  const loading = deferred();
  const writes = [];
  h.context.oldBinding = binding();
  h.run("profileBindings = [oldBinding]; renderProfileBindings(); populateProfileBindingForm(oldBinding)");
  h.context.request = async (url, options = {}) => {
    if (!options.method) return loading.promise;
    writes.push({ url, options });
    return {};
  };
  const select = h.run('selectProfile("B")');
  assert.equal(h.bindingForm.elements.id.value, "");
  assert.equal(h.bindingForm.dataset.profileId, "B");
  assert.equal(h.document.getElementById("profile-binding-list").children.length, 0, "old edit buttons are removed while B loads");
  h.run("populateProfileBindingForm(oldBinding)");
  assert.equal(h.bindingForm.elements.id.value, "", "an obsolete A edit callback must not repopulate B's form");
  const create = h.bindingForm.fire("submit");
  assert.equal(writes[0].url, "/api/profile-bindings");
  assert.equal(writes[0].options.method, "POST");
  assert.equal(writes[0].options.body.profile_id, "B");
  loading.resolve({ data: [binding("B", "binding-b")] });
  await Promise.all([select, create]);
  h.context.newBinding = binding("B", "binding-b");
  h.run("populateProfileBindingForm(newBinding)");
  await h.bindingForm.fire("submit");
  assert.equal(writes[1].url, "/api/profile-bindings/binding-b");
  assert.equal(writes[1].options.method, "PUT");
  assert.equal(writes[1].options.body.profile_id, "B");
});

test("a stale binding form is rejected instead of moving a binding to the selected Profile", async () => {
  const h = profileHarness();
  const writes = [];
  h.context.request = async (url) => { writes.push(url); return {}; };
  h.context.oldBinding = binding();
  h.run('populateProfileBindingForm(oldBinding); selectedProfileID = "B"');
  await h.bindingForm.fire("submit");
  assert.deepEqual(writes, []);
  assert.equal(h.bindingForm.elements.id.value, "");
  assert.equal(h.bindingForm.dataset.profileId, "B");
});

test("rapid A to B to A switches ignore both obsolete binding responses", async () => {
  const h = profileHarness();
  const loads = [deferred(), deferred(), deferred()];
  let index = 0;
  h.context.request = async () => loads[index++].promise;
  const first = h.run('selectProfile("A")');
  const second = h.run('selectProfile("B")');
  const third = h.run('selectProfile("A")');
  loads[2].resolve({ data: [binding("A", "current-a")] });
  await third;
  loads[1].resolve({ data: [binding("B", "old-b")] });
  loads[0].resolve({ data: [binding("A", "old-a")] });
  await Promise.all([first, second]);
  assert.equal(h.run("profileBindings[0].id"), "current-a");
  assert.equal(h.bindingForm.dataset.profileId, "A");
});

test("a pending binding save cannot clear another Profile's later edits", async () => {
  const h = profileHarness();
  const save = deferred();
  h.context.oldBinding = binding();
  h.run("populateProfileBindingForm(oldBinding)");
  const writes = [];
  h.context.request = async (url, options = {}) => {
    if (!options.method) return { data: [] };
    writes.push({ url, options });
    return save.promise;
  };
  const pending = h.bindingForm.fire("submit");
  await h.run('selectProfile("B")');
  h.bindingForm.elements.model_pattern.value = "unfinished-b-*";
  save.resolve({});
  await pending;
  assert.equal(writes[0].url, "/api/profile-bindings/binding-a");
  assert.equal(writes[0].options.body.profile_id, "A");
  assert.equal(h.bindingForm.elements.model_pattern.value, "unfinished-b-*");
  assert.equal(h.bindingForm.dataset.profileId, "B");
});

test("publishing freezes the Profile and revision throughout save, publish and refresh", async () => {
  const h = profileHarness();
  const save = deferred();
  const requests = [];
  h.window.location.search = "?profile=A";
  h.run('collectProfileFromEditor = () => ({ operations: [{ operation: "images.create" }] })');
  h.context.request = async (url, options = {}) => {
    requests.push({ url, options });
    if (options.method === "PUT") return save.promise;
    if (options.method === "POST") return {};
    if (url === "/api/profiles") return { data: h.run("profiles") };
    if (url.startsWith("/api/profiles/")) return { revision: { revision: 1, state: url.includes("/A?") ? "published" : "draft", content_json: '{"operations":[{"operation":"images.create"}]}' } };
    return { data: [] };
  };
  const publish = h.run('changeProfileRevisionState("publish")');
  assert.equal(requests[0].url, "/api/profiles/A/revisions/1");
  await h.run('selectProfile("B")');
  h.bindingForm.elements.model_pattern.value = "unfinished-b-*";
  const editorRendersBeforeRefresh = h.run("editorRenders.length");
  save.resolve({});
  await publish;
  assert.equal(requests.find((item) => item.options.method === "POST").url, "/api/profiles/A/revisions/1/publish");
  assert.equal(h.run("selectedProfileID"), "B", "the URL's initial Profile must not override the user's current choice");
  assert.equal(h.run("editorRenders.length"), editorRendersBeforeRefresh, "an A action must not redraw B's unfinished editor");
  assert.equal(h.bindingForm.elements.model_pattern.value, "unfinished-b-*");
  assert.equal(h.run('profileRevisions.get("A")[0].state'), "published");
});

test("initial Profile URL selection is honored and refresh keeps a later selection", async () => {
  const h = profileHarness();
  h.window.location.search = "?profile=B";
  h.run('selectedProfileID = ""; selectedProfileRevision = 0');
  h.context.request = async (url) => url === "/api/profiles" ? { data: h.run("profiles") }
    : url.startsWith("/api/profiles/") ? { revision: { revision: 1, state: "draft" } } : { data: [] };
  await h.run("loadProfiles()");
  assert.equal(h.run("selectedProfileID"), "B");
  await h.run('selectProfile("A")');
  await h.run("loadProfiles()");
  assert.equal(h.run("selectedProfileID"), "A");
  assert.equal(h.bindingForm.dataset.profileId, "A");
});

test("new channels still apply the explicitly chosen published Profile", async () => {
  const h = channelHarness();
  const writes = [];
  h.context.request = async (url, options = {}) => { writes.push({ url, options }); return { channel: { id: "new-channel" } }; };
  h.run("openChannelDialog()");
  h.channelForm.elements.fetch_models.checked = false;
  h.picker.value = "profile:A";
  await h.picker.fire("change");
  await h.channelForm.fire("submit");
  assert.equal(writes[1].url, "/api/channels/new-channel/bind-profile");
  assert.equal(writes[1].options.body.profile_id, "A");
  assert.equal(writes[1].options.body.profile_revision, 2);
});

test("switching revisions during publish keeps the new revision's editor intact", async () => {
  const h = profileHarness();
  const save = deferred();
  const requests = [];
  h.run(`
    profiles[0].latest_revision = 2;
    profileRevisions.get("A").push({ revision: 2, state: "draft" });
    collectProfileFromEditor = () => ({ operations: [{ operation: "images.create" }] });
  `);
  h.context.request = async (url, options = {}) => {
    requests.push({ url, options });
    if (options.method === "PUT") return save.promise;
    if (options.method === "POST") return {};
    if (url === "/api/profiles") return { data: h.run("profiles") };
    if (url.startsWith("/api/profiles/")) return { revision: { revision: Number(new URL(url, "http://gateway.test").searchParams.get("revision")), state: "draft" } };
    return { data: [] };
  };
  const publish = h.run('changeProfileRevisionState("publish")');
  h.run("selectedProfileRevision = 2; renderProfileEditor()");
  const renders = h.run("editorRenders.length");
  save.resolve({});
  await publish;
  assert.equal(requests.find((item) => item.options.method === "POST").url, "/api/profiles/A/revisions/1/publish");
  assert.equal(h.run("selectedProfileRevision"), 2);
  assert.equal(h.run("editorRenders.length"), renders);
});
