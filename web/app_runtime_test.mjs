import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

class FakeClassList {
  constructor() {
    this.values = new Set();
  }

  add(...values) {
    values.forEach((value) => this.values.add(value));
  }

  remove(...values) {
    values.forEach((value) => this.values.delete(value));
  }

  contains(value) {
    return this.values.has(value);
  }

  toggle(value, force) {
    const enabled = force === undefined ? !this.contains(value) : Boolean(force);
    if (enabled) this.add(value);
    else this.remove(value);
    return enabled;
  }
}

class FakeElement {
  constructor(tagName = "div") {
    this.tagName = String(tagName).toUpperCase();
    this.attributes = new Map();
    this.children = [];
    this.listeners = new Map();
    this.classList = new FakeClassList();
    this.dataset = {};
    this.style = {};
    this.textContent = "";
    this.loadCalls = 0;
    this.pauseCalls = 0;
    this.playCalls = 0;
  }

  set className(value) {
    this.classList = new FakeClassList();
    String(value).split(/\s+/).filter(Boolean).forEach((item) => this.classList.add(item));
  }

  get className() {
    return [...this.classList.values].join(" ");
  }

  set src(value) {
    this.setAttribute("src", value);
  }

  get src() {
    return this.getAttribute("src") || "";
  }

  set href(value) {
    this.setAttribute("href", value);
  }

  get href() {
    return this.getAttribute("href") || "";
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  }

  hasAttribute(name) {
    return this.attributes.has(name);
  }

  removeAttribute(name) {
    this.attributes.delete(name);
  }

  append(...children) {
    this.children.push(...children);
  }

  prepend(...children) {
    this.children.unshift(...children);
  }

  replaceChildren(...children) {
    this.children = [...children];
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }

  dispatchEvent(event) {
    event.target ||= this;
    event.currentTarget = this;
    for (const listener of this.listeners.get(event.type) || []) listener(event);
  }

  matches(selector) {
    if (selector.startsWith(".")) return this.classList.contains(selector.slice(1));
    return this.tagName === selector.toUpperCase();
  }

  querySelector(selector) {
    return this.querySelectorAll(selector)[0] || null;
  }

  querySelectorAll(selector) {
    const matches = [];
    for (const child of this.children) {
      if (!(child instanceof FakeElement)) continue;
      if (child.matches(selector)) matches.push(child);
      matches.push(...child.querySelectorAll(selector));
    }
    return matches;
  }

  closest() {
    return null;
  }

  load() {
    this.loadCalls += 1;
  }

  pause() {
    this.pauseCalls += 1;
  }

  play() {
    this.playCalls += 1;
    return Promise.resolve();
  }

  remove() {}

  focus() {}
}

class FakeDocument {
  constructor() {
    this.elements = new Map();
    this.listeners = new Map();
    this.visibilityState = "visible";
    this.body = new FakeElement("body");
  }

  createElement(tagName) {
    return new FakeElement(tagName);
  }

  getElementById(id) {
    if (!this.elements.has(id)) this.elements.set(id, new FakeElement("div"));
    return this.elements.get(id);
  }

  querySelectorAll() {
    return [];
  }

  addEventListener(type, listener) {
    const listeners = this.listeners.get(type) || [];
    listeners.push(listener);
    this.listeners.set(type, listeners);
  }
}

function response(status, data) {
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: { get: () => "application/json" },
    json: async () => data,
    text: async () => JSON.stringify(data),
  };
}

function logDetail(id, status = "processing", progress = 25) {
  return {
    log: {
      id,
      method: "POST",
      path: "/v1/videos",
      outcome: "success",
      status_code: 200,
      duration_ms: 10,
      async_task_kind: "video",
      async_task_id: "video-task",
      async_task_status: status,
      async_poll_count: 1,
      async_result_body: JSON.stringify({ status, progress }),
    },
    events: [],
  };
}

const timers = new Map();
let nextTimerID = 1;
const document = new FakeDocument();
const fetchedPaths = [];
let fetchImpl = async () => response(500, { error: "fetch response not configured" });
const window = {
  addEventListener() {},
  open: () => null,
};
const context = vm.createContext({
  console,
  document,
  window,
  location: { origin: "http://gateway.test", assign() {} },
  navigator: { clipboard: { writeText: async () => {} } },
  fetch: async (requestPath, options) => {
    fetchedPaths.push(String(requestPath));
    return fetchImpl(requestPath, options);
  },
  FormData: class { entries() { return []; } },
  URL,
  URLSearchParams,
  confirm: () => true,
  setTimeout: (callback, delay) => {
    const id = nextTimerID++;
    timers.set(id, { callback, delay });
    return id;
  },
  clearTimeout: (id) => timers.delete(id),
});

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, "assets", "app.js"), "utf8");
vm.runInContext(source, context, { filename: "app.js" });

const figure = vm.runInContext(
  `createMediaFigure("/api/playground/video-content/task", "video", "video")`,
  context,
);
for (const [unsafe, type] of [
  ["javascript:alert(1)", "image"],
  ["data:text/html,<script>alert(1)</script>", "image"],
  ["data:image/svg+xml,<svg onload=alert(1) />", "image"],
  ["data:image/png;base64,AAAA", "video"],
  ["//attacker.example/image.png", "image"],
]) {
  assert.equal(vm.runInContext(`normalizeMediaURL(${JSON.stringify(unsafe)}, ${JSON.stringify(type)})`, context), null, `unsafe media URL should be rejected: ${unsafe}`);
}
assert.equal(
  vm.runInContext(`normalizeMediaURL("data:image/png;base64,AAAA", "image")`, context),
  "data:image/png;base64,AAAA",
  "safe image data URL should remain usable",
);
const videoResultBody = JSON.stringify({
  type: "video_task",
  data: [{ url: "/v1/videos/task/content" }],
  video_url: "/v1/videos/task/content",
});
const nestedImageResultBody = JSON.stringify({
  status: "completed",
  raw: { job: { assets: [{ proxy_url: "https://cdn.example/generated.png" }] } },
  raw_payload: { job: { assets: [{ proxy_url: "https://cdn.example/generated.png" }] } },
});
assert.deepEqual(
  JSON.parse(vm.runInContext(
    `JSON.stringify(extractImagesFromResponse(${JSON.stringify(nestedImageResultBody)}, { async_task_kind: "image", path: "/v1/images" }))`,
    context,
  )),
  ["https://cdn.example/generated.png"],
  "nested durable image payloads should be parsed and de-duplicated",
);
const redactedImageResultBody = JSON.stringify({
  response: JSON.stringify({ data: [{ url: "/v1/media/image/%5BREDACTED%5D" }] }),
});
assert.deepEqual(
  JSON.parse(vm.runInContext(
    `JSON.stringify(extractImagesFromResponse(${JSON.stringify(redactedImageResultBody)}, { async_task_kind: "image" }))`,
    context,
  )),
  [],
  "redacted media capability URLs must not create a broken image preview",
);
assert.deepEqual(
  JSON.parse(vm.runInContext(
    `JSON.stringify(extractImagesFromResponse(${JSON.stringify(videoResultBody)}, { async_task_kind: "video", path: "/v1/videos" }))`,
    context,
  )),
  [],
  "video data URLs must not create a failed image card",
);
assert.equal(
  vm.runInContext(
    `extractVideoFromResponse(${JSON.stringify(videoResultBody)}, { async_task_kind: "video", path: "/v1/videos" })`,
    context,
  ),
  "/v1/videos/task/content",
  "nested video result URLs should be rendered in the log preview",
);
const nestedVideoResultBody = JSON.stringify({
  raw_payload: { type: "video_task", data: [{ url: "https://cdn.example/generated.mp4" }] },
});
assert.deepEqual(
  JSON.parse(vm.runInContext(
    `JSON.stringify(extractImagesFromResponse(${JSON.stringify(nestedVideoResultBody)}, { async_task_kind: "video" }))`,
    context,
  )),
  [],
  "nested video payloads must not create image cards",
);
assert.equal(
  vm.runInContext(
    `extractVideoFromResponse(${JSON.stringify(nestedVideoResultBody)}, { async_task_kind: "video" })`,
    context,
  ),
  "https://cdn.example/generated.mp4",
  "nested durable video payloads should be rendered",
);
assert.equal(
  vm.runInContext(
    `normalizeLogMediaURL("http://old-gateway.example/api/playground/video-content/task?channel_id=legacy")`,
    context,
  ),
  "/api/playground/video-content/task?channel_id=legacy",
  "historical absolute gateway media URLs should follow the current console origin",
);
assert.deepEqual(
  JSON.parse(vm.runInContext(`JSON.stringify(managedLogMediaURLs([
    { id: 4, kind: "video", status: "available", ordinal: 1, public_url: "/v1/media/video-4/cap-4" },
    { id: 3, kind: "video", status: "available", ordinal: 0, publicUrl: "/v1/media/video-3/cap-3" },
    { id: 2, kind: "video", status: "pending", ordinal: 2 },
  ], "video"))`, context)),
  ["/v1/media/video-3/cap-3", "/v1/media/video-4/cap-4"],
  "available managed videos should expose only their public capability URLs",
);
assert.deepEqual(
  JSON.parse(vm.runInContext(`JSON.stringify(managedLogMediaURLs([
    { id: 4, kind: "video", status: "available", ordinal: 1 },
  ], "video"))`, context)),
  [],
  "managed content URLs must not be treated as public URLs when capability data is absent",
);
const video = figure.querySelector("video");
assert.ok(video, "video preview should be created");
assert.equal(video.controls, true, "native video controls should own playback");
assert.equal(figure.querySelector(".media-video-overlay"), null, "video preview must not render a second playback layer");
assert.equal(video.hasAttribute("src"), true, "video must have source for first frame preview");
assert.equal(video.getAttribute("preload"), "metadata", "video should preload metadata for first frame");
const reloadButton = figure.querySelectorAll("button")[0];
assert.ok(reloadButton, "video reload control should be created");
reloadButton.dispatchEvent({ type: "click" });
assert.ok(video.src.includes("/api/playground/video-content/task"));
assert.equal(video.pauseCalls, 1, "reload should stop the old decoder before replacing its source");
assert.equal(video.loadCalls, 2, "reload should clear the old frame before loading the source again");
assert.equal(video.playCalls, 1);

const posterFigure = vm.runInContext(
  `createMediaFigure("/api/playground/video-content/task", "video", "video", { poster: "/first-frame.png" })`,
  context,
);
assert.equal(posterFigure.querySelector("video")?.getAttribute("poster"), null, "video poster must NOT be set to prevent ghosting artifacts");
const managedOnlyFigure = vm.runInContext(
  `createMediaFigure("/api/media-assets/9/content", "video", "video", { publicURL: null })`,
  context,
);
assert.equal(managedOnlyFigure.querySelector("a"), null, "admin-only log previews must not expose open/download links");
assert.ok(managedOnlyFigure.querySelector("button"), "admin-only log previews may retain playback controls");

const mediaStorage = new Map([["media_view_mode", "retired-layout"]]);
context.localStorage = {
  getItem: (key) => mediaStorage.get(key) ?? null,
  setItem: (key, value) => mediaStorage.set(key, value),
};
fetchImpl = async (requestPath) => response(200, requestPath === "/api/auth/me"
  ? { username: "admin", csrf_token: "csrf" }
  : { data: [] });
await vm.runInContext("initMedia()", context);
assert.equal(document.getElementById("media-grid").classList.contains("hidden"), false, "invalid saved media view should show the grid");
assert.equal(document.getElementById("media-table-wrap").classList.contains("hidden"), true, "invalid saved media view should hide the table");
assert.equal(mediaStorage.get("media_view_mode"), "grid", "invalid saved media view should be replaced with a usable mode");

const imagePollPathStart = fetchedPaths.length;
let imagePollCalls = 0;
fetchImpl = async () => {
  imagePollCalls += 1;
  return response(200, {
    status: "ok",
    task_id: "image-task",
    task_status: imagePollCalls === 1 ? "processing" : "completed",
    images: ["https://cdn.example/image.png"],
  });
};
const imagePoll = vm.runInContext(`(async () => {
  playgroundPollToken = 41;
  await pollPlaygroundImage({ task_id: "image-task", channel: "image-channel" }, 41);
})()`, context);
await new Promise((resolve) => setImmediate(resolve));
assert.equal(document.getElementById("playground-media").querySelectorAll("img").length, 1, "provider image should render while local media is still materializing");
assert.equal(document.getElementById("playground-status").textContent, "图片已生成，本地托管处理中…");
const imageDelay = [...timers.entries()].find(([, timer]) => timer.delay === 4000);
assert.ok(imageDelay, "non-terminal image task should schedule another status poll");
timers.delete(imageDelay[0]);
imageDelay[1].callback();
await imagePoll;
assert.ok(
  fetchedPaths.slice(imagePollPathStart).some((requestPath) => requestPath.includes("/api/playground/image-status?") && requestPath.includes("task_id=image-task")),
  "async image tasks should poll the dashboard image status endpoint",
);
assert.equal(document.getElementById("playground-status").textContent, "图片生成完成");
assert.equal(document.getElementById("playground-media").querySelectorAll("img").length, 1);


let resolveOld;
let resolveCurrent;
fetchImpl = (requestPath) => new Promise((resolve) => {
  if (String(requestPath).endsWith("/old")) resolveOld = resolve;
  else resolveCurrent = resolve;
});
const oldRequest = vm.runInContext(`openLog("old")`, context);
const currentRequest = vm.runInContext(`openLog("current")`, context);
resolveOld(response(200, logDetail("old")));
await oldRequest;
assert.notEqual(vm.runInContext(`activeLogRecord?.id`, context), "old", "stale log response must be ignored");
resolveCurrent(response(200, logDetail("current")));
await currentRequest;
assert.equal(vm.runInContext(`activeLogRecord.id`, context), "current");
assert.equal(timers.size, 1, "only one detail refresh timer may remain");
assert.equal([...timers.values()][0].delay, 4000);
vm.runInContext(`summaryChildBeforeRefresh = byId("detail-summary").children[0]`, context);
const responseTextBeforeRefresh = vm.runInContext(`byId("visual-response-body").textContent`, context);
fetchImpl = async () => response(200, logDetail("current"));
await vm.runInContext(`openLog("current")`, context);
assert.equal(
  vm.runInContext(`byId("detail-summary").children[0] === summaryChildBeforeRefresh`, context),
  true,
  "unchanged log refreshes must preserve the existing summary DOM",
);
assert.equal(
  vm.runInContext(`byId("visual-response-body").textContent`, context),
  responseTextBeforeRefresh,
  "unchanged log refreshes must keep one response text render",
);
assert.equal(timers.size, 1, "reopening a refreshing log must still keep one timer");

const terminalDetail = logDetail("terminal", "completed", 100);
terminalDetail.media_assets = [{
  id: 9,
  kind: "video",
  status: "available",
  ordinal: 0,
  public_url: "/v1/media/video-9/cap-9",
}];
fetchImpl = async () => response(200, terminalDetail);
await vm.runInContext(`openLog("terminal")`, context);
assert.equal(timers.size, 0, "terminal tasks must stop refreshing");
assert.equal(document.getElementById("visual-response-body").classList.contains("hidden"), true, "playable media should replace the duplicate structured response text");
assert.equal(document.getElementById("copy-visual-response").classList.contains("hidden"), true, "hidden structured response text must not leave an orphan copy action");
assert.equal(document.getElementById("visual-media-view").querySelectorAll("video").length, 1, "completed video logs should render one player inside the response");
vm.runInContext(`terminalVideoBeforeRefresh = byId("visual-media-view").querySelector("video")`, context);
await vm.runInContext(`openLog("terminal")`, context);
assert.equal(
  vm.runInContext(`byId("visual-media-view").querySelector("video") === terminalVideoBeforeRefresh`, context),
  true,
  "unchanged terminal log renders must preserve the existing video node",
);

const synchronousImageDetail = {
  log: {
    id: "sync-image",
    method: "POST",
    path: "/api/playground/run",
    outcome: "success",
    status_code: 200,
    duration_ms: 100,
    request_body: JSON.stringify({ kind: "image", prompt: "test" }),
    response_body: JSON.stringify({ images: ["http://localhost:8000/v1/media/image/%5BREDACTED%5D"] }),
  },
  events: [],
  media_assets: [{ id: 35, kind: "image", status: "available", ordinal: 0, public_url: "/v1/media/image/capability" }],
};
fetchImpl = async () => response(200, synchronousImageDetail);
await vm.runInContext(`openLog("sync-image")`, context);
const synchronousLogImage = document.getElementById("visual-media-view").querySelector("img");
assert.ok(synchronousLogImage, "synchronous image log should render its managed media asset");
assert.equal(synchronousLogImage.getAttribute("src"), "/api/media-assets/35/content", "redacted audit URL must not be used for the image preview");

fetchImpl = async () => response(200, logDetail("visible"));
await vm.runInContext(`openLog("visible")`, context);
assert.equal(timers.size, 1);
document.visibilityState = "hidden";
vm.runInContext(`handleActiveLogVisibilityChange()`, context);
assert.equal(timers.size, 0, "hidden pages must stop refreshing");
document.visibilityState = "visible";
vm.runInContext(`handleActiveLogVisibilityChange()`, context);
assert.equal(timers.size, 1, "a visible page should resume its active non-terminal log");
assert.equal([...timers.values()][0].delay, 0);

fetchImpl = async () => response(500, { error: "temporary" });
await vm.runInContext(
  `openLog(activeLogID, { background: true, token: activeLogRequestToken })`,
  context,
);
assert.equal(timers.size, 1, "a transient error should retain one retry timer");
assert.equal([...timers.values()][0].delay, 8000, "the first transient retry should back off");

fetchImpl = async () => response(404, { error: "deleted" });
await vm.runInContext(
  `openLog(activeLogID, { background: true, token: activeLogRequestToken })`,
  context,
);
assert.equal(timers.size, 0, "a permanently missing log must stop refreshing");
assert.equal(vm.runInContext(`activeLogRefreshDisabled`, context), true);
assert.equal(
  fetchedPaths.some((requestPath) => requestPath.includes("video-status")),
  false,
  "log inspection must not poll an upstream video task",
);

// Exercise actual form round trips: a hidden default poll used to make every
// newly created synchronous profile fail backend validation when saved.
for (const kind of ["chat", "image", "video"]) {
  for (const mode of ["direct", "async"]) {
    if (kind === "chat" && mode === "async") continue;
    vm.runInContext(`
      byId("profile-json").value = JSON.stringify(defaultProfileContent("example", ${JSON.stringify(kind)}, ${JSON.stringify(mode)}));
      renderProfileOperations(JSON.parse(byId("profile-json").value));
    `, context);
    const profile = JSON.parse(vm.runInContext(`JSON.stringify(collectProfileFromEditor())`, context));
    const operation = profile.operations[0];
    assert.equal(operation.execution_mode, mode);
    assert.equal(Object.hasOwn(operation, "media_retention"), false, "new profiles must leave media saving to the channel");
    assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-op-retention"), null, "Profile operations must not expose media retention");
    if (mode === "direct") {
      assert.equal(operation.polling_mode, "off");
      assert.equal(operation.poll, undefined, "synchronous saves must omit hidden polling defaults");
    } else {
      assert.equal(operation.polling_mode, "background");
      assert.ok(operation.response.task_id_paths.length);
      assert.ok(operation.poll.success_values.length);
      assert.ok(operation.poll.failure_values.length);
      assert.ok(operation.poll.result_url_paths.length);
    }
  }
}
const imageAsyncProfile = JSON.parse(vm.runInContext(`JSON.stringify(defaultProfileContent("fanren", "image", "async"))`, context));
const imageAsyncOperation = imageAsyncProfile.operations[0];
assert.equal(imageAsyncOperation.submit.path, "/images/jobs", "async image creation should use the documented jobs endpoint");
assert.deepEqual(imageAsyncOperation.response.task_id_paths.slice(0, 1), ["job.id"], "async image tasks should read the nested job ID");
assert.equal(imageAsyncOperation.poll.path, "/images/jobs/{task_id}");
assert.equal(imageAsyncOperation.poll.status_path, "job.status");
assert.equal(imageAsyncOperation.poll.interval_ms, 5000);
assert.equal(imageAsyncOperation.poll.max_duration_ms, 1800000);
assert.ok(imageAsyncOperation.poll.result_url_paths.includes("job.assets.0.proxy_url"), "async image polling should read documented proxy URLs");
const videoAsyncProfile = JSON.parse(vm.runInContext(`JSON.stringify(defaultProfileContent("fanren", "video", "async"))`, context));
const videoAsyncOperation = videoAsyncProfile.operations[0];
assert.equal(videoAsyncOperation.submit.path, "/videos/generations");
assert.equal(videoAsyncOperation.poll.path, "/videos/generations/{task_id}");
assert.equal(videoAsyncOperation.poll.status_path, "status");
assert.equal(videoAsyncOperation.content.path, "/videos/generations/{task_id}/content", "async video profiles should expose the documented content endpoint");
const operationCard = document.getElementById("profile-operation-editor").querySelector(".profile-operation-card");
const executionMode = operationCard.querySelector(".profile-op-execution");
assert.equal(operationCard.querySelector("strong").textContent, "视频生成", "operation cards should use readable capability names");
assert.equal(operationCard.querySelector("code").textContent, "video.create", "operation cards should retain the technical operation name");
assert.equal(operationCard.querySelector(".profile-op-polling").value, "background");
assert.equal(operationCard.querySelectorAll("select").some((field) => field.classList.contains("profile-op-polling")), false, "do not expose a polling strategy dropdown");
assert.equal(document.getElementById("profile-operation-editor").querySelectorAll(".profile-add-operation").length, 1);
assert.deepEqual(
  document.getElementById("profile-operation-editor").querySelector(".profile-add-kind").querySelectorAll("option").map((option) => option.value),
  ["chat", "image", "video"],
  "the editor must allow all channel capabilities in one profile",
);
const multiOperationProfile = JSON.parse(vm.runInContext(`JSON.stringify(defaultMultiOperationProfileContent("example"))`, context));
assert.deepEqual(multiOperationProfile.operations.map((operation) => operation.operation), ["chat.completions", "images.create", "video.create"]);
assert.equal(multiOperationProfile.name, "example");
assert.ok(multiOperationProfile.operations.every((operation) => !Object.hasOwn(operation, "media_retention")), "multi-operation templates must omit media retention");
const selectedOperations = JSON.parse(vm.runInContext(`JSON.stringify(defaultMultiOperationProfileContent("selected", ["chat", "video"]))`, context));
assert.deepEqual(selectedOperations.operations.map((operation) => operation.operation), ["chat.completions", "video.create"]);
assert.deepEqual(
  ["chat.completions", "images.create", "video.create", "moderations.create", "messages.count_tokens"].map((name) => vm.runInContext(`profileOperationLabel(${JSON.stringify(name)})`, context)),
  ["聊天", "图片生成", "视频生成", "内容审核", "Token 计数"],
  "default and extended operations should have readable labels",
);
assert.equal(vm.runInContext(`profileOperationLabel("custom.moderation.v2")`, context), "内容审核", "keyword moderation should map to 内容审核");
assert.equal(vm.runInContext(`profileOperationLabel("custom.reranker")`, context), "文本重排", "keyword rerank should map to 文本重排");
const originalFormData = context.FormData;
const originalRequest = vm.runInContext("request", context);
const originalLoadProfiles = vm.runInContext("loadProfiles", context);
context.FormData = class {
  entries() {
    return Object.entries({ name: "Multi Provider" });
  }
};
document.getElementById("profile-create-chat").checked = true;
document.getElementById("profile-create-image").checked = false;
document.getElementById("profile-create-video").checked = true;
document.getElementById("profile-dialog").close = () => {};
vm.runInContext(`
  request = async (path, options) => { globalThis.createdProfileRequest = { path, options }; return { profile: { id: "generated-id" }, revision: { revision: 1 } }; };
  loadProfiles = async () => {};
`, context);
await vm.runInContext("createProfile()", context);
const createdProfileRequest = vm.runInContext("createdProfileRequest", context);
assert.equal(createdProfileRequest.path, "/api/profiles");
assert.equal(createdProfileRequest.options.body.id, undefined, "the server generates the internal ID");
assert.deepEqual(Array.from(createdProfileRequest.options.body.profile.operations, (operation) => operation.operation), ["chat.completions", "video.create"]);
assert.ok(Array.from(createdProfileRequest.options.body.profile.operations).every((operation) => !Object.hasOwn(operation, "media_retention")), "new Profile creation must not submit operation retention defaults");
assert.equal(vm.runInContext("selectedProfileID", context), "generated-id");
document.getElementById("profile-create-chat").checked = false;
document.getElementById("profile-create-video").checked = false;
await assert.rejects(vm.runInContext("createProfile()", context), /至少选择一种功能/);
context.FormData = originalFormData;
context.request = originalRequest;
context.loadProfiles = originalLoadProfiles;

vm.runInContext(`
  selectedProfileID = "example";
  selectedProfileRevision = 1;
  profileRevisions.set("example", [{ revision: 1, state: "published" }]);
  setProfileEditorReadOnly(true);
`, context);
assert.equal(executionMode.disabled, true);
assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-add-operation").classList.contains("hidden"), true, "add operation button must be hidden for read-only published revisions");
assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-add-operation-section").classList.contains("hidden"), true, "add operation section must be hidden for read-only published revisions");
assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-remove-operation").classList.contains("hidden"), true, "remove operation button must be hidden for read-only published revisions");
let syncCalled = false;
const testSelect = document.getElementById("profile-operation-editor").querySelector("select");
if (testSelect) testSelect._syncCustomSelectDisabled = () => { syncCalled = true; };
vm.runInContext(`setProfileEditorReadOnly(true);`, context);
assert.equal(syncCalled, true, "setProfileEditorReadOnly must invoke _syncCustomSelectDisabled on selects");
assert.notEqual(document.getElementById("profile-revision-select").disabled, true, "published revisions must remain switchable");
assert.notEqual(document.getElementById("profile-binding-form").disabled, true, "bindings remain editable after publishing");
assert.equal(document.getElementById("new-profile-revision").textContent, "基于此版本修改", "published revisions should expose an explicit editable-draft action");
assert.equal(document.getElementById("delete-profile-revision").disabled, false, "unbound published revisions should be deletable");
assert.equal(document.getElementById("publish-profile-revision").disabled, true);
assert.equal(document.getElementById("retire-profile-revision").disabled, false);
vm.runInContext(`profileBindings = [{ profile_id: "example", profile_revision: 1 }]; updateProfileRevisionDeleteState();`, context);
assert.equal(document.getElementById("delete-profile-revision").disabled, true, "bound revisions must not be deletable");
vm.runInContext(`profileBindings = []; profileRevisions.set("example", [{ revision: 1, state: "draft" }]); setProfileEditorReadOnly(false);`, context);
assert.equal(document.getElementById("delete-profile-revision").disabled, false, "draft revisions should expose deletion");
assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-add-operation").classList.contains("hidden"), false, "add operation button must be visible for draft revisions");
assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-add-operation-section").classList.contains("hidden"), false, "add operation section must be visible for draft revisions");
assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-remove-operation").classList.contains("hidden"), false, "remove operation button must be visible for draft revisions");

vm.runInContext(`
  profiles.push({ id: "empty-profile", name: "Empty Profile", source: "custom", latest_revision: 0 });
  selectedProfileID = "empty-profile";
  selectedProfileRevision = 0;
  profileRevisions.set("empty-profile", []);
  renderProfileEditor();
`, context);
assert.equal(document.getElementById("profile-editor").classList.contains("hidden"), false, "profile editor must remain visible for 0-revision profiles");
assert.equal(document.getElementById("delete-profile").classList.contains("hidden"), false, "delete-profile button must remain visible for custom 0-revision profiles");
assert.equal(document.getElementById("delete-profile").disabled, false, "delete-profile button must remain enabled for custom 0-revision profiles");
assert.equal(document.getElementById("new-profile-revision").disabled, false, "new-profile-revision button must remain enabled for 0-revision profiles");
assert.equal(document.getElementById("profile-editor-title").textContent, "Empty Profile");

for (const strategy of ["client", "gateway_wait", "background"]) {
  vm.runInContext(`{
    const legacy = defaultProfileContent("legacy", "video", "async");
    legacy.operations[0].polling_mode = ${JSON.stringify(strategy)};
    byId("profile-json").value = JSON.stringify(legacy);
    renderProfileOperations(legacy);
  }`, context);
  assert.equal(vm.runInContext(`collectProfileFromEditor().operations[0].polling_mode`, context), strategy, "editing legacy profiles must preserve their execution strategy");
}

// Removing the Profile setting must not rewrite historical revision JSON or
// silently drop an old draft's retention field when another field is edited.
for (const retention of ["disabled", "best_effort", "required"]) {
  const legacyProfile = JSON.parse(vm.runInContext(`JSON.stringify(defaultMultiOperationProfileContent("legacy"))`, context));
  legacyProfile.operations[1].media_retention = retention;
  const legacyJSON = JSON.stringify(legacyProfile);
  vm.runInContext(`
    byId("profile-json").value = ${JSON.stringify(legacyJSON)};
    renderProfileOperations(JSON.parse(byId("profile-json").value));
  `, context);
  assert.equal(document.getElementById("profile-json").value, legacyJSON, "rendering a legacy revision must not rewrite its JSON");
  assert.equal(document.getElementById("profile-operation-editor").querySelector(".profile-op-retention"), null);
  document.getElementById("profile-operation-editor").querySelectorAll(".profile-op-policy")[1].value = "edited-policy";
  const collected = JSON.parse(vm.runInContext("JSON.stringify(collectProfileFromEditor())", context));
  assert.equal(collected.operations[1].media_retention, retention, "editing another field retains the original legacy value on that operation");
  assert.equal(collected.operations[1].policy, "edited-policy");
  assert.equal(Object.hasOwn(collected.operations[0], "media_retention"), false, "an absent legacy field stays absent");
  assert.equal(Object.hasOwn(collected.operations[2], "media_retention"), false, "retention must not leak to another operation");
  vm.runInContext("syncProfileJSONFromEditor()", context);
  assert.equal(JSON.parse(document.getElementById("profile-json").value).operations[1].media_retention, retention, "JSON synchronization preserves legacy retention");
}
vm.runInContext(`
  selectedProfileID = "legacy-retention";
  selectedProfileRevision = 1;
  profileRevisions.set(selectedProfileID, [{ revision: 1, state: "draft" }]);
  request = async (path, options) => { globalThis.savedLegacyProfileRequest = { path, options }; return {}; };
  loadProfiles = async () => {};
`, context);
await vm.runInContext("saveProfileRevision()", context);
const savedLegacyProfileRequest = vm.runInContext("savedLegacyProfileRequest", context);
assert.equal(savedLegacyProfileRequest.options.method, "PUT");
assert.equal(savedLegacyProfileRequest.options.body.profile.operations[1].media_retention, "required", "saving an existing draft keeps its legacy JSON field");
context.request = originalRequest;
context.loadProfiles = originalLoadProfiles;

// Test collapsible operation cards and bulk toolbar
const multiOps = JSON.parse(vm.runInContext(`JSON.stringify(defaultMultiOperationProfileContent("test-ops", ["chat", "image", "video"]))`, context));
vm.runInContext(`renderProfileOperations(${JSON.stringify(multiOps)})`, context);
const renderedCards = document.getElementById("profile-operation-editor").querySelectorAll(".profile-operation-card");
assert.equal(renderedCards.length, 3, "should render 3 cards");
assert.equal(renderedCards[0].classList.contains("collapsed"), false, "1st card should be expanded by default");
assert.equal(renderedCards[1].classList.contains("collapsed"), true, "2nd card should be collapsed by default");
assert.equal(renderedCards[2].classList.contains("collapsed"), true, "3rd card should be collapsed by default");

// Verify ARIA role, tabindex and initial aria-expanded
const header1 = renderedCards[1].querySelector(".profile-operation-header");
assert.equal(header1.getAttribute("role"), "button", "header should have role='button'");
assert.equal(header1.getAttribute("tabindex"), "0", "header should have tabindex='0'");
assert.equal(header1.getAttribute("aria-expanded"), "false", "collapsed card header should have aria-expanded='false'");

// Click header to toggle
header1.dispatchEvent({ type: "click" });
assert.equal(renderedCards[1].classList.contains("collapsed"), false, "clicking header expands card");
assert.equal(header1.getAttribute("aria-expanded"), "true", "expanded card header should have aria-expanded='true'");

// Keydown Enter / Space to toggle
header1.dispatchEvent({ type: "keydown", key: "Enter", preventDefault: () => {} });
assert.equal(renderedCards[1].classList.contains("collapsed"), true, "Enter key collapses card");
assert.equal(header1.getAttribute("aria-expanded"), "false", "collapsed card header should have aria-expanded='false'");
header1.dispatchEvent({ type: "keydown", key: " ", preventDefault: () => {} });
assert.equal(renderedCards[1].classList.contains("collapsed"), false, "Space key expands card");
assert.equal(header1.getAttribute("aria-expanded"), "true", "expanded card header should have aria-expanded='true'");

header1.dispatchEvent({ type: "click" });
assert.equal(renderedCards[1].classList.contains("collapsed"), true, "clicking header collapses card again");
assert.equal(header1.getAttribute("aria-expanded"), "false");

// Bulk collapse all and expand all
const expandBtn = document.getElementById("profile-operation-editor").querySelector(".profile-expand-all");
const collapseBtn = document.getElementById("profile-operation-editor").querySelector(".profile-collapse-all");
assert.ok(expandBtn, "expand-all button should exist in toolbar");
assert.ok(collapseBtn, "collapse-all button should exist in toolbar");
expandBtn.dispatchEvent({ type: "click" });
assert.equal(Array.from(renderedCards).every((c) => !c.classList.contains("collapsed")), true, "expand all should expand every card");
assert.equal(Array.from(renderedCards).every((c) => c.querySelector(".profile-operation-header").getAttribute("aria-expanded") === "true"), true, "expand all should set aria-expanded='true'");
collapseBtn.dispatchEvent({ type: "click" });
assert.equal(Array.from(renderedCards).every((c) => c.classList.contains("collapsed")), true, "collapse all should collapse every card");
assert.equal(Array.from(renderedCards).every((c) => c.querySelector(".profile-operation-header").getAttribute("aria-expanded") === "false"), true, "collapse all should set aria-expanded='false'");

// Verify mobile nav and sidebar backdrop presence across all shell pages
for (const pageName of ["dashboard.html", "profiles.html", "media.html", "logs.html"]) {
  const pageHtml = fs.readFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)), pageName), "utf8");
  assert.ok(pageHtml.includes('id="mobile-nav-toggle"'), `${pageName} must include #mobile-nav-toggle`);
  assert.ok(pageHtml.includes('id="sidebar-backdrop"'), `${pageName} must include #sidebar-backdrop`);
  assert.ok(pageHtml.includes('id="app-sidebar"'), `${pageName} must include #app-sidebar`);
}

fetchImpl = async () => response(401, { error: "用户名或密码错误" });
await assert.rejects(vm.runInContext(`request("/api/auth/login", { method: "POST" })`, context), /用户名或密码错误/);
fetchImpl = async () => { throw new TypeError("Failed to fetch"); };
await assert.rejects(vm.runInContext(`request("/api/profiles")`, context), /无法连接网关/);
fetchImpl = async () => ({ status: 502, ok: false, headers: { get: () => "text/html" }, text: async () => "<html>proxy failure</html>" });
await assert.rejects(vm.runInContext(`request("/api/profiles")`, context), /请求失败（HTTP 502）/);
fetchImpl = async () => ({ status: 200, ok: true, headers: { get: () => "application/json" }, json: async () => { throw new SyntaxError("unexpected token"); } });
await assert.rejects(vm.runInContext(`request("/api/profiles")`, context), /数据格式异常/);
assert.throws(() => vm.runInContext(`profileJSON("{")`, context), /JSON 格式错误/);
assert.equal(vm.runInContext(`formatMediaSize("invalid")`, context), "—");
fetchImpl = async () => ({ status: 401, ok: false, headers: { get: () => "application/json" }, json: async () => { throw new SyntaxError("empty"); } });
await assert.rejects(vm.runInContext(`request("/api/profiles")`, context), /登录已过期/);
// Model native select behavior: assigning an absent option clears its value.
// A plain value property would miss the legacy-channel editing regression.
class ChannelSelect extends FakeElement {
  constructor() { super("select"); this._value = ""; }
  get options() { return this.querySelectorAll("option"); }
  get selectedOptions() { return this.options.filter((option) => option.value === this.value); }
  get value() { return this.options.some((option) => option.value === this._value) ? this._value : ""; }
  set value(value) { this._value = this.options.some((option) => option.value === value) ? value : ""; }
  append(...children) {
    super.append(...children);
    const selected = children.find((child) => child.selected);
    if (selected) this.value = selected.value;
  }
}
const channelDocument = new FakeDocument();
const channelForm = channelDocument.getElementById("channel-form");
channelForm.elements = Object.fromEntries(["id", "name", "base_url", "api_keys_raw", "models_raw", "headers_raw", "priority", "weight", "enabled", "fetch_models"].map((name) => [name, new FakeElement("input")]));
channelForm.elements.type = new ChannelSelect();
channelForm.elements.profile_id = new ChannelSelect();
const dashboardHTML = fs.readFileSync(path.join(here, "dashboard.html"), "utf8");
const channelRetentionMarkup = dashboardHTML.match(/<select\b[^>]*name="media_retention"[^>]*>([\s\S]*?)<\/select>/);
assert.ok(channelRetentionMarkup, "the channel form must submit a named media retention field");
assert.equal(dashboardHTML.match(/name="media_retention"/g)?.length, 1, "media saving has one channel setting for images and videos");
channelForm.elements.media_retention = new ChannelSelect();
for (const [, value, attributes, label] of channelRetentionMarkup[1].matchAll(/<option value="([^"]+)"([^>]*)>([^<]+)<\/option>/g)) {
  const option = new FakeElement("option");
  option.value = value;
  option.textContent = label;
  option.selected = /\bselected\b/.test(attributes);
  channelForm.elements.media_retention.append(option);
}
assert.deepEqual(channelForm.elements.media_retention.options.map((option) => [option.value, option.textContent]), [["disabled", "不保存"], ["best_effort", "尽力保存"], ["required", "必须保存"]]);
assert.equal(channelForm.elements.media_retention.value, "disabled", "the native channel form defaults to no automatic saving");
for (const type of ["openai", "anthropic", "newapi", "sub2api"]) {
  const option = new FakeElement("option"); option.value = type;
  channelForm.elements.type.append(option);
}
const noProfile = new FakeElement("option"); noProfile.value = "";
const customProfile = new FakeElement("option"); customProfile.value = "custom-profile";
channelForm.elements.profile_id.append(noProfile, customProfile);
channelForm.reset = () => { channelForm.elements.type.value = "openai"; channelForm.elements.profile_id.value = ""; channelForm.elements.media_retention.value = "disabled"; };
channelForm.reset();
const channelPicker = new ChannelSelect();
channelDocument.elements.set("channel-protocol-picker", channelPicker);
const channelDialog = channelDocument.getElementById("channel-dialog");
channelDialog.showModal = () => { channelDialog.open = true; };
channelDialog.close = () => { channelDialog.open = false; channelDialog.dispatchEvent({ type: "close" }); };
channelDocument.querySelector = (selector) => selector.includes("profile_id") ? channelForm.elements.profile_id : null;
let channelBindings = [];
let channelFetchImpl = async () => response(200, { data: channelBindings });
const channelContext = vm.createContext({
  console, document: channelDocument,
  Event: class { constructor(type) { this.type = type; } },
  fetch: async (url, options) => channelFetchImpl(url, options),
  FormData: class {
    constructor(form) { this.form = form; }
    entries() { return Object.entries(this.form.elements).map(([name, input]) => [name, input.value || ""]); }
  },
  setTimeout: () => 1,
  clearTimeout() {},
});
vm.runInContext(source, channelContext, { filename: "app.js" });
vm.runInContext(`
  adapterTypes = ["openai", "anthropic", "newapi", "sub2api"].map(type => ({type, name:type, default_url:"https://api.example/v1"}));
  channelProfiles = [{id:"custom-profile",name:"Custom",revision:2}];
  renderUnifiedProtocolPicker();
`, channelContext);
for (const type of ["newapi", "sub2api"]) {
  assert.equal(channelPicker.options.some(option => option.value === `builtin:${type}`), false);
  vm.runInContext(`openChannelDialog({id:"legacy",type:${JSON.stringify(type)},enabled:true,base_url:"https://legacy.example/v1"})`, channelContext);
  assert.equal(channelPicker.value, `builtin:${type}`, "editing must select the original legacy protocol");
  assert.equal(channelForm.elements.type.value, type);
  await new Promise(resolve => setImmediate(resolve));
  vm.runInContext(`openChannelDialog()`, channelContext);
  assert.equal(channelPicker.value, "builtin:openai", "new channels must return to the default protocol");
  assert.equal(channelPicker.options.some(option => option.value === `builtin:${type}`), false);
}
channelBindings = [{model_pattern:"*",precedence:0,profile_id:"custom-profile",profile_revision:2}];
vm.runInContext(`openChannelDialog({id:"custom",type:"newapi",enabled:true,media_retention:"required"})`, channelContext);
await new Promise(resolve => setImmediate(resolve));
assert.equal(channelPicker.value, "profile:custom-profile", "async binding restoration must still select the custom profile");
assert.equal(channelForm.elements.profile_id.value, "custom-profile");
assert.equal(channelForm.elements.type.value, "newapi", "restoring a profile must retain the credential adapter");
assert.equal(channelForm.elements.media_retention.value, "required", "binding restoration must retain the channel's media setting");

// Discovery remains complete while availability and user selection stay separate.
const selectedChannel = {
  models_synced_raw: JSON.stringify(["provider-a", "provider-b", "gpt-*", "model-[ab]", "Provider-A"]),
  models_raw: "manual-model\n*\ngpt-?",
  model_map_raw: JSON.stringify({ friendly: "provider-a", hidden: "provider-b" }),
  selected_models: ["provider-a"],
};
const runChannel = (code) => vm.runInContext(code, channelContext);
const selectedModels = () => JSON.parse(runChannel("JSON.stringify([...channelSelectedModels])"));
const modelChoices = () => channelDocument.getElementById("model-catalog").querySelectorAll("label");
assert.deepEqual(JSON.parse(runChannel(`JSON.stringify(channelModelCandidates(${JSON.stringify(selectedChannel)}))`)), ["provider-a", "provider-b", "manual-model"]);
assert.deepEqual(JSON.parse(runChannel(`JSON.stringify(channelModels(${JSON.stringify(selectedChannel)}))`)), ["provider-a", "friendly"]);
assert.deepEqual(JSON.parse(runChannel(`JSON.stringify(channelModels(${JSON.stringify({ ...selectedChannel, selected_models: [] })}))`)), []);
assert.deepEqual(JSON.parse(runChannel(`JSON.stringify(channelModels({selected_models:['a'],model_map_raw:'{"a":"b"}'}))`)), [], "selected incoming names cannot bypass an unselected mapping target");
assert.deepEqual(JSON.parse(runChannel(`JSON.stringify(channelModels({selected_models:['b'],model_map_raw:'{"a":"b"}'}))`)), ["b", "a"], "aliases are available when their mapped target is selected");
assert.deepEqual(JSON.parse(runChannel(`JSON.stringify(channelModels({selected_models:['constructor'],model_map_raw:'{}'}))`)), ["constructor"], "object prototype properties are not model mappings");
assert.deepEqual(JSON.parse(runChannel(`JSON.stringify(channelModels(${JSON.stringify({ ...selectedChannel, selected_models: ["PROVIDER-A"] })}))`)), ["PROVIDER-A", "friendly"], "alias target matching agrees with case-insensitive backend model matching");

runChannel("bindChannelForm()");
runChannel("openChannelDialog()");
assert.equal(channelForm.elements.media_retention.value, "disabled", "a new channel must not inherit the previous channel's saving setting");
for (const retention of ["disabled", "best_effort", "required"]) {
  runChannel(`openChannelDialog({id:'retention-edit',type:'openai',media_retention:${JSON.stringify(retention)}})`);
  assert.equal(channelForm.elements.media_retention.value, retention, "editing restores the stored retention value");
}
runChannel("openChannelDialog({id:'pre-migration',type:'openai'})");
assert.equal(channelForm.elements.media_retention.value, "disabled", "old channel responses without retention default to disabled");
runChannel("openChannelDialog()");
const fetchButton = channelDocument.getElementById("fetch-channel-models");
channelFetchImpl = async () => response(200, { status: "ok", models: ["provider-a", "provider-b"], count: 2 });
await runChannel("fetchChannelModels(byId('fetch-channel-models'))");
assert.deepEqual(selectedModels(), ["provider-a", "provider-b"], "the first discovery for a new empty channel selects all concrete models");
assert.equal(modelChoices().length, 2);
assert.equal(channelDocument.getElementById("channel-model-count").textContent, "已选 2 / 2 个");
const firstChoice = modelChoices()[0].querySelector("input");
firstChoice.checked = false;
firstChoice.dispatchEvent({ type: "change" });
assert.deepEqual(selectedModels(), ["provider-b"], "checkbox changes update selection");
assert.equal(modelChoices()[0].querySelector("input"), firstChoice, "toggling selection keeps the focused checkbox in the document for keyboard navigation");
channelFetchImpl = async () => response(200, { status: "ok", models: ["provider-a", "provider-b", "provider-c"], count: 3 });
await runChannel("fetchChannelModels(byId('fetch-channel-models'))");
assert.deepEqual(selectedModels(), ["provider-b"], "refresh preserves exclusions and does not select newly discovered models");
const search = channelDocument.getElementById("channel-model-search");
search.value = "PROVIDER-C";
search.dispatchEvent({ type: "input" });
assert.equal(modelChoices().length, 1, "search only changes visible candidates");
channelDocument.getElementById("channel-model-select-all").dispatchEvent({ type: "click" });
assert.deepEqual(selectedModels(), ["provider-a", "provider-b", "provider-c"], "select all includes models hidden by search");
channelDocument.getElementById("channel-model-clear-all").dispatchEvent({ type: "click" });
assert.deepEqual(selectedModels(), [], "clear all includes models hidden by search");
await runChannel("fetchChannelModels(byId('fetch-channel-models'))");
assert.deepEqual(selectedModels(), [], "refresh never reselects after clear all");

runChannel(`openChannelDialog(${JSON.stringify({ id: "restricted", type: "openai", enabled: true, base_url: "https://api.example/v1", ...selectedChannel })})`);
assert.deepEqual(selectedModels(), ["provider-a"]);
assert.equal(search.value, "", "switching channels resets search");
assert.equal(modelChoices().length, 3, "an edited channel offers all discovered/manual/mapped targets, not just selected models");
channelForm.elements.models_raw.value = "manual-added\n*\nmodel-?\nmodel-[ab]";
channelForm.elements.models_raw.dispatchEvent({ type: "input" });
const manualChoice = modelChoices().find(label => label.querySelector("span").textContent === "manual-added");
assert.ok(manualChoice, "manual concrete models enter the selectable catalog");
assert.equal(manualChoice.querySelector("input").checked, false);
manualChoice.querySelector("input").checked = true;
manualChoice.querySelector("input").dispatchEvent({ type: "change" });
assert.deepEqual(selectedModels(), ["provider-a", "manual-added"]);
assert.ok(modelChoices().every(label => !/[\*?\[]/.test(label.querySelector("span").textContent)), "wildcards never enter the catalog");
runChannel("addModelMappingRow('new-alias', 'new-target')");
const targetInput = channelDocument.getElementById("mapping-list").querySelectorAll(".mapping-target").at(-1);
targetInput.dispatchEvent({ type: "input" });
assert.ok(modelChoices().some(label => label.querySelector("span").textContent === "new-target"));
assert.ok(!selectedModels().includes("new-target"), "mapping targets require an explicit selection");

runChannel(`openChannelDialog(${JSON.stringify({ id: "empty", type: "openai", base_url: "https://api.example/v1", selected_models: [] })})`);
await runChannel("fetchChannelModels(byId('fetch-channel-models'))");
assert.deepEqual(selectedModels(), [], "saved empty selections remain empty even when they had no candidates");
runChannel(`openChannelDialog(${JSON.stringify({ id: "legacy", type: "openai", ...selectedChannel, selected_models: undefined })})`);
assert.deepEqual(selectedModels(), ["provider-a", "provider-b", "manual-model"], "old unrestricted channels preselect their existing concrete candidates");
runChannel("openChannelDialog({id:'legacy-empty',type:'openai',base_url:'https://api.example/v1'})");
await runChannel("fetchChannelModels(byId('fetch-channel-models'))");
assert.deepEqual(selectedModels(), ["provider-a", "provider-b", "provider-c"], "unrestricted legacy channels without cached candidates select their first successful discovery");
runChannel("openChannelDialog()");
runChannel("selectAllChannelModels(false)");
await runChannel("fetchChannelModels(byId('fetch-channel-models'))");
assert.deepEqual(selectedModels(), [], "clear all before the first discovery explicitly prevents default selection");

// Pending fetches and binding lookups must not mutate a later dialog session.
runChannel("openChannelDialog()");
let finishOldFetch;
channelFetchImpl = async (url) => url.includes("probe-models")
  ? new Promise(resolve => { finishOldFetch = resolve; })
  : response(200, { data: [] });
const oldFetch = runChannel("fetchChannelModels(byId('fetch-channel-models'))");
channelDialog.close();
runChannel("openChannelDialog()");
finishOldFetch(response(200, { status: "ok", models: ["stale-model"] }));
await oldFetch;
assert.deepEqual(selectedModels(), []);
assert.equal(modelChoices().length, 0, "a fetch from a closed dialog cannot populate a reopened dialog");
assert.equal(fetchButton.disabled, false);

runChannel("openChannelDialog()");
let finishConfiguredFetch;
channelFetchImpl = async () => new Promise(resolve => { finishConfiguredFetch = resolve; });
const configuredFetch = runChannel("fetchChannelModels(byId('fetch-channel-models'))");
channelForm.elements.base_url.value = "https://different.example/v1";
finishConfiguredFetch(response(200, { status: "ok", models: ["wrong-provider"] }));
await configuredFetch;
assert.equal(modelChoices().length, 0, "changing provider inputs invalidates the pending discovery");
assert.match(channelDocument.getElementById("model-fetch-status").textContent, /配置已更改/);

let finishBindings;
channelFetchImpl = async () => new Promise(resolve => { finishBindings = resolve; });
runChannel("openChannelDialog({id:'old-profile',type:'openai'})");
const resolveOldBindings = finishBindings;
runChannel("openChannelDialog()");
resolveOldBindings(response(200, { data: [{model_pattern:'*',precedence:0,profile_id:'custom-profile',profile_revision:2}] }));
await new Promise(resolve => setImmediate(resolve));
assert.equal(channelPicker.value, "builtin:openai", "stale binding restoration cannot change a new dialog");

// Save includes explicit empty arrays, and reopening is sourced from saved state.
const submittedChannels = [];
channelFetchImpl = async (url, options) => {
  if (url === "/api/channels" && options?.method === "POST") {
    const payload = JSON.parse(options.body);
    submittedChannels.push(payload);
    return response(200, { channel: {} });
  }
  return response(200, { channels: [], data: [] });
};
const submitButton = new FakeElement("button");
channelForm.querySelector = () => submitButton;
// These dashboard functions are outside this form harness; saving still uses real request/payload logic.
runChannel("loadChannels = async () => {}");
runChannel(`openChannelDialog(${JSON.stringify({ id: "saved", type: "openai", base_url: "https://api.example/v1", ...selectedChannel })})`);
runChannel("selectAllChannelModels(false)");
for (const handler of channelForm.listeners.get("submit") || []) await handler({ preventDefault() {}, currentTarget: channelForm });
assert.deepEqual(submittedChannels.at(-1).selected_models, [], "save must distinguish explicit empty selection from an omitted field");
runChannel(`openChannelDialog(${JSON.stringify({ id: "saved", type: "openai", base_url: "https://api.example/v1", ...selectedChannel })})`);
for (const handler of channelForm.listeners.get("submit") || []) await handler({ preventDefault() {}, currentTarget: channelForm });
assert.deepEqual(submittedChannels.at(-1).selected_models, ["provider-a"], "saving persists precisely the selected upstream names");
assert.equal(submittedChannels.at(-1).media_retention, "disabled", "saving an unchanged old channel submits the disabled default as a string");

const submittedBindings = [];
channelFetchImpl = async (url, options) => {
  if (url === "/api/channels" && options?.method === "POST") {
    submittedChannels.push(JSON.parse(options.body));
    return response(200, { channel: { id: "retention-saved" } });
  }
  if (url.endsWith("/bind-profile")) {
    submittedBindings.push(JSON.parse(options.body));
    return response(200, { bindings: [] });
  }
  return response(200, { channels: [], data: [] });
};
for (const retention of ["disabled", "best_effort", "required"]) {
  runChannel("openChannelDialog()");
  channelForm.elements.media_retention.value = retention;
  channelForm.elements.profile_id.value = "custom-profile";
  channelForm.elements.fetch_models.checked = false;
  for (const handler of channelForm.listeners.get("submit") || []) await handler({ preventDefault() {}, currentTarget: channelForm });
  const payload = submittedChannels.at(-1);
  assert.equal(payload.media_retention, retention, "FormData must preserve the selected channel retention string");
  assert.equal(Object.hasOwn(payload, "profile_id"), false, "channel retention is separate from the Profile binding payload");
  assert.deepEqual(submittedBindings.at(-1), { profile_id: "custom-profile", profile_revision: 2 }, "retention must not become a Profile binding field");
  runChannel(`openChannelDialog(${JSON.stringify({ ...payload, id: "retention-saved" })})`);
  assert.equal(channelForm.elements.media_retention.value, retention, "reopening the saved channel retains the selected value");
}

async function setupHarness(status, statusFailure = false) {
  const setupDocument = new FakeDocument();
  const form = setupDocument.getElementById("setup-form");
  const button = new FakeElement("button");
  form.querySelector = () => button;
  const field = setupDocument.getElementById("setup-secret-field");
  field.classList.add("hidden");
  const secret = setupDocument.getElementById("setup-secret");
  secret.value = "deployment-secret";
  secret.disabled = true;
  const calls = [];
  const redirects = [];
  const setupContext = vm.createContext({
    document: setupDocument,
    location: { assign: (target) => redirects.push(target) },
    FormData: class {
      entries() { return Object.entries({ username: "admin", password: "long-password", confirm_password: "long-password" }); }
    },
    fetch: async (url, options) => {
      calls.push({ url, options });
      if (url === "/api/auth/status") return response(statusFailure ? 503 : 200, statusFailure ? { error: "service unavailable" } : status);
      return response(200, { csrf_token: "csrf", gateway_api_key: "gateway-key" });
    },
  });
  vm.runInContext(source, setupContext, { filename: "app.js" });
  await vm.runInContext("initSetup()", setupContext);
  return { setupDocument, form, button, field, secret, calls, redirects,
    submit: async () => {
      for (const handler of form.listeners.get("submit") || []) await handler({ preventDefault() {} });
    },
  };
}

for (const required of [false, true]) {
  const setup = await setupHarness({ initialized: false, setup_secret_required: required });
  assert.equal(setup.button.disabled, false);
  assert.equal(setup.field.classList.contains("hidden"), !required);
  assert.equal(setup.secret.required, required);
  assert.equal(setup.secret.disabled, !required);
  if (required) {
    setup.secret.value = "";
    await setup.submit();
    assert.equal(setup.calls.length, 1, "missing required secret must not submit");
    assert.match(setup.setupDocument.getElementById("auth-error").textContent, /初始化口令/);
    setup.secret.value = "deployment-secret";
  }
  await setup.submit();
  const post = setup.calls.find(call => call.url === "/api/auth/setup");
  assert.ok(post, "setup should submit after status loads");
  assert.equal(post.options.headers["X-Relay-Setup-Secret"], required ? "deployment-secret" : undefined);
  assert.deepEqual(JSON.parse(post.options.body), { username: "admin", password: "long-password" });
  assert.equal(JSON.stringify(post).includes("deployment-secret"), required);
  assert.equal(setup.secret.value, "", "successful setup clears secret input");
  assert.equal(setup.setupDocument.getElementById("setup-token").textContent, "gateway-key");
}
const failedSetup = await setupHarness({}, true);
assert.equal(failedSetup.button.disabled, true);
assert.match(failedSetup.setupDocument.getElementById("auth-error").textContent, /无法读取初始化状态.*service unavailable.*刷新/);
await failedSetup.submit();
assert.equal(failedSetup.calls.length, 1, "unknown initialization status must not submit");
const initializedSetup = await setupHarness({ initialized: true, setup_secret_required: false });
assert.deepEqual(initializedSetup.redirects, ["/login"]);
assert.equal(initializedSetup.button.disabled, true);

console.log("app runtime behavior tests passed");
