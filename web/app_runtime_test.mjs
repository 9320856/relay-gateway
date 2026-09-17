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
for (const type of ["openai", "anthropic", "newapi", "sub2api"]) {
  const option = new FakeElement("option"); option.value = type;
  channelForm.elements.type.append(option);
}
const noProfile = new FakeElement("option"); noProfile.value = "";
const customProfile = new FakeElement("option"); customProfile.value = "custom-profile";
channelForm.elements.profile_id.append(noProfile, customProfile);
channelForm.reset = () => { channelForm.elements.type.value = "openai"; channelForm.elements.profile_id.value = ""; };
channelForm.reset();
const channelPicker = new ChannelSelect();
channelDocument.elements.set("channel-protocol-picker", channelPicker);
const channelDialog = channelDocument.getElementById("channel-dialog");
channelDialog.showModal = () => { channelDialog.open = true; };
channelDocument.querySelector = (selector) => selector.includes("profile_id") ? channelForm.elements.profile_id : null;
let channelBindings = [];
const channelContext = vm.createContext({
  console, document: channelDocument,
  Event: class { constructor(type) { this.type = type; } },
  fetch: async () => response(200, { data: channelBindings }),
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
vm.runInContext(`openChannelDialog({id:"custom",type:"newapi",enabled:true})`, channelContext);
await new Promise(resolve => setImmediate(resolve));
assert.equal(channelPicker.value, "profile:custom-profile", "async binding restoration must still select the custom profile");
assert.equal(channelForm.elements.profile_id.value, "custom-profile");
assert.equal(channelForm.elements.type.value, "newapi", "restoring a profile must retain the credential adapter");

console.log("app runtime behavior tests passed");
