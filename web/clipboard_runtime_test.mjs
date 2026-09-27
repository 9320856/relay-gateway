import assert from "node:assert/strict";
import test from "node:test";
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

// Model focus, selection and actual DOM removal: these are the browser effects
// the fallback must undo, rather than merely checking that execCommand ran.
FakeElement.prototype.append = function (...children) {
  for (const child of children) {
    child.parentElement = this;
    child.ownerDocument = this.ownerDocument;
    this.children.push(child);
  }
};
FakeElement.prototype.remove = function () {
  if (this.parentElement) {
    this.parentElement.children = this.parentElement.children.filter((child) => child !== this);
    this.parentElement = null;
  }
};
FakeElement.prototype.focus = function () { this.ownerDocument.activeElement = this; };
FakeElement.prototype.select = function () { this.setSelectionRange(0, this.value.length); };
FakeElement.prototype.setSelectionRange = function (start, end, direction = "none") {
  this.selectionStart = start;
  this.selectionEnd = end;
  this.selectionDirection = direction;
};
FakeElement.prototype.closest = function (selector) {
  for (let node = this; node; node = node.parentElement) {
    if (selector === "dialog[open]" && node.tagName === "DIALOG" && node.open) return node;
  }
  return null;
};
FakeElement.prototype.showModal = function () { this.open = true; };
FakeElement.prototype.scrollIntoView = function () {};

const here = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(here, "assets", "app.js"), "utf8");
const relativeURL = "/v1/media/image-42/capability?token=abc%2Bdef";

function harness({ clipboard, legacy = true, origin = "http://192.168.1.10:8000" } = {}) {
  const document = new FakeDocument();
  document.body.ownerDocument = document;
  const baseCreate = document.createElement.bind(document);
  document.createElement = (tag) => {
    const element = baseCreate(tag);
    element.ownerDocument = document;
    return element;
  };
  const baseById = document.getElementById.bind(document);
  document.getElementById = (id) => {
    const element = baseById(id);
    element.ownerDocument = document;
    return element;
  };
  const range = { cloneRange() { return this; } };
  const selection = {
    ranges: [range],
    get rangeCount() { return this.ranges.length; },
    getRangeAt(index) { return this.ranges[index]; },
    removeAllRanges() { this.ranges = []; },
    addRange(value) { this.ranges.push(value); },
  };
  const copies = [];
  document.execCommand = (command) => {
    assert.equal(command, "copy");
    const textarea = document.activeElement;
    assert.equal(textarea.tagName, "TEXTAREA");
    assert.ok(textarea.readOnly || textarea.hasAttribute("readonly"));
    assert.equal(textarea.selectionStart, 0);
    assert.equal(textarea.selectionEnd, textarea.value.length);
    copies.push({ value: textarea.value, parent: textarea.parentElement });
    selection.removeAllRanges();
    if (legacy instanceof Error) throw legacy;
    return legacy;
  };
  document.getSelection = () => selection;
  document.activeElement = document.body;
  const location = { origin, href: `${origin}/console`, assign() {} };
  const window = { location, getSelection: () => selection, addEventListener() {}, open: () => null };
  const context = vm.createContext({
    console, document, window, location, navigator: { clipboard }, URL, URLSearchParams,
    FormData: class { entries() { return []; } }, confirm: () => true,
    setTimeout: () => 1, clearTimeout() {},
    fetch: async () => ({
      status: 200, ok: true, headers: { get: () => "application/json" },
      json: async () => ({
        log: { id: "log-42", method: "POST", path: "/v1/images", outcome: "success", status_code: 200,
          response_body: JSON.stringify({ data: [{ url: relativeURL }] }) }, events: [],
      }),
    }),
  });
  vm.runInContext(source, context, { filename: "app.js" });
  return { document, selection, range, copies, context, absoluteURL: origin + relativeURL,
    run: (code) => vm.runInContext(code, context),
    toasts: () => document.getElementById("toast-region").querySelectorAll(".toast")
      .map((item) => item.children[1].textContent),
  };
}

function copyButton(element) {
  const button = element.querySelectorAll("button").find((item) => item.textContent === "复制链接");
  assert.ok(button, "rendered media must expose a copy link button");
  return button;
}

async function click(button) {
  for (const listener of button.listeners.get("click") || []) {
    await listener({ type: "click", target: button, currentTarget: button });
  }
  // Flush promises even if the event handler uses then() without returning it.
  await new Promise((resolve) => setImmediate(resolve));
}

test("LAN HTTP playground copies the full public URL and restores focus and selections", async () => {
  const h = harness();
  const input = h.document.createElement("input");
  input.value = "original prompt";
  input.setSelectionRange(2, 7, "backward");
  h.document.body.append(input);
  input.focus();
  h.run(`renderPlaygroundImages([${JSON.stringify(relativeURL)}])`);
  const button = copyButton(h.document.getElementById("playground-media"));
  await click(button);
  assert.deepEqual(h.copies.map((copy) => copy.value), [h.absoluteURL]);
  assert.equal(button.textContent, "已复制 ✓");
  assert.equal(h.document.activeElement, input);
  assert.equal(input.selectionStart, 2);
  assert.equal(input.selectionEnd, 7);
  assert.equal(input.selectionDirection, "backward");
  assert.deepEqual(h.selection.ranges, [h.range]);
  assert.equal(h.document.body.querySelectorAll("textarea").length, 0);
});

test("missing clipboard invokes the fallback synchronously and rejects empty content", async () => {
  const h = harness();
  const pending = h.run('copyText("gesture-bound text")');
  assert.deepEqual(h.copies.map((copy) => copy.value), ["gesture-bound text"],
    "legacy copy must occur within the original user gesture");
  await pending;
  const copyCount = h.copies.length;
  await assert.rejects(h.run('copyText("")'), /没有可复制/);
  assert.equal(h.copies.length, copyCount);
});

test("HTTPS modern clipboard success waits for completion and skips legacy", async () => {
  let finish;
  const writes = [];
  const h = harness({ origin: "https://gateway.test", clipboard: { writeText(value) {
    writes.push(value);
    return new Promise((resolve) => { finish = resolve; });
  } } });
  const figure = h.run(`createMediaFigure(${JSON.stringify(relativeURL)}, "image")`);
  const button = copyButton(figure);
  const pending = click(button);
  assert.equal(button.textContent, "复制链接", "success must wait for the actual write");
  finish();
  await pending;
  assert.deepEqual(writes, [h.absoluteURL]);
  assert.equal(h.copies.length, 0);
  assert.equal(button.textContent, "已复制 ✓");
});

test("modern rejection falls back inside an open dialog for a log preview", async () => {
  const h = harness({ clipboard: { writeText: async () => { throw new Error("denied"); } } });
  const dialog = h.document.createElement("dialog");
  dialog.open = true;
  const input = h.document.createElement("input");
  input.value = "filter";
  dialog.append(input);
  h.document.body.append(dialog);
  input.focus();
  await h.run('openLog("log-42")');
  const button = copyButton(h.document.getElementById("visual-media-view"));
  await click(button);
  assert.equal(button.textContent, "已复制 ✓");
  assert.equal(h.copies[0].value, h.absoluteURL);
  assert.equal(h.copies[0].parent, dialog);
  assert.equal(dialog.querySelectorAll("textarea").length, 0);
  assert.equal(h.document.activeElement, input);
});

for (const legacy of [false, new Error("legacy clipboard unavailable")]) {
  test(`legacy ${legacy === false ? "false" : "exception"} reports failure and cleans up`, async () => {
    const h = harness({ legacy });
    const originalFocus = h.document.activeElement;
    const figure = h.run(`createMediaFigure(${JSON.stringify(relativeURL)}, "image")`);
    const button = copyButton(figure);
    await click(button);
    assert.equal(button.textContent, "复制失败");
    assert.ok(h.toasts().some((message) => /复制失败/.test(message)));
    assert.ok(h.toasts().every((message) => !/已复制/.test(message)));
    assert.equal(h.document.body.querySelectorAll("textarea").length, 0);
    assert.equal(h.document.activeElement, originalFocus);
    assert.deepEqual(h.selection.ranges, [h.range]);
    await assert.rejects(h.run('copyText("content")'), /复制|剪贴板/);
  });
}
