import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const script = fs.readFileSync(path.join(here, "assets", "app.js"), "utf8");
const page = fs.readFileSync(path.join(here, "media.html"), "utf8");

for (const required of [
  'id="media-preview-dialog"',
  'id="media-preview-content"',
  'id="media-preview-close"',
  'id="mobile-nav-toggle"',
  'id="sidebar-backdrop"',
  'id="app-sidebar"',
]) {
  assert.ok(page.includes(required) || script.includes(required), `media preview/shell is missing ${required}`);
}
for (const required of [
  "media-assets/${encodeURIComponent(asset.id)}/content",
  "video.pause()",
  'video.removeAttribute("src")',
  "`/api/media-assets/${asset.id}`",
  'method: "DELETE"',
  "媒体已彻底删除",
  'media-assets/${encodeURIComponent(asset.id)}/link',
  "正在加载公开链接…",
  "公开链接加载失败，请稍后重试",
  "previewLinkVersion",
  "复制公开链接",
  'thumbWrap.setAttribute("role", "button")',
  'thumbWrap.setAttribute("tabindex", "0")',
  'img.setAttribute("role", "button")',
  'img.setAttribute("tabindex", "0")',
  'e.key === "Enter"',
]) {
  assert.ok(script.includes(required), `media lifecycle UI is missing ${required}`);
}

for (const forbidden of [
  "rotate-link",
  "轮换链接",
  "确定轮换公开链接吗",
]) {
  assert.ok(!script.includes(forbidden), `media UI should not contain rotation code: ${forbidden}`);
}

console.log("media runtime behavior tests passed");
