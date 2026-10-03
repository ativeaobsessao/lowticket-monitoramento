import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

test("dashboard inline script remains valid JavaScript", async () => {
  const source = await readFile(new URL("../index.js", import.meta.url), "utf8");
  const renderMarker = source.indexOf("function render(D,HD,P){");
  const scriptStart = source.lastIndexOf("<script>", renderMarker);
  const scriptEnd = source.indexOf("<\\/script>", renderMarker);

  assert.notEqual(renderMarker, -1);
  assert.notEqual(scriptStart, -1);
  assert.notEqual(scriptEnd, -1);
  const script = source
    .slice(scriptStart + 8, scriptEnd)
    .replace("const IG_SVG=\\`${IG_SVG_ESC}\\`;", 'const IG_SVG="";');
  assert.doesNotThrow(() => new Function(script));
});
