/** Temporary old-version installation; no legacy dependency in the production lock. */
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { runAgentLoop, type AgentLoopConfig } from "@earendil-works/pi-agent-core";
import { createAssistantMessageEventStream, type AssistantMessage, type Model } from "@earendil-works/pi-ai";

const directory = mkdtempSync(join(tmpdir(), "market-impact-pi-upgrade-"));
try {
  execFileSync("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--prefix", directory,
    "@earendil-works/pi-agent-core@0.84.3"], { stdio: "pipe", timeout: 120000 });
  const priorEntry = execFileSync(process.execPath,
    ["--input-type=module", "-e", "console.log(import.meta.resolve('@earendil-works/pi-agent-core'))"],
    { cwd: directory, encoding: "utf8", timeout: 10000 }).trim();
  const prior = await import(priorEntry);
  const model: Model<"openai-responses"> = {
    id: "synthetic", name: "Synthetic", provider: "fixture", api: "openai-responses", reasoning: false,
    baseUrl: "http://127.0.0.1:8317/v1",
    input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 32000, maxTokens: 100,
  };
  const message: AssistantMessage = {
    role: "assistant", content: [{ type: "text", text: "done" }], model: model.id,
    api: model.api, provider: model.provider, stopReason: "stop", timestamp: 0,
    usage: { input: 10, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 11,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
  };
  const preparations: number[] = [];
  for (const loop of [prior.runAgentLoop as typeof runAgentLoop, runAgentLoop]) {
    let prepared = 0, completed = 0, requests = 0;
    const config: AgentLoopConfig = { model, convertToLlm: messages => messages as AssistantMessage[],
      prepareNextTurn: async () => { prepared++; return undefined; } };
    await loop([], { messages: [{ role: "user", content: "finish", timestamp: 0 }], tools: [], systemPrompt: "test" },
      config, event => { if (event.type === "agent_end") completed++; }, undefined, async () => {
        requests++;
        const stream = createAssistantMessageEventStream();
        stream.push({ type: "done", reason: "stop", message }); stream.end(message); return stream;
      });
    assert.equal(requests, 1); assert.equal(completed, 1);
    preparations.push(prepared);
  }
  assert.deepEqual(preparations, [1, 0]);
  console.log(JSON.stringify({ passed: true, versions: ["0.84.3", "0.84.4"], preparations,
    requests_per_version: 1, terminal_events_per_version: 1, real_model_requests: 0,
    prior_lock_hash: createHash("sha256").update(readFileSync(join(directory, "package-lock.json"))).digest("hex") }));
} finally {
  rmSync(directory, { recursive: true });
}
