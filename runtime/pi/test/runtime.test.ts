import assert from "node:assert/strict";
import { test } from "node:test";
import { type AssistantMessage, type Model } from "@earendil-works/pi-ai";
import type { AgentContext } from "@earendil-works/pi-agent-core";
import { run, type Callback, type RunInput } from "../src/runtime.ts";

const model: Model<"openai-responses"> = {
  id: "gpt-5.6-luna", name: "Luna", provider: "fixture", api: "openai-responses",
  baseUrl: "http://127.0.0.1:8317/v1", reasoning: true, thinkingLevelMap: { max: "max" },
  input: ["text"], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 32000, maxTokens: 100,
};
const message: AssistantMessage = {
  role: "assistant", content: [{ type: "text", text: "done" }], model: model.id,
  api: model.api, provider: model.provider, stopReason: "stop", timestamp: 0,
  usage: { input: 10, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 11,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
};

const input: RunInput = {
  runId: "offline", profile: {
    provider_id: "fixture", model: model.id, origin: "http://127.0.0.1:8317", api_path: "/v1/responses",
    credential_env: "PI_OFFLINE_KEY", reasoning_effort: "max", context_window_tokens: 32000,
    reserved_output_tokens: 100, temperature: 1, top_p: 0.95,
    runtime: { api: "openai-responses", supported_efforts: ["max"], request_options: {} },
  }, messages: [{ role: "system", content: "Frozen policy" }, { role: "user", content: "Read selected evidence." }],
  tools: [{ function: { name: "read_selected", description: "Read selected evidence", parameters: { type: "object", properties: {}, additionalProperties: false } } }],
};

test("public loop replays opaque native state and waits for tool result before next admission", async () => {
  const order: string[] = [];
  const native = { ...message, content: [
    { type: "thinking", thinking: "", thinkingSignature: "opaque-reasoning-state" },
    { type: "toolCall", id: "call|opaque", name: "read_selected", arguments: {} },
  ], stopReason: "toolUse" };
  const callback: Callback = async (method, payload) => {
    order.push(method);
    if (method === "model_admit") {
      if (payload.number === 2) {
        assert.match(JSON.stringify(payload.context), /frozen-fact-42/);
        assert.match(JSON.stringify(payload.context), /opaque-reasoning-state/);
      }
      return { replay: payload.number === 1 ? native : message };
    }
    if (method === "tool") return { content: "frozen-fact-42" };
    if (method === "turn_end") return { stop: (payload.message as AssistantMessage).stopReason === "stop" };
    return {};
  };
  await run(input, callback, new AbortController().signal);
  assert.deepEqual(order, ["model_admit", "tool", "tool_message", "turn_end", "context_check", "model_admit", "turn_end", "agent_end"]);
});

test("rejected persistence callback stops before tools and further model requests", async () => {
  let calls = 0;
  await assert.rejects(run(input, async () => { calls++; throw new Error("disk write failed"); }, new AbortController().signal));
  assert.equal(calls, 1);
});

test("imported native history precedes the new task without losing opaque state", async () => {
  const old = { ...message, content: [
    { type: "thinking" as const, thinking: "", thinkingSignature: "previous-opaque-state" },
    { type: "text" as const, text: "Previous final answer" },
  ] };
  await run({ ...input, nativeMessages: [old], tools: [] }, async (method, payload) => {
    if (method === "model_admit") {
      const messages = (payload.context as AgentContext).messages;
      assert.deepEqual(messages.map(m => m.role), ["assistant", "user"]);
      assert.match(JSON.stringify(messages[0]), /previous-opaque-state/);
      const latest = messages.at(-1);
      assert.ok(latest?.role === "user");
      assert.equal(latest.content, "Read selected evidence.");
      return { replay: message };
    }
    if (method === "turn_end") return { stop: true };
    return {};
  }, new AbortController().signal);
});

test("two upstream compactions retain fixed policy and use incremental summaries", async () => {
  let decisions = 0, summaries = 0, checkpoints = 0;
  const artifacts: unknown[] = [];
  await run({ ...input, compaction: { reserveTokens: 1024, keepRecentTokens: 0 } }, async (method, payload) => {
    if (method === "model_admit") {
      if (payload.purpose === "compaction") {
        summaries++;
        if (summaries > 1) assert.match(JSON.stringify(payload.context), /previous-summary-1/);
        return { replay: { ...message, content: [{ type: "text", text: `previous-summary-${summaries}` }] } };
      }
      decisions++;
      assert.match(JSON.stringify(payload.context), /Frozen policy/);
      if (decisions > 1) assert.match(JSON.stringify(payload.context), /previous-summary/);
      return { replay: decisions <= 2 ? { ...message, stopReason: "toolUse", content: [
        { type: "toolCall", id: `call-${decisions}`, name: "read_selected", arguments: {} },
      ] } : message };
    }
    if (method === "tool") return { content: "Frozen fact, reference e1. ".repeat(100) };
    if (method === "turn_end") return { stop: decisions > 2 };
    if (method === "context_check") return { compact: checkpoints < 2 };
    if (method === "compaction_commit") { checkpoints++; artifacts.push(payload.entry); }
    return {};
  }, new AbortController().signal);
  assert.equal(checkpoints, 2); assert.equal(summaries, 2); assert.equal(decisions, 3);
  assert.equal(artifacts.length, 2);
});
