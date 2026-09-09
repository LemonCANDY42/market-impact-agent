import assert from "node:assert/strict";
import { test } from "node:test";
import { type AssistantMessage, type Model } from "@earendil-works/pi-ai";
import type { AgentContext } from "@earendil-works/pi-agent-core";
import { run, estimatePiContext, type Callback, type RunInput } from "../src/runtime.ts";

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
  await run({ ...input, profile: { ...input.profile, runtime: { ...input.profile.runtime, context_estimator: "pi-usage-v1" } },
    skills: [{ name: "optional-risk", description: "Review concentration", filePath: "/frozen/optional-risk/SKILL.md" }],
    compaction: { reserveTokens: 1024, keepRecentTokens: 0 } }, async (method, payload) => {
    if (method === "model_admit") {
      if (payload.purpose === "compaction") {
        summaries++;
        if (summaries > 1) assert.match(JSON.stringify(payload.context), /previous-summary-1/);
        return { replay: { ...message, content: [{ type: "text", text: `previous-summary-${summaries}` }] } };
      }
      decisions++;
      assert.match(JSON.stringify(payload.context), /Frozen policy/);
      assert.match(JSON.stringify(payload.context), /optional-risk/);
      if (decisions > 1) {
        assert.match(JSON.stringify(payload.context), /previous-summary/);
        assert.equal((payload.context_estimate as { usageTokens: number }).usageTokens, 0);
      }
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

test("UTF-8 tool history compacts below admission while preserving the pinned task", async () => {
  let decisions = 0, checkpoints = 0;
  const limit = 65536 - 8192;
  await run({ ...input, profile: { ...input.profile, context_window_tokens: 65536, reserved_output_tokens: 8192 } }, async (method, payload) => {
    if (method === "context_check") return { compact: Buffer.byteLength(JSON.stringify(payload.context)) >= limit };
    if (method === "model_admit") {
      if (payload.purpose === "compaction") {
        assert.match(JSON.stringify(payload.context), /Read selected evidence/);
        assert.match(JSON.stringify(payload.context), /Frozen policy/);
        return { replay: { ...message, content: [{ type: "text", text: "Evidence e1 and e2 read; finish the pinned task." }] } };
      }
      assert.ok(Buffer.byteLength(JSON.stringify(payload.context)) < limit);
      decisions++;
      return { replay: decisions <= 2 ? { ...message, stopReason: "toolUse", content: [
        { type: "thinking", thinking: "Analysis ".repeat(700) },
        { type: "toolCall", id: `utf8-${decisions}`, name: "read_selected", arguments: {} },
      ] } : message };
    }
    if (method === "tool") return { content: "证据".repeat(5000) };
    if (method === "turn_end") return { stop: decisions > 2 };
    if (method === "compaction_commit") checkpoints++;
    return {};
  }, new AbortController().signal);
  assert.equal(checkpoints, 1);
  assert.equal(decisions, 3);
});

test("native pi usage avoids byte-triggered compaction of a large request", async () => {
  let decisions = 0;
  const limit = 98304 - 16384;
  const pinned = "Read selected evidence. " + "x".repeat(42000);
  await run({ ...input, messages: [...input.messages.slice(0, 1), { role: "user", content: pinned }],
    profile: { ...input.profile, context_window_tokens: 98304, reserved_output_tokens: 16384,
      runtime: { ...input.profile.runtime, context_estimator: "pi-usage-v1" } } }, async (method, payload) => {
    if (method === "context_check") {
      return { compact: Number((payload.context_estimate as { tokens: number }).tokens) >= limit };
    }
    if (method === "model_admit") {
      if (payload.purpose === "compaction") {
        assert.fail("Native token usage does not require a summary");
      }
      assert.ok(Number((payload.context_estimate as { tokens: number }).tokens) < limit);
      assert.ok(JSON.stringify(payload.context).includes(pinned));
      decisions++;
      if (decisions === 3) {
        assert.match(JSON.stringify(payload.context), /large-2/);
        assert.match(JSON.stringify(payload.context), /large-1/);
        assert.ok(Buffer.byteLength(JSON.stringify(payload.context)) > limit);
      }
      return { replay: decisions <= 2 ? { ...message, usage: { ...message.usage, input: 35000, output: 5000, totalTokens: 40000 }, stopReason: "toolUse", content: [
        { type: "thinking", thinking: "Analysis ".repeat(2000) },
        { type: "toolCall", id: `large-${decisions}`, name: "read_selected", arguments: {} },
      ] } : message };
    }
    if (method === "tool") return { content: "证据".repeat(500) };
    if (method === "turn_end") return { stop: decisions > 2 };
    return {};
  }, new AbortController().signal);
  assert.equal(decisions, 3);
});

test("rebuilt context ignores stale pre-compaction usage and counts pinned inputs", () => {
  const context = { systemPrompt: "pinned ".repeat(100), tools: [], messages: [
    { ...message, usage: { ...message.usage, input: 90000, output: 5000, totalTokens: 95000 } },
    { role: "user" as const, content: "New task and retained evidence.", timestamp: 0 },
  ] };
  assert.ok(estimatePiContext(context).tokens >= 95000);
  const rebuilt = estimatePiContext(context, false);
  assert.equal(rebuilt.usageTokens, 0);
  assert.ok(rebuilt.fixedTokens > 0);
  assert.ok(rebuilt.tokens < 1000);
  assert.equal((context.messages[0] as AssistantMessage).usage.totalTokens, 95000);
});
