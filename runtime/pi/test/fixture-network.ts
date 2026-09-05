/** Test preload only: replace external I/O, never the pi loop or protocol decoder. */
import assert from "node:assert/strict";
let requests = 0;
let physical = 0;
const outcome = {
  event_id: "energy-outage-1", decision: "abstain", summary: "Frozen outage evidence was read.",
  transmission_steps: [], candidates: [], blockers: ["Outage duration is unknown"],
  unresolved_questions: ["outage duration"], stopped_reason: "Evidence read; timing unresolved",
};
export function responses(body: any): Response {
  requests++;
  const isSummary = !body.tools?.length;
  const hasResult = body.input.some((m: any) => m.type === "function_call_output");
  const mode = process.env.PI_FIXTURE_KEY;
  if (mode === "unclassified429") return new Response("unclassified", { status: 429 });
  if (hasResult && mode !== "bad-arguments") assert.match(JSON.stringify(body.input), /official-outage/);
  assert.equal(body.reasoning?.effort, "max");
  const empty = mode === "empty-always" || (mode === "empty-once" && hasResult && requests === 2)
    || (mode === "empty-summary" && isSummary);
  const answer = empty ? "" : mode === "whitespace-summary" && isSummary ? "   " : JSON.stringify(outcome);
  const item = !hasResult && !isSummary && mode !== "empty-always"
    ? { type: "function_call", id: "fc_1", call_id: "call-1", name: "read_evidence", arguments: mode === "bad-arguments" ? '{}' : '{"evidence_id":"official-outage"}' }
    : { type: "message", id: "msg_1", role: "assistant", content: [{ type: "output_text", text: answer, annotations: [] }] };
  const frames = [
    { type: "response.created", response: { id: `response-${requests}` } },
    { type: "response.output_item.added", output_index: 0, item },
    ...(item.type === "message" ? [{ type: "response.output_text.delta", output_index: 0, delta: answer }] : []),
    { type: "response.output_item.done", output_index: 0, item },
    { type: "response.completed", response: { id: `response-${requests}`, model: mode === "model-fallback" ? "unexpected-fallback" : body.model,
      status: "completed", output: [item], usage: { input_tokens: 100, output_tokens: 20,
        input_tokens_details: { cached_tokens: hasResult ? 64 : 0 }, total_tokens: 120 } } },
  ];
  return new Response(frames.map(frame => `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`).join(""),
    { headers: { "Content-Type": "text/event-stream" } });
}
globalThis.fetch = async (input, init) => {
  physical++;
  const request = new Request(input, init);
  if (process.env.PI_FIXTURE_KEY === "hang") {
    await new Promise((_resolve, reject) => {
      request.signal.addEventListener("abort", () => reject(new Error("fixture abort")), { once: true });
    });
  }
  assert.ok(["http://127.0.0.1:8317/v1/responses", "https://api.minimaxi.com/v1/chat/completions"].includes(request.url));
  const mode = process.env.PI_FIXTURE_KEY;
  if ((mode === "received408" && physical === 1) || mode === "repeated408") {
    return new Response(JSON.stringify({ error: { code: "timeout" } }), { status: 408 });
  }
  if (mode === "quota" || (mode === "rate-limit" && physical === 1)) {
    return new Response(JSON.stringify({ error: { code: mode === "quota" ? "insufficient_quota" : "rate_limited" } }), { status: 429 });
  }
  if (mode === "broken-stream") return new Response(new ReadableStream({
    start(controller) { controller.error(new Error("synthetic broken stream")); },
  }), { headers: { "Content-Type": "text/event-stream" } });
  const body = await request.json() as any;
  if (process.env.MARKET_IMPACT_CLIPROXY_API_KEY?.startsWith("synthetic-cpa") || process.env.MINIMAX_API_KEY === "synthetic-minimax") return canaryResponse(body);
  return responses(body);
};

function canaryResponse(body: any): Response {
  const chat = !!body.messages;
  const messages = body.messages ?? body.input;
  const continuation = JSON.stringify(messages).includes("Synthetic concurrency acceptance only.");
  const summary = !body.tools?.length && !continuation;
  if (continuation) {
    assert.ok(!body.tools?.length);
    assert.equal(messages.at(-1).role, "user");
    for (const ref of ["synthetic-event", "synthetic-exposure", "synthetic-market"])
      assert.ok(JSON.stringify(messages).includes(ref));
  }
  const extraSearch = process.env.MARKET_IMPACT_CLIPROXY_API_KEY === "synthetic-cpa-extra-search" && physical === 2;
  const compacted = JSON.stringify(messages).includes("Qualification summary");
  const final = !extraSearch && (summary || compacted || messages.some((m: any) => m.role === "tool" || m.type === "function_call_output"));
  if (final && !summary && !compacted) assert.match(JSON.stringify(messages), /outage lasts 18 hours/);
  const text = summary ? "Qualification summary: observed outage lasts 18 hours; net exposure unknown. Sources synthetic-event, synthetic-exposure, synthetic-market. No executable quote or trading permission."
    : JSON.stringify({ ...outcome, event_id: "pi-synthetic-outage", summary: "Synthetic outage duration is 18 hours; net exposure remains unknown." });
  const calls = ["event_revelation", "exposure_candidates", "market_context"].map((kind, index) => ({
    index, id: `canary-${extraSearch ? "search-" : ""}${index}`, type: "function", function: { name: `${extraSearch ? "search" : "read_selected"}_${kind}`, arguments: "{}" },
  }));
  if (chat) {
    assert.equal(body.reasoning_split, true);
    const details = [{ type: "reasoning.text", id: "reasoning-text-1", format: "MiniMax-response-v1", index: 0, text: "Synthetic private reasoning marker." }];
    if (final && !summary && !compacted) {
      const prior = messages.find((m: any) => m.role === "assistant" && m.tool_calls);
      assert.deepEqual(prior.reasoning_details, details);
      assert.ok(!prior.content?.includes("Synthetic private reasoning marker."));
    }
    const delta = { ...(final ? { content: text } : { tool_calls: calls }), reasoning_details: details };
    const frame = { id: "canary-response", object: "chat.completion.chunk", model: body.model,
      choices: [{ index: 0, delta, finish_reason: final ? "stop" : "tool_calls" }],
      usage: { prompt_tokens: 100, completion_tokens: 20, prompt_tokens_details: { cached_tokens: final ? 64 : 0 } } };
    return new Response(`data: ${JSON.stringify(frame)}\n\ndata: [DONE]\n\n`, { headers: { "Content-Type": "text/event-stream" } });
  }
  assert.equal(body.reasoning.effort, "max");
  const items = final ? [{ type: "message", id: "msg-canary", role: "assistant", content: [{ type: "output_text", text, annotations: [] }] }]
    : calls.map(call => ({ type: "function_call", id: `fc_${call.index}`, call_id: call.id, name: call.function.name, arguments: "{}" }));
  const frames: any[] = [{ type: "response.created", response: { id: "response-canary" } }];
  items.forEach((item, index) => {
    frames.push({ type: "response.output_item.added", output_index: index, item });
    if (final) frames.push({ type: "response.output_text.delta", output_index: index, delta: text });
    frames.push({ type: "response.output_item.done", output_index: index, item });
  });
  frames.push({ type: "response.completed", response: { id: "response-canary", model: body.model, output: items, status: "completed",
    usage: { input_tokens: 100, output_tokens: 20, input_tokens_details: { cached_tokens: final ? 64 : 0 } } } });
  return new Response(frames.map(frame => `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`).join(""), { headers: { "Content-Type": "text/event-stream" } });
}
