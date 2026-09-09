/** Pinned upstream mechanisms; the callback owns every durable/financial decision. */
import { setTimeout as delay } from "node:timers/promises";
import { dirname } from "node:path";
import { formatSkillsForPrompt, createSyntheticSourceInfo } from "@earendil-works/pi-coding-agent";
import { Stream } from "openai/core/streaming";
import {
  createModels, createProvider, createAssistantMessageEventStream,
  type AssistantMessage, type Context, type Model, type Models, type SimpleStreamOptions,
  type TSchema, type ThinkingLevel,
} from "@earendil-works/pi-ai";
import { openAICompletionsApi } from "@earendil-works/pi-ai/api/openai-completions.lazy";
import { openAIResponsesApi } from "@earendil-works/pi-ai/api/openai-responses.lazy";
import {
  runAgentLoop, prepareCompaction, compact, buildSessionContext, convertToLlm,
  estimateContextTokens, estimateTokens, calculateContextTokens, DEFAULT_COMPACTION_SETTINGS,
  type AgentContext, type AgentMessage, type AgentTool, type Entry,
} from "@earendil-works/pi-agent-core";

export const RUNTIME = {
  adapter: "market-impact-pi-v2", upstream: "0.84.4",
  revision: "b79e4cc834970cca69daebffab7df1da7d1e52c4",
} as const;
export type Callback = (method: string, payload: Record<string, unknown>) => Promise<Record<string, any>>;
export interface RunInput {
  runId: string;
  conversationId?: string;
  cacheKey?: string;
  profile: {
    provider_id: string; model: string; origin: string; api_path: string;
    credential_env: string; reasoning_effort: ThinkingLevel | null;
    context_window_tokens: number; reserved_output_tokens: number;
    temperature: number; top_p: number;
    runtime: { api: "openai-responses" | "openai-completions";
      request_options: Record<string, unknown>; supported_efforts: string[]; context_estimator?: "pi-usage-v1" };
  };
  messages: { role: string; content: unknown }[];
  tools: { function: { name: string; description: string; parameters: TSchema } }[];
  compaction?: { keepRecentTokens: number; reserveTokens: number };
  nativeMessages?: AgentMessage[];
  skills?: { name: string; description: string; filePath: string }[];
}

export function replayStream(message: AssistantMessage) {
  const stream = createAssistantMessageEventStream();
  if (message.stopReason === "error" || message.stopReason === "aborted") {
    stream.push({ type: "error", reason: message.stopReason, error: message });
  } else {
    stream.push({ type: "done", reason: message.stopReason as "stop" | "length" | "toolUse", message });
  }
  stream.end(message);
  return stream;
}

/** Public pi accounting: actual usage plus the unmeasured suffix, or size estimation. */
export function estimatePiContext(context: Context, allowUsage = true) {
  const messages = context.messages as AgentMessage[];
  const measured = estimateContextTokens(messages);
  const estimate = allowUsage ? measured : {
    tokens: messages.reduce((n, m) => n + estimateTokens(m), 0),
    usageTokens: 0, trailingTokens: messages.reduce((n, m) => n + estimateTokens(m), 0), lastUsageIndex: null,
  };
  // A usage receipt already includes the system prompt and tool definitions.
  const fixedTokens = estimate.lastUsageIndex === null ? estimateTokens({
    role: "user", content: JSON.stringify({ systemPrompt: context.systemPrompt, tools: context.tools }), timestamp: 0,
  }) : 0;
  return { kind: "pi-usage-v1", ...estimate, fixedTokens, tokens: estimate.tokens + fixedTokens };
}

/** Presence comes from the official SDK's SSE decoder, not zero-filled pi defaults. */
async function observeUsage(response: Response, signal: AbortSignal) {
  let usage: Record<string, unknown> | null = null;
  const models = new Set<string>();
  let bytes = 0;
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal.addEventListener("abort", abort, { once: true });
  try {
    for await (const frame of Stream.fromSSEResponse<any>(response, controller)) {
      bytes += JSON.stringify(frame).length;
      if (bytes > 8_000_000) throw new Error("Provider response exceeds capture limit");
      if (frame.usage) usage = frame.usage;
      if (frame.response?.usage) usage = frame.response.usage;
      const model = frame.response?.model ?? frame.model;
      if (typeof model === "string" && model) models.add(model);
    }
    return { usage, models: [...models] };
  } finally {
    signal.removeEventListener("abort", abort);
    controller.abort();
  }
}

/** One physical-call bridge, also used by isolated upstream acceptance controls. */
export function createInvocation(input: RunInput, callback: Callback, signal: AbortSignal) {
  const p = input.profile;
  const api = p.runtime.api;
  const expectedUrl = new URL(p.api_path, p.origin).href;
  if (new URL(expectedUrl).origin !== p.origin) throw new Error("Unpinned origin");
  const model: Model<typeof api> = {
    id: p.model, name: p.model, provider: p.provider_id, api,
    baseUrl: expectedUrl.slice(0, expectedUrl.lastIndexOf("/")),
    reasoning: !!p.reasoning_effort, thinkingLevelMap: { xhigh: "xhigh", max: "max" },
    input: ["text"], contextWindow: p.context_window_tokens, maxTokens: p.reserved_output_tokens,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  };
  // Chat's suffix is /chat/completions rather than /responses.
  if (api === "openai-completions") model.baseUrl = expectedUrl.slice(0, -"/chat/completions".length);
  const models = createModels();
  models.setProvider(createProvider({
    id: p.provider_id, models: [model],
    auth: { apiKey: { name: p.provider_id, resolve: async () => ({ auth: {} }) } },
    api: api === "openai-responses" ? openAIResponsesApi() : openAICompletionsApi(),
  }));
  let callNumber = 0;
  let fatal: unknown;
  // Imported history and retained pre-compaction messages have stale usage.
  let currentContextUsage = !input.nativeMessages?.length;
  const contextPayload = (context: Context) => ({ context,
    ...(p.runtime.context_estimator === "pi-usage-v1"
      ? { context_estimate: estimatePiContext(context, currentContextUsage) } : {}),
  });
  const recordContextUsage = (message: AssistantMessage, purpose: string) => {
    if (purpose === "decision" && !["error", "aborted"].includes(message.stopReason) && calculateContextTokens(message.usage) > 0)
      currentContextUsage = true;
  };
  const guarded = async (method: string, payload: Record<string, unknown>) => {
    if (fatal) throw fatal;
    try { return await callback(method, payload); }
    catch (error) { fatal = error; throw error; }
  };
  const invoke = async (context: Context, purpose: string, options: SimpleStreamOptions = {}) => {
    signal.throwIfAborted();
    const number = ++callNumber;
    const admitted = await guarded("model_admit", { number, purpose, ...contextPayload(context), runtime: RUNTIME });
    if (admitted.replay) {
      recordContextUsage(admitted.replay as AssistantMessage, purpose);
      return admitted.replay as AssistantMessage;
    }
    const key = process.env[p.credential_env];
    if (!key) throw new Error("Configured credential environment is unavailable");
    let attempts = 0;
    let capture: Promise<{ usage: Record<string, unknown> | null; models: string[] }> = Promise.resolve({ usage: null, models: [] });
    const started = performance.now();
    const pinnedFetch: typeof fetch = async (url, init) => {
      const request = new Request(url, init);
      if (request.url !== expectedUrl || request.method !== "POST") throw new Error("Unpinned Provider request");
      for (;;) {
        signal.throwIfAborted();
        const attempt = ++attempts;
        await guarded("attempt_start", { number, attempt });
        let response: Response;
        try { response = await fetch(request.clone(), { redirect: "error", signal }); }
        catch {
          await guarded("attempt_end", { number, attempt, status: null, latency_ms: performance.now() - started });
          throw new Error("Provider transport failed; generation state unknown");
        }
        if (response.ok) {
          capture = observeUsage(response.clone(), signal);
          // Attach immediately: malformed streams must not become unhandled rejections.
          capture.catch(() => undefined);
          return response;
        }
        // Only a bounded machine error code crosses IPC; no raw error text/headers.
        let errorCode: string | undefined;
        const errorReader = response.body?.getReader();
        if (errorReader) {
          const part = await errorReader.read();
          if (part.value && part.value.byteLength <= 16_384) {
            try {
              const body = JSON.parse(new TextDecoder().decode(part.value));
              const code = body.error?.code ?? body.error?.type;
              if (typeof code === "string" && /^[a-zA-Z0-9_]{1,80}$/.test(code)) errorCode = code;
            } catch { /* Missing code is unknown, not proof of a transient rate limit. */ }
          }
          await errorReader.cancel();
          errorReader.releaseLock();
        }
        await response.body?.cancel();
        const outcome = await guarded("attempt_end", {
          number, attempt, status: response.status,
          retry_after: response.headers.get("retry-after"), error_code: errorCode,
          latency_ms: performance.now() - started,
        });
        if (!outcome.retry) throw new Error(`Provider HTTP ${response.status}; no retry admitted`);
        await delay(outcome.delay_ms, undefined, { signal });
      }
    };
    const response = await models.completeSimple(model, context, {
      ...options, apiKey: key, signal, fetch: pinnedFetch, maxRetries: 0,
      transport: "sse", reasoning: p.reasoning_effort ?? undefined,
      temperature: p.temperature, maxTokens: Math.min(options.maxTokens ?? admitted.max_output, admitted.max_output),
      sessionId: purpose === "decision" ? input.conversationId : undefined,
      cacheRetention: purpose === "decision" ? "short" : "none",
      onPayload: payload => {
        const body = payload as Record<string, unknown>;
        const maximum = body.max_output_tokens ?? body.max_completion_tokens ?? body.max_tokens;
        if (typeof maximum !== "number" || maximum > admitted.max_output)
          throw new Error("Upstream payload exceeds admitted output budget");
        return {
          ...body, ...p.runtime.request_options, top_p: p.top_p,
          // Separate conversation tracing from OpenAI's documented cache routing key.
          ...(api === "openai-responses" && purpose === "decision"
            ? { prompt_cache_key: input.cacheKey } : {}),
        };
      },
    });
    const observed = await capture;
    await guarded("model_completed", {
      number, purpose, message: response, raw_usage: observed.usage, response_models: observed.models, attempts,
      latency_ms: performance.now() - started, runtime: RUNTIME,
    });
    recordContextUsage(response, purpose);
    return response;
  };
  return { model, models, invoke, guarded,
    checkContext: (context: Context) => guarded("context_check", contextPayload(context)),
    invalidateContextUsage() { currentContextUsage = false; },
    get fatal() { return fatal; },
    get callNumber() { return callNumber; },
    restoreCallNumber(number: number) { callNumber = number; },
  };
}

export async function run(input: RunInput, callback: Callback, signal: AbortSignal) {
  const p = input.profile;
  const invocation = createInvocation(input, callback, signal);
  const { model, models, invoke, guarded } = invocation;
  const tools: AgentTool[] = input.tools.map(({ function: definition }) => ({
    ...definition, label: definition.name,
    execute: async (toolCallId, args) => {
      const result = await guarded("tool", { call_id: toolCallId, name: definition.name, arguments: args });
      return { content: [{ type: "text", text: result.content as string }], details: {} };
    },
  }));
  const fixedMessages: AgentMessage[] = input.messages.filter(m => m.role !== "system").map(m => {
    if (m.role !== "user") throw new Error("Imported continuation requires native pi messages");
    return { role: "user", content: typeof m.content === "string" ? m.content : JSON.stringify(m.content), timestamp: 0 };
  });
  const skillPrompt = formatSkillsForPrompt((input.skills ?? []).map(skill => ({
    ...skill, baseDir: dirname(skill.filePath), disableModelInvocation: false,
    sourceInfo: createSyntheticSourceInfo(skill.filePath, { source: "frozen-harness-catalog" }),
  })));
  const systemPrompt = input.messages.filter(m => m.role === "system").map(m => m.content).join("\n\n") + skillPrompt;
  let entries: Entry[] = [];
  let correction: AgentMessage[] = [];
  let terminal: Record<string, any> | undefined;
  let compactions = 0;
  const entryBase = () => ({ id: `${input.runId}:${entries.length}`, seq: entries.length,
    parentId: entries.at(-1)?.id ?? null, timestamp: 0 });
  const summaryModels: Models = {
    ...models,
    completeSimple: (_model, context, options) => invoke(context, "compaction", options),
  };
  for (const message of input.nativeMessages ?? []) entries.push({ ...entryBase(), type: "message", message });
  // Same chronology as upstream runAgentLoop(prompts, context): old history,
  // then the current task. Never ask a provider to continue an old final answer.
  const context: AgentContext = { systemPrompt, messages: [...(input.nativeMessages ?? []), ...fixedMessages], tools };
  await runAgentLoop([], context, {
    model, convertToLlm, toolExecution: "sequential", maxRetries: 0,
    shouldStopAfterTurn: async ({ message }) => {
      if (invocation.fatal) return true;
      const result = await guarded("turn_end", { message });
      if (result.correction) correction = [{ role: "user", content: result.correction, timestamp: 0 }];
      if (result.stop) terminal = result;
      return !!result.stop;
    },
    getFollowUpMessages: async () => { const pending = correction; correction = []; return pending; },
    prepareNextTurn: async ({ context: current }) => {
      signal.throwIfAborted();
      if (invocation.fatal) throw invocation.fatal;
      // Harness owns limits; pinned pi owns token estimates, cuts and summaries.
      const required = await invocation.checkContext({
        systemPrompt: current.systemPrompt, messages: convertToLlm(current.messages), tools: input.tools.map(t => t.function),
      });
      if (!required.compact) return;
      const replay = await guarded("compaction_lookup", { number: compactions + 1 });
      let entry: Entry;
      if (replay.entry) entry = replay.entry as Entry;
      else {
        const settings = { enabled: true, reserveTokens: input.compaction?.reserveTokens ?? p.reserved_output_tokens,
          keepRecentTokens: input.compaction?.keepRecentTokens ?? (p.runtime.context_estimator === "pi-usage-v1"
            ? DEFAULT_COMPACTION_SETTINGS.keepRecentTokens : Math.floor(p.context_window_tokens / 16)) };
        const preparation = prepareCompaction(entries, settings);
        if (!preparation.ok || !preparation.value) throw new Error("No safe compaction boundary");
        const result = await compact(preparation.value, summaryModels, model,
          "Evidence references must remain references. Summaries are not original evidence. " +
          "The following task remains pinned outside the summarized history. Use it to describe " +
          "the current goal; do not execute it while summarizing: " +
          JSON.stringify({ systemPrompt, messages: fixedMessages }), signal,
          p.reasoning_effort ?? undefined, { enabled: false, maxRetries: 0, baseDelayMs: 0 });
        if (!result.ok) throw new Error("Upstream compaction failed");
        entry = { ...entryBase(), type: "compaction", ...result.value };
        await guarded("compaction_commit", { number: compactions + 1, entry, call_number: invocation.callNumber });
      }
      if (replay.call_number) invocation.restoreCallNumber(replay.call_number);
      compactions++;
      entries.push(entry);
      invocation.invalidateContextUsage();
      return { context: { ...current, messages: [...fixedMessages, ...buildSessionContext(entries).messages] } };
    },
  }, async event => {
    if (invocation.fatal) throw invocation.fatal;
    if (event.type === "message_end") {
      entries.push({ ...entryBase(), type: "message", message: event.message });
      if (event.message.role === "toolResult") await guarded("tool_message", { message: event.message });
    }
    if (event.type === "agent_end") await guarded("agent_end", { call_number: invocation.callNumber });
  }, signal, async (_model, request, options) => replayStream(await invoke(request, "decision", options)));
  if (invocation.fatal) throw invocation.fatal;
  if (!terminal) throw new Error("pi exited without a Harness terminal decision");
  return terminal;
}
