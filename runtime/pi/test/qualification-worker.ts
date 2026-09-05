/** Acceptance executable only; never selected by the production model factory.
 * Native pi controls share only the mandatory physical admission/audit bridge.
 */
import {
  runAgentLoop, convertToLlm, prepareCompaction, compact, buildSessionContext,
  type AgentMessage, type AgentTool, type Entry,
} from "@earendil-works/pi-agent-core";
import type { Models } from "@earendil-works/pi-ai";
import { createInvocation, replayStream, type RunInput } from "../src/runtime.ts";
import { serve } from "../src/stdio.ts";

serve(async (raw, callback, signal) => {
  const input = raw as RunInput & { qualification: "direct" | "compression" };
  if (!["direct", "compression"].includes(input.qualification)) throw new Error("Unregistered qualification");
  const physical = createInvocation(input, callback, signal);
  const tools: AgentTool[] = input.tools.map(({ function: definition }) => ({
    ...definition, label: definition.name,
    execute: async (call_id, args) => {
      const result = await physical.guarded("tool", { call_id, name: definition.name, arguments: args });
      return { content: [{ type: "text", text: result.content as string }], details: {} };
    },
  }));
  const fixed: AgentMessage[] = input.messages.filter(m => m.role === "user")
    .map(m => ({ role: "user", content: String(m.content), timestamp: 0 }));
  const context = {
    systemPrompt: input.messages.filter(m => m.role === "system").map(m => m.content).join("\n\n"),
    messages: [...fixed, ...(input.nativeMessages ?? [])], tools,
  };
  if (input.qualification === "compression") {
    const entries: Entry[] = [];
    const base = () => ({ id: `${input.runId}:${entries.length}`, seq: entries.length,
      parentId: entries.at(-1)?.id ?? null, timestamp: 0 });
    for (const message of input.nativeMessages ?? []) entries.push({ ...base(), type: "message", message });
    const summaryModels: Models = { ...physical.models,
      completeSimple: (_model, context, options) => physical.invoke(context, "compaction", options) };
    for (let number = 1; number <= 2; number++) {
      // A new review instruction is a real continuation, not duplicated evidence.
      entries.push({ ...base(), type: "message", message: { role: "user", timestamp: 0,
        content: `Review ${number}: retain evidence references, the observed outage duration, unresolved exposure questions and the authority boundary. Do not treat the summary as original evidence.` } });
      const saved = await physical.guarded("compaction_lookup", { number });
      let entry: Entry;
      if (saved.entry) {
        entry = saved.entry;
        physical.restoreCallNumber(saved.call_number);
      } else {
        const prepared = prepareCompaction(entries, { enabled: true, reserveTokens: 4096, keepRecentTokens: 0 });
        if (!prepared.ok || !prepared.value || prepared.value.isSplitTurn) throw new Error("Qualification needs a whole-turn safe cut");
        const result = await compact(prepared.value, summaryModels, physical.model,
          "Keep source references. A summary is not evidence or trading permission.", signal,
          input.profile.reasoning_effort ?? undefined,
          { enabled: false, maxRetries: 0, baseDelayMs: 0 });
        if (!result.ok) throw new Error("Native compaction failed");
        entry = { ...base(), type: "compaction", ...result.value };
        await physical.guarded("compaction_commit", { number, entry, call_number: physical.callNumber });
      }
      entries.push(entry);
    }
    context.messages = [...fixed, ...buildSessionContext(entries).messages, {
      role: "user", timestamp: 0,
      content: "Return the required JudgmentProposal based on the already-read selected evidence. State the observed duration numerically. No new evidence or trading permission has been granted.",
    }];
  }
  let terminal: Record<string, unknown> | undefined;
  let followUp: AgentMessage[] = [];
  await runAgentLoop([], context, {
    model: physical.model, convertToLlm, toolExecution: "sequential", maxRetries: 0,
    shouldStopAfterTurn: async ({ message }) => {
      const outcome = await physical.guarded("turn_end", { message });
      if (outcome.correction) followUp = [{ role: "user", content: outcome.correction, timestamp: 0 }];
      if (outcome.stop) terminal = outcome;
      return !!outcome.stop;
    },
    getFollowUpMessages: async () => { const pending = followUp; followUp = []; return pending; },
  }, async event => {
    if (event.type === "message_end" && event.message.role === "toolResult")
      await physical.guarded("tool_message", { message: event.message });
    if (event.type === "agent_end") await physical.guarded("agent_end", { call_number: physical.callNumber });
  }, signal, async (_model, context, options) => replayStream(await physical.invoke(context, "decision", options)));
  if (physical.fatal) throw physical.fatal;
  if (!terminal) throw new Error("Qualification ended without audited completion");
  return terminal;
});
