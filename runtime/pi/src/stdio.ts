/** Transport only. Production exposes run; the acceptance executable is separate. */
import { createInterface } from "node:readline";
import { readFileSync } from "node:fs";
import { RUNTIME, type Callback, type RunInput } from "./runtime.ts";

export function serve(runner: (input: RunInput, callback: Callback, signal: AbortSignal) => Promise<unknown>) {
  const maximum = 4_000_000;
  for (const name of ["pi-ai", "pi-agent-core"]) {
    const metadata = JSON.parse(readFileSync(new URL(`../node_modules/@earendil-works/${name}/package.json`, import.meta.url), "utf8"));
    if (metadata.version !== RUNTIME.upstream) throw new Error("Installed pi version differs from frozen runtime");
  }
  let sequence = 0;
  let active: AbortController | undefined;
  const pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void }>();
  function send(value: unknown) {
    const frame = JSON.stringify(value);
    if (Buffer.byteLength(frame) > maximum) throw new Error("IPC frame too large");
    process.stdout.write(frame + "\n");
  }
  const callback: Callback = async (method, payload) => new Promise((resolve, reject) => {
    const id = ++sequence;
    pending.set(id, { resolve, reject });
    send({ type: "callback", id, method, payload });
  });
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity });
  lines.on("line", line => {
    try {
      if (Buffer.byteLength(line) > maximum) throw new Error("IPC frame too large");
      const message = JSON.parse(line);
      if (message.type === "reply") {
        const waiter = pending.get(message.id);
        if (!waiter) throw new Error("Unknown callback reply");
        pending.delete(message.id);
        if (message.error) waiter.reject(new Error("Harness callback rejected"));
        else waiter.resolve(message.payload);
      } else if (message.type === "run" && !active) {
        active = new AbortController();
        void runner(message.payload, callback, active.signal).then(
          result => send({ type: "done", result }),
          () => send({ type: "failed", error: "pi runtime failed; inspect durable Harness callbacks" }),
        ).finally(() => { active = undefined; });
      } else if (message.type === "cancel") active?.abort();
      else throw new Error("Unexpected IPC message");
    } catch { process.exitCode = 1; lines.close(); }
  });
  lines.on("close", () => {
    active?.abort();
    for (const waiter of pending.values()) waiter.reject(new Error("Harness disconnected"));
    pending.clear();
  });
  send({ type: "ready", runtime: RUNTIME });
}
