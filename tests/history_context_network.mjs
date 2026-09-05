// Actual pinned pi provider protocol; only the external transport is synthetic.
import assert from 'node:assert/strict';
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  const outputs = body.input.filter(item => item.type === 'function_call_output');
  const compactionMode = process.env.HISTORY_COMPACTION === '1';
  const summary = compactionMode && !body.tools?.length;
  const contextText = JSON.stringify(body.input);
  const priorNumbers = [...contextText.matchAll(/history-(\d+)/g)].map(match => Number(match[1]));
  const next = Math.max(-1, ...priorNumbers) + 1;
  const number = compactionMode
    ? Math.max(next, contextText.includes('NEXT_HISTORY_STEP=1') ? 1 : 0)
    : outputs.length;
  const maximum = Number(process.env.HISTORY_TOOL_CALLS ?? '1');
  const item = !summary && number < maximum
    ? {type: 'function_call', id: `fc-${number}`, call_id: `history-${number}`,
       name: body.tools[0].name, arguments: '{}'}
    : {type: 'message', id: 'final', role: 'assistant', content: [{type:'output_text', text:summary ? 'NEXT_HISTORY_STEP=1. Prior opinions were read; source references remain authoritative.' : 'done', annotations:[]}]};
  const frames = [
    {type:'response.created', response:{id:`history-response-${number}`, model:body.model}},
    {type:'response.output_item.added', output_index:0, item},
    {type:'response.output_item.done', output_index:0, item},
    {type:'response.completed', response:{id:`history-response-${number}`, model:body.model, status:'completed',
      output:[item], usage:{input_tokens:100, output_tokens:20,
      input_tokens_details:{cached_tokens:0}, total_tokens:120}}},
  ];
  return new Response(frames.map(frame => `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`).join(''),
    {headers:{'Content-Type':'text/event-stream'}});
};
