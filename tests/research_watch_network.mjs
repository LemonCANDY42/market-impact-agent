// Only network I/O is replaced; pinned pi handles tool-call decoding and the loop.
import assert from 'node:assert/strict';
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  if (process.env.RESEARCH_GENERATION_UNKNOWN === "1") throw new Error("synthetic unknown generation");
  assert.ok(body.tools.some(tool => tool.name === 'request_research_watch'));
  const hasResult = body.input.some(item => item.type === 'function_call_output');
  const item = hasResult
    ? {type: 'message', id: 'msg-final', role: 'assistant', content: [{type: 'output_text',
       text: process.env.RESEARCH_WATCH_ANSWER, annotations: []}]}
    : {type: 'function_call', id: 'fc-acquire', call_id: 'acquire-1', name: 'request_research_watch',
       arguments: process.env.RESEARCH_WATCH_ARGUMENTS};
  if (hasResult) {
    const result = JSON.stringify(body.input);
    assert.ok(result.includes('proposal_recorded'));
    if (process.env.WATCH_RELATIVE === '1') {
      const outputs = body.input.filter(item => item.type === 'function_call_output');
      assert.ok(!JSON.stringify(outputs).includes('2026-08-28'));
    }
  }
  const frames = [
    {type: 'response.created', response: {id: hasResult ? 'response-final' : 'response-acquire'}},
    {type: 'response.output_item.added', output_index: 0,
      item: hasResult ? item : {...item, arguments: ''}},
    hasResult ? {type: 'response.output_text.delta', output_index: 0, delta: process.env.RESEARCH_WATCH_ANSWER}
      : {type: 'response.function_call_arguments.delta', output_index: 0, delta: item.arguments},
    {type: 'response.output_item.done', output_index: 0, item},
    {type: 'response.completed', response: {id: hasResult ? 'response-final' : 'response-acquire',
      model: body.model, status: 'completed', output: [item],
      usage: {input_tokens: 100, output_tokens: 80, input_tokens_details: {cached_tokens: 0}, total_tokens: 180}}},
  ];
  return new Response(frames.map(frame => `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`).join(''),
      {headers: {'Content-Type': 'text/event-stream'}});
};
