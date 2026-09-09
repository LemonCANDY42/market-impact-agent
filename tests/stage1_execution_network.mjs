// Replace external model I/O only: real pi, tools, signed ResearchThesis and Usage remain.
import assert from 'node:assert/strict';

globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  assert.ok(['gpt-5.6-luna', 'gpt-5.6-terra'].includes(body.model));
  const user = [...body.input].reverse().find(item => item.role === 'user');
  const text = typeof user.content === 'string' ? user.content : user.content.map(item => item.text ?? '').join('');
  const data = JSON.parse(text);
  assert.deepEqual(data.allowed_horizons, [5]);
  assert.equal(data.target_id, '510300.SH');
  assert.equal(data.schema_version, 'market-impact.research-thesis-inputs.v2');
  assert.ok(!text.includes('broad-rebound'));
  assert.ok(!text.includes('h5_return'));
  const evidenceId = data.evidence[0].reference.evidence_id;
  const event = data.evidence.some(item => item.reference.claim_id !== 'completed-session-price-history');
  const outputs = body.input.filter(item => item.type === 'function_call_output');
  let item;
  if (outputs.length === 0) {
    assert.ok(body.tools.some(tool => tool.name === 'read_evidence'));
    item = {type:'function_call', id:'read-source', call_id:'read-source', name:'read_evidence', arguments:JSON.stringify({evidence_id:evidenceId})};
  } else {
    const invalid = process.env.STAGE1_WIRE_MODE === 'invalid_one' && body.model === 'gpt-5.6-luna'
      && !event && data.point_in_time_cutoff.startsWith('2024');
    const answer = {
      primary_horizon_sessions:5, base_case_direction:event ? 'up' : 'unknown',
      event_support:event ? 'supported' : 'unsupported',
      thesis:'Synthetic test: event evidence changes a forecast but does not establish an investment edge.',
      expectations:'Investor expectations are not observed directly.',
      priced_in_assessment:'The prior completed prices do not identify causal pricing.',
      transmission:event ? ['Official response -> financing pressure -> market valuation'] : [],
      counter_scenario:'Weak demand or selling pressure may dominate the policy response.',
      evidence_refs:[invalid ? 'unbound-reference' : evidenceId], counterevidence_refs:[],
      invalidation_conditions:['Contrary source evidence'], review_after_sessions:1,
      typed_unknowns:['Future market response'], revision_new_facts:[], revision_old_assumptions:[],
      revision_conclusion:'Initial research using only the frozen supplied evidence.', candidate_comparisons:[],
    };
    item = {type:'message', id:'answer', role:'assistant', content:[{type:'output_text', text:JSON.stringify(answer), annotations:[]}]};
  }
  const frames = [
    {type:'response.created',response:{id:'stage1-test'}},
    {type:'response.output_item.added',output_index:0,item},
    ...(item.type === 'message' ? [{type:'response.output_text.delta',output_index:0,delta:item.content[0].text}] : []),
    {type:'response.output_item.done',output_index:0,item},
    {type:'response.completed',response:{id:'stage1-test',model:body.model,status:'completed',output:[item],usage:{input_tokens:100,output_tokens:80,input_tokens_details:{cached_tokens:0},total_tokens:180}}},
  ];
  return new Response(frames.map(frame => `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`).join(''), {headers:{'Content-Type':'text/event-stream'}});
};
