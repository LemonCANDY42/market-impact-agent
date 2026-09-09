// Only model wire I/O is replaced. The pinned pi tools and signed owners run.
import assert from 'node:assert/strict';
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  const user = [...body.input].reverse().find(x => x.role === 'user');
  const text = typeof user.content === 'string' ? user.content : user.content.map(x => x.text ?? '').join('');
  const data = JSON.parse(text);
  const portfolio = !!data.inputs?.account_state;
  const unknown = process.env.V2_MODE === 'unknown';
  const outputs = body.input.filter(x => x.type === 'function_call_output');
  let item;
  if (!portfolio && !unknown && outputs.length === 0) {
    item = {type:'function_call', id:'profile', call_id:'profile', name:'lookup_company_profile', arguments:JSON.stringify({ts_code:'000001.SZ'})};
  } else {
    const common = {primary_horizon_sessions:1, priced_in_assessment:'The frozen industry release is not reflected in the prior close.',
      transmission:unknown ? [] : ['Industry demand -> company revenue -> cash flow'],
      counter_scenario:'The demand change may not persist.', counterevidence_refs:[],
      invalidation_conditions:['A contrary industry release.'], review_after_sessions:1};
    let answer;
    if (portfolio) {
      assert.equal(data.schema_version, 'market-impact.portfolio-prompt-projection.v2');
      assert.equal(data.inputs.account_state.environment, 'backtest');
      assert.ok(data.inputs.account_state.positions.length > 0);
      assert.ok(data.targets['holding:510300.SH:buy']);
      if (!unknown) assert.ok(data.targets['candidate:000001.SZ']);
      else assert.ok(!data.targets['candidate:000001.SZ']);
      answer = {...common, requested_action:unknown ? 'hold' : 'open',
        rationale:unknown ? 'Industry evidence does not establish a company exposure; retain the reconciled account.' : 'The compared company has direct sensitivity to industry demand.',
        evidence_refs:['Current account cash and positions','Current portfolio exposure and risk limits'],
        ...(unknown ? {} : {target_ref:'candidate:000001.SZ', target_gross_exposure_ratio:'0.30'})};
    } else {
      assert.ok(data.evidence.some(x => x.reference.evidence_id === 'release'));
      answer = {...common, event_support:unknown ? 'unsupported' : 'supported',
        expectations:'Prior industry demand was weaker.', revision_conclusion:'Initial review of frozen industry evidence.',
        base_case_direction:unknown ? 'unknown' : 'up',
        thesis:unknown ? 'Available evidence does not identify a supported security exposure.' : 'Industry demand supports the directly exposed company.',
        evidence_refs:['release','market'], typed_unknowns:['Company execution and persistence are uncertain.'],
        candidate_comparisons:Object.entries(data.candidate_proofs ?? {}).map(([candidate_ref, refs]) => ({candidate_ref,
          transmission:['Industry demand -> company revenue'], support_refs:refs, counter_refs:[],
          comparison_reason:'The identified company is more directly exposed than the diversified holding.', gaps:['Demand persistence is uncertain.']}))};
    }
    item = {type:'message', id:'answer', role:'assistant', content:[{type:'output_text', text:JSON.stringify(answer), annotations:[]}]};
  }
  const frames = [
    {type:'response.created',response:{id:'v2'}},
    {type:'response.output_item.added',output_index:0,item},
    ...(item.type === 'message' ? [{type:'response.output_text.delta',output_index:0,delta:item.content[0].text}] : []),
    {type:'response.output_item.done',output_index:0,item},
    {type:'response.completed',response:{id:'v2',model:body.model,status:'completed',output:[item],usage:{input_tokens:100,output_tokens:80,input_tokens_details:{cached_tokens:0},total_tokens:180}}},
  ];
  return new Response(frames.map(x => `event: ${x.type}\ndata: ${JSON.stringify(x)}\n\n`).join(''), {headers:{'Content-Type':'text/event-stream'}});
};
