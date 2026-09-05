// Synthetic wire only; native pi and all Harness authority paths execute normally.
import assert from 'node:assert/strict';
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  const user = [...body.input].reverse().find(x => x.role === 'user');
  const text = typeof user.content === 'string' ? user.content : user.content.map(x => x.text ?? '').join('');
  const data = JSON.parse(text);
  const portfolio = !!data.inputs?.account_state;
  const candidate = data.target_id === '000001.SZ';
  const common = {horizon_band:'immediate',primary_horizon_sessions:1,
    priced_in_assessment:'The frozen news may already be reflected in prices.',
    transmission:['reported revenue -> expected cash flows'],counter_scenario:'The report may not generalize.',
    evidence_refs:['release','market'],counterevidence_refs:[],invalidation_conditions:['A contradictory release.'],review_after_sessions:1};
  const hasResult = body.input.some(x => x.type === 'function_call_output');
  let item;
  const followup = !!data.prior_thesis;
  if (followup && !portfolio) {
    assert.ok(text.includes('invalidation_conditions'));
    assert.ok(text.includes('counterevidence_refs'));
    const answer = {...common, evidence_refs:data.evidence.map(x => x.reference.evidence_id),
      base_case_direction:'up',thesis:'New actual receipt updates the counted prior thesis.', typed_unknowns:[]};
    item = {type:'message',id:'watch-followup',role:'assistant',content:[{type:'output_text',text:JSON.stringify(answer),annotations:[]}]};
  } else   if (!portfolio && !candidate && process.env.DISCOVERY_NO_CANDIDATE !== '1') {
    assert.ok(body.tools.some(t => t.name === 'lookup_company_profile'));
    assert.ok(text.includes('headline'));
    item = {type:'function_call',id:'fc-candidate',call_id:'candidate-profile-1',name:'lookup_company_profile',arguments:JSON.stringify({ts_code:'000001.SZ'})};
  } else if (candidate && !portfolio && !hasResult) {
    assert.ok(!text.includes('Synthetic discovered company'));
    assert.ok(text.includes('read_tool'));
    item = {type:'function_call',id:'fc-read-profile',call_id:'read-profile-1',name:'lookup_company_profile',arguments:JSON.stringify({ts_code:'000001.SZ',offset:0,limit:1})};
  } else if (candidate && !portfolio && process.env.DISCOVERY_WATCH === '1' && !JSON.stringify(body.input).includes('proposal_recorded')) {
    const tool = body.tools.find(t => t.name === 'request_research_watch');
    assert.ok(tool);
    item = {type:'function_call',id:'fc-watch',call_id:'watch-1',name:tool.name,arguments:JSON.stringify({
      delegate_profile_id:tool.parameters.properties.delegate_profile_id.enum[0],
      rationale:'Wait for a fresh policy fact.',watch_question:'Did the policy decision change?',
      evidence_refs:['release'],matcher:{clauses:[{field_path:'headline',mode:'contains_all',terms:['policy','decision']}]}
    })};
  } else {
    let answer;
    if (portfolio) {
      assert.equal(data.inputs.mandate.currency, 'CNY');
      answer = {...common,requested_action:'open',rationale:'Qualified candidate fits the reconciled account budget.',
        evidence_refs:['account_state','exposure_view'],instrument_id:'000001.SZ',venue:'XSHE',instrument_class:'equity',
        direction:'long',target_gross_exposure_ratio:'0.30'};
    } else {
      if (candidate) {
        assert.ok(JSON.stringify(body.input).includes('Synthetic discovered company'));
        assert.ok(JSON.stringify(body.input).includes('compact-facts-v2'));
        assert.ok(text.includes('data-snapshot-'));
      }
      answer = {...common,base_case_direction:'up',thesis:candidate ? 'Candidate research grounded in the acquired metadata and news.' : 'No candidate selected.',typed_unknowns:['Execution eligibility remains unverified.']};
    }
    item = {type:'message',id:'discovery-answer',role:'assistant',content:[{type:'output_text',text:JSON.stringify(answer),annotations:[]}]};
  }
  const frames = [
    {type:'response.created',response:{id:'discovery'}},
    {type:'response.output_item.added',output_index:0,item},
    ...(item.type === 'message' ? [{type:'response.output_text.delta',output_index:0,delta:item.content[0].text}] : []),
    {type:'response.output_item.done',output_index:0,item},
    {type:'response.completed',response:{id:'discovery',model:body.model,status:'completed',output:[item],usage:{input_tokens:100,output_tokens:80,input_tokens_details:{cached_tokens:0},total_tokens:180}}},
  ];
  return new Response(frames.map(x => `event: ${x.type}\ndata: ${JSON.stringify(x)}\n\n`).join(''), {headers:{'Content-Type':'text/event-stream'}});
};
