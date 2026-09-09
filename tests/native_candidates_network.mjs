// Replace wire only: all native pi calls, results and signed stores are real.
import assert from 'node:assert/strict';
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  const symbols = JSON.parse(process.env.CANDIDATE_SYMBOLS);
  const outputs = body.input.filter(x => x.type === 'function_call_output');
  if (process.env.EXPECT_MODELED === '1') {
    assert.ok(!JSON.stringify(outputs).includes('Synthetic company'));
    assert.ok(!JSON.stringify(outputs).includes('list_status'));
  }
  let item;
  if (outputs.length < symbols.length) {
    const i = outputs.length;
    item = {type:'function_call', id:`fc-${i}`, call_id:`profile-${i}`,
      name:'lookup_company_profile', arguments:JSON.stringify({ts_code:symbols[i]})};
  } else {
    if (process.env.EXPECT_CANDIDATE_LIMIT === '1')
      assert.ok(JSON.stringify(outputs).includes('candidate_limit_exceeded'));
    const user = [...body.input].reverse().find(x => x.role === 'user');
    const text = typeof user.content === 'string' ? user.content : user.content.map(x=>x.text ?? '').join('');
    const data = JSON.parse(text);
    const answer = JSON.parse(process.env.CANDIDATE_ANSWER);
    answer.candidate_comparisons = Object.entries(data.candidate_proofs ?? {}).map(([candidate_ref, refs]) => ({
      candidate_ref, transmission:['News -> cash flows'], support_refs:refs.slice(0,1), counter_refs:[],
      comparison_reason:'The company has a direct event linkage.',gaps:['Execution eligibility is unverified.']
    }));
    item = {type:'message',id:'answer',role:'assistant',content:[{type:'output_text',
      text:JSON.stringify(answer),annotations:[]}]};
  }
  const frames = [
    {type:'response.created',response:{id:'candidates'}},
    {type:'response.output_item.added',output_index:0,item},
    ...(item.type === 'message' ? [{type:'response.output_text.delta',output_index:0,delta:item.content[0].text}] : []),
    {type:'response.output_item.done',output_index:0,item},
    {type:'response.completed',response:{id:'candidates',model:body.model,status:'completed',output:[item],usage:{input_tokens:100,output_tokens:80,input_tokens_details:{cached_tokens:0},total_tokens:180}}},
  ];
  return new Response(frames.map(x => `event: ${x.type}\ndata: ${JSON.stringify(x)}\n\n`).join(''), {headers:{'Content-Type':'text/event-stream'}});
};
