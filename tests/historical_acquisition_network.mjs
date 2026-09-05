import assert from 'node:assert/strict';
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  const user = [...body.input].reverse().find(x=>x.role==='user');
  const text = typeof user.content==='string' ? user.content : user.content.map(x=>x.text??'').join('');
  const data = JSON.parse(text);
  const portfolio = !!data.inputs?.account_state;
  const seedClose = process.env.HISTORY_SEED==='1' && portfolio && data.inputs.account_state.positions.length > 0;
  const result = body.input.find(x=>x.type==='function_call_output');
  const common={horizon_band:'immediate',primary_horizon_sessions:1,
    priced_in_assessment:'Historical price context is incomplete.',transmission:['price -> expectations'],
    counter_scenario:'Price may reflect unrelated conditions.',evidence_refs:(data.evidence??[]).map(x=>x.reference.evidence_id).slice(0,2),counterevidence_refs:[],
    invalidation_conditions:['New contrary evidence'],review_after_sessions:1};
  let item;
  if (!portfolio && !result && data.prior_thesis) {
    item={type:'function_call',id:'recall-call',call_id:'recall',name:'read_current_thesis',arguments:'{}'};
  } else if (!portfolio && !result) {
    item={type:'function_call',id:'price-call',call_id:'prices',name:process.env.HISTORY_SEED==='1'?'lookup_fund_prices':'lookup_stock_prices',arguments:JSON.stringify({ts_code:process.env.HISTORY_SEED==='1'?'510300.SH':(process.env.HISTORY_SYMBOL??'000001.SZ'),start_date:process.env.HISTORY_DAY??'20250102',end_date:process.env.HISTORY_DAY??'20250102'})};
  } else {
    if (!portfolio && !data.prior_thesis) {
      const payload=JSON.stringify(result);
      assert.ok(payload.includes('modeled-completed-raw-prices-v1'));
      assert.ok(payload.includes(data.point_in_time_cutoff.replace('Z','')));
      assert.ok(payload.includes('2026-08-28'));
      assert.ok(!payload.includes('999999999'));
    }
    if (data.prior_thesis) {
      const priorResult=JSON.parse(result.output).result;
      assert.equal(priorResult.current_thesis.source.thesis,'Prior evidence calls for a source close.');
      assert.equal(priorResult.authority,'prior_signed_opinion_not_source_fact');
    }
    const answer=portfolio ? {...common,requested_action:seedClose?'close':'open',rationale:'Act on the reconciled historical account.',evidence_refs:['account_state','exposure_view'],instrument_id:seedClose?'510300.SH':'000001.SZ',venue:seedClose?'XSHG':'XSHE',instrument_class:seedClose?'exchange_traded_fund':'equity',direction:'long',target_gross_exposure_ratio:seedClose?'0':'0.30'} : {...common,base_case_direction:'up',thesis:process.env.HISTORY_SEED==='1'?'Prior evidence calls for a source close.':'Modeled historical completed prices support a candidate thesis.',typed_unknowns:['Historical projection is not strict PIT.']};
    item={type:'message',id:'answer',role:'assistant',content:[{type:'output_text',text:JSON.stringify(answer),annotations:[]}]};
  }
  const frames=[{type:'response.created',response:{id:'history'}},{type:'response.output_item.added',output_index:0,item},...(item.type==='message'?[{type:'response.output_text.delta',output_index:0,delta:item.content[0].text}]:[]),{type:'response.output_item.done',output_index:0,item},{type:'response.completed',response:{id:'history',model:body.model,status:'completed',output:[item],usage:{input_tokens:100,output_tokens:80,input_tokens_details:{cached_tokens:0},total_tokens:180}}}];
  return new Response(frames.map(x=>`event: ${x.type}\ndata: ${JSON.stringify(x)}\n\n`).join(''),{headers:{'Content-Type':'text/event-stream'}});
};
