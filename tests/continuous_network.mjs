// Synthetic wire responses; actual pinned pi loop, callbacks and journals run.
import assert from 'node:assert/strict';
const response = (item, model) => {
  const frames = [
    {type: 'response.created', response: {id: 'continuous-test'}},
    {type: 'response.output_item.added', output_index: 0, item},
    ...(item.type === 'message' ? [{type: 'response.output_text.delta', output_index: 0, delta: item.content[0].text}] : []),
    {type: 'response.output_item.done', output_index: 0, item},
    {type: 'response.completed', response: {id: 'continuous-test', model, status: 'completed', output: [item],
      usage: {input_tokens: 100, output_tokens: 80, input_tokens_details: {cached_tokens: 0}, total_tokens: 180}}},
  ];
  return new Response(frames.map(x => `event: ${x.type}\ndata: ${JSON.stringify(x)}\n\n`).join(''),
    {headers: {'Content-Type': 'text/event-stream'}});
};
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  if (process.env.CONTINUOUS_UNKNOWN_FIXTURE === '1') throw new Error('synthetic unknown dispatch');
  const user = [...body.input].reverse().find(x => x.role === 'user');
  const userText = typeof user.content === 'string' ? user.content : user.content.map(x => x.text ?? '').join('');
  const data = JSON.parse(userText);
  const portfolio = !!data.inputs?.account_state;
  const prior = !portfolio && !!data.prior_thesis;
  if (prior && !body.input.some(x => x.type === 'function_call_output')) {
    assert.ok(data.prior_thesis.terminal_hash);
    assert.ok(data.prior_thesis.journal_hash);
    assert.ok(body.tools.some(x => x.name === 'read_current_thesis'));
    return response({type: 'function_call', id: 'fc-recall', call_id: 'recall-1', name: 'read_current_thesis', arguments: '{}'}, body.model);
  }
  if (prior) {
    const result = JSON.parse(body.input.find(x => x.type === 'function_call_output').output).result;
    assert.equal(result.authority, 'prior_signed_opinion_not_source_fact');
    assert.equal(result.current_thesis.source.thesis, 'Prior evidence calls for a source close.');
  }
  const common = {primary_horizon_sessions: 1, horizon_band: 'immediate',
    priced_in_assessment: 'Only frozen prior-close evidence is considered.',
    transmission: ['frozen evidence -> desired exposure'], counter_scenario: 'The next release may contradict the thesis.',
    review_after_sessions: 1, counterevidence_refs: [], invalidation_conditions: ['Review on a new conflicting release.']};
  let answer;
  if (portfolio) {
    assert.equal(data.inputs.account_state.environment, 'backtest');
    assert.equal(data.inputs.mandate.currency, 'CNY');
    const hasPosition = data.inputs.account_state.positions.length > 0;
    answer = {...common, requested_action: hasPosition ? 'close' : 'open', rationale: 'Adjust this exact reconciled account.',
      evidence_refs: ['account_state', 'exposure_view', ...(data.evidence_ids.includes('release') ? ['release'] : [])], instrument_id: hasPosition ? '510300.SH' : '000001.SZ',
      venue: hasPosition ? 'XSHG' : 'XSHE', instrument_class: hasPosition ? 'exchange_traded_fund' : 'equity',
      direction: 'long', target_gross_exposure_ratio: hasPosition ? '0' : (process.env.CONTINUOUS_BUY_RATIO ?? '0.30')};
    if (hasPosition && process.env.CONTINUOUS_INITIAL_ROTATE === '1') {
      answer.requested_action = 'rotate';
      answer.rotation_source_instrument_id = '510300.SH';
      answer.instrument_id = '000001.SZ';
      answer.venue = 'XSHE';
      answer.instrument_class = 'equity';
      answer.target_gross_exposure_ratio = '0.30';
    }
  } else {
    answer = {...common, base_case_direction: prior ? 'up' : 'down',
      thesis: prior ? 'Updated evidence permits a new position.' : 'Prior evidence calls for a source close.',
      evidence_refs: ['release', 'market'], typed_unknowns: ['Future company execution remains unknown.']};
  }
  return response({type: 'message', id: 'msg-continuous', role: 'assistant', content: [{type: 'output_text', text: JSON.stringify(answer), annotations: []}]}, body.model);
};
