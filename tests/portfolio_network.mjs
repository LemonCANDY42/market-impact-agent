// Test-only network replacement. The actual pinned pi modules decode and run it.
import assert from 'node:assert/strict';
globalThis.fetch = async (input, init) => {
  const request = new Request(input, init);
  assert.equal(request.url, 'http://127.0.0.1:8317/v1/responses');
  const body = await request.json();
  assert.ok(!body.tools?.length);
  let answer = process.env.PORTFOLIO_FIXTURE_ANSWER;
  assert.ok(answer);
  if (process.env.DYNAMIC_STUDY_FIXTURE === '1') {
    const serialized = JSON.stringify(body);
    const userItem = Array.isArray(body.input)
      ? [...body.input].reverse().find(item => item.role === 'user')
      : undefined;
    const userText = typeof userItem?.content === 'string'
      ? userItem.content
      : Array.isArray(userItem?.content)
        ? userItem.content.map(item => item.text ?? item.content ?? '').join('')
        : '';
    if (serialized.includes('Recommend an action for the entire supplied account')) {
      const review = JSON.parse(userText);
      const direction = review.research_theses[0].thesis.base_case_direction;
      const positions = review.inputs.account_state.positions;
      let action = 'open';
      if (direction === 'rangebound') action = 'hold';
      else if (direction === 'down') action = 'reduce';
      else if (positions.some(position => position.concentration === '0.8')) action = 'reduce';
      const proposal = {
        requested_action: action,
        rationale: action === 'hold'
          ? 'Maintain cash because the rangebound thesis does not justify turnover.'
          : 'Adjust the simulated account target while respecting its current exposure.',
        horizon_band: 'tactical', primary_horizon_sessions: 5,
        priced_in_assessment: 'The account action reflects both the frozen thesis and current exposure.',
        transmission: ['frozen thesis -> portfolio target -> bounded account action'],
        counter_scenario: 'New evidence could invalidate the frozen thesis.',
        review_after_sessions: 1,
        evidence_refs: ['account_state', 'exposure_view'], counterevidence_refs: [],
        invalidation_conditions: ['Review when new evidence changes the thesis.'],
      };
      if (action !== 'hold') Object.assign(proposal, {
        instrument_id: 'broad-market-a', venue: 'ARCX',
        instrument_class: 'exchange_traded_fund', direction: 'long',
        target_gross_exposure_ratio: action === 'reduce' ? '0.40' : '0.50',
      });
      answer = JSON.stringify(proposal);
    } else {
      const thesis = JSON.parse(answer);
      if (serialized.includes('2020-02-03')) thesis.base_case_direction = 'down';
      else if (serialized.includes('2021-07-01')) thesis.base_case_direction = 'rangebound';
      answer = JSON.stringify(thesis);
    }
  }
  if (JSON.parse(answer).__network_failure) throw new Error('synthetic ambiguous transport');
  if (JSON.parse(answer).__hang) {
    await new Promise((_, reject) => {
      request.signal.addEventListener('abort', () => reject(new Error('synthetic abort')), {once: true});
      if (request.signal.aborted) reject(new Error('synthetic abort'));
    });
  }
  const item = {type: 'message', id: 'msg-portfolio', role: 'assistant',
    content: [{type: 'output_text', text: answer, annotations: []}]};
  const frames = [
    {type: 'response.created', response: {id: 'portfolio-response'}},
    {type: 'response.output_item.added', output_index: 0, item},
    {type: 'response.output_text.delta', output_index: 0, delta: answer},
    {type: 'response.output_item.done', output_index: 0, item},
    {type: 'response.completed', response: {id: 'portfolio-response', model: body.model,
      status: 'completed', output: [item], usage: {input_tokens: 100, output_tokens: 80,
      input_tokens_details: {cached_tokens: 0}, total_tokens: 180}}},
  ];
  return new Response(frames.map(frame => `event: ${frame.type}\ndata: ${JSON.stringify(frame)}\n\n`).join(''),
    {headers: {'Content-Type': 'text/event-stream'}});
};
