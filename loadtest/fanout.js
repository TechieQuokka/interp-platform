// Fan-out load test: a few speakers, thousands of listeners spread over both gateways and all
// languages. Every listener checks seq continuity; a share of them disconnects and resumes with
// lastSeq to exercise replay under load.
//
//   docker run --rm --network host -v "$PWD/loadtest:/scripts" grafana/k6:2.3.0 run \
//     -e LISTENERS=3000 /scripts/fanout.js
import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { WebSocket } from 'k6/websockets';
import { setTimeout, clearTimeout } from 'k6/timers';

const GATEWAYS = (__ENV.GATEWAYS || 'http://localhost:8081,http://localhost:8082').split(',');
const LANGS = ['ko', 'en', 'ja'];
const SESSIONS = Number(__ENV.SESSIONS || 3);
const LISTENERS = Number(__ENV.LISTENERS || 3000);
const RAMP_S = Number(__ENV.RAMP_S || 30);
const SPEAK_S = Number(__ENV.SPEAK_S || 120);
const DRAIN_S = Number(__ENV.DRAIN_S || 15);
const UTTER_INTERVAL_S = Number(__ENV.UTTER_INTERVAL_S || 2);
const RECONNECT_EVERY = Number(__ENV.RECONNECT_EVERY || 5); // every Nth listener reconnects periodically

const e2eLatency = new Trend('interp_e2e_latency', true);
const fanoutLatency = new Trend('interp_fanout_latency', true);
const received = new Counter('interp_received');
const duplicates = new Counter('interp_duplicates');
const gaps = new Counter('interp_gap_messages');
const missing = new Counter('interp_missing_at_end');
const reconnects = new Counter('interp_reconnects');
const unexpectedCloses = new Counter('interp_unexpected_closes');
const utterances = new Counter('interp_utterances');
const listenersWithData = new Counter('interp_listeners_with_data');

export const options = {
  setupTimeout: '60s',
  scenarios: {
    listeners: {
      executor: 'per-vu-iterations',
      exec: 'listen',
      vus: LISTENERS,
      iterations: 1,
      maxDuration: `${RAMP_S + SPEAK_S + DRAIN_S + 60}s`,
    },
    speakers: {
      executor: 'constant-vus',
      exec: 'speak',
      vus: SESSIONS,
      startTime: `${RAMP_S}s`,
      duration: `${SPEAK_S}s`,
    },
  },
  thresholds: {
    interp_gap_messages: ['count==0'],
    interp_missing_at_end: ['count==0'],
    interp_duplicates: ['count==0'],
    interp_unexpected_closes: ['count==0'],
    // Every listener must actually receive traffic, otherwise the zero-loss checks prove nothing.
    interp_listeners_with_data: [`count==${LISTENERS}`],
    'http_req_duration{name:utterance}': ['p(95)<200'],
    'http_req_duration{name:final-check}': ['max>=0'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

export function setup() {
  const sessions = [];
  for (let i = 0; i < SESSIONS; i++) {
    sessions.push(http.post(`${GATEWAYS[0]}/sessions`).json('sessionId'));
  }
  return { sessions };
}

export function speak(data) {
  // Rotate by iteration: k6 does not number VUs per scenario, so exec.vu.idInTest cannot map
  // speakers to sessions (it once left one session silent and doubled up another).
  const sessionId = data.sessions[exec.scenario.iterationInTest % data.sessions.length];
  const body = JSON.stringify({ utteranceId: crypto.randomUUID(), text: `utterance at ${Date.now()}` });
  // Retries reuse the utteranceId, so an attempt that timed out but was committed is not duplicated.
  for (let attempt = 0; attempt < GATEWAYS.length; attempt++) {
    const gateway = GATEWAYS[(exec.scenario.iterationInTest + attempt) % GATEWAYS.length];
    const res = http.post(`${gateway}/sessions/${sessionId}/utterances`, body,
      { headers: { 'Content-Type': 'application/json' }, tags: { name: 'utterance' } });
    if (res.status === 202) {
      utterances.add(1);
      break;
    }
  }
  sleep(UTTER_INTERVAL_S);
}

export function listen(data) {
  const id = exec.vu.idInTest;
  const sessionId = data.sessions[id % data.sessions.length];
  const lang = LANGS[id % LANGS.length];
  const gateway = GATEWAYS[id % GATEWAYS.length];
  const flaky = id % RECONNECT_EVERY === 0;

  // Spread connects over the ramp, then stay until speakers are done and the pipeline drained.
  sleep(Math.random() * RAMP_S);
  const endAt = Date.now() + (RAMP_S + SPEAK_S + DRAIN_S) * 1000 - (Date.now() - exec.scenario.startTime);

  let last = 0;
  const connect = () => {
    // lastSeq=0 on the first connect replays from the start, so late joiners must not see gaps either.
    const ws = new WebSocket(`${gateway.replace('http', 'ws')}/ws/listen?session=${sessionId}&lang=${lang}&lastSeq=${last}`);
    let closingByUs = false;
    let timer;
    ws.onopen = () => {
      const stayMs = flaky ? Math.min(5000 + Math.random() * 15000, endAt - Date.now()) : endAt - Date.now();
      timer = setTimeout(() => {
        closingByUs = true;
        ws.close();
      }, Math.max(stayMs, 0));
    };
    ws.onmessage = (event) => {
      const now = Date.now();
      const m = JSON.parse(event.data);
      if (m.seq <= last) {
        duplicates.add(1);
        return;
      }
      if (m.seq > last + 1) gaps.add(m.seq - last - 1);
      last = m.seq;
      received.add(1);
      e2eLatency.add(now - m.ingestedAtMs);
      fanoutLatency.add(now - m.translatedAtMs);
    };
    ws.onclose = () => {
      clearTimeout(timer);
      if (!closingByUs) unexpectedCloses.add(1);
      if (Date.now() < endAt - 1000) {
        reconnects.add(1);
        connect();
      } else {
        const lastSeq = http.get(`${gateway}/sessions/${sessionId}`, { tags: { name: 'final-check' } }).json('lastSeq');
        if (lastSeq > last) missing.add(lastSeq - last);
        if (last > 0) listenersWithData.add(1);
      }
    };
  };
  connect();
}
