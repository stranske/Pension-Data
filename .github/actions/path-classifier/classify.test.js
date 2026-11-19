'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { parseGithubContext } = require('./classify');

const REPO = 'stranske/Pension-Data';
const HEAD_SHA = 'abc123';

function pullRequest(overrides = {}) {
  return {
    state: 'open',
    number: 1052,
    body: 'ordinary body',
    head: { sha: HEAD_SHA, ref: 'cursor/issue-1048', repo: { full_name: REPO } },
    base: { sha: 'def456', ref: 'main', repo: { full_name: REPO } },
    labels: [],
    ...overrides,
  };
}

function compactClassifierPr(overrides = {}) {
  const pr = pullRequest(overrides);
  const compact = {
    state: pr.state,
    number: pr.number,
    head: pr.head,
    base: pr.base,
    labels: pr.labels,
  };
  if (overrides.body !== undefined) compact.body = overrides.body;
  return compact;
}

function withContext({ eventPayload, gatePr }, fn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'classifier-'));
  const eventPath = path.join(dir, 'event.json');
  if (eventPayload !== null && eventPayload !== undefined) {
    fs.writeFileSync(eventPath, typeof eventPayload === 'string' ? eventPayload : JSON.stringify(eventPayload), 'utf8');
  }
  const saved = { ...process.env };
  try {
    delete process.env.GITHUB_CONTEXT_JSON;
    process.env.GITHUB_EVENT_PATH = eventPayload === null ? path.join(dir, 'missing.json') : eventPath;
    process.env.GITHUB_EVENT_NAME = 'pull_request';
    process.env.GITHUB_SHA = HEAD_SHA;
    process.env.GITHUB_REF = 'refs/pull/1052/merge';
    process.env.GITHUB_BASE_REF = 'main';
    process.env.GITHUB_REPOSITORY = REPO;
    if (gatePr === undefined) delete process.env.GATE_PR_JSON;
    else process.env.GATE_PR_JSON = typeof gatePr === 'string' ? gatePr : JSON.stringify(gatePr);
    return fn();
  } finally {
    process.env = saved;
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('oversized ordinary PR body classifies via compact resolved snapshot', () => {
  const bigBody = 'x'.repeat(200_000);
  withContext(
    { eventPayload: { pull_request: pullRequest({ body: bigBody }), number: 1052 }, gatePr: compactClassifierPr() },
    () => {
      const context = parseGithubContext();
      assert.equal(context.event.pull_request.number, 1052);
      assert.equal(context.event.pull_request.head.sha, HEAD_SHA);
    },
  );
});

test('stable-delivery body is preserved from compact transport', () => {
  const sealBody = 'sealed delivery record';
  const head = { sha: HEAD_SHA, ref: 'sync/workflows-delivery', repo: { full_name: REPO } };
  withContext(
    {
      eventPayload: { pull_request: pullRequest({ head, body: sealBody }), number: 1052 },
      gatePr: compactClassifierPr({ head, body: sealBody }),
    },
    () => {
      const context = parseGithubContext();
      assert.equal(context.event.pull_request.body, sealBody);
      assert.equal(context.event.pull_request.head.ref, 'sync/workflows-delivery');
    },
  );
});

test('malformed event payload is rejected', () => {
  withContext({ eventPayload: '{not json', gatePr: compactClassifierPr() }, () => {
    assert.throws(() => parseGithubContext());
  });
});

test('missing event payload is rejected', () => {
  withContext({ eventPayload: null, gatePr: compactClassifierPr() }, () => {
    assert.throws(() => parseGithubContext());
  });
});

test('malformed resolved PR data is rejected', () => {
  withContext(
    { eventPayload: { pull_request: pullRequest(), number: 1052 }, gatePr: '{broken' },
    () => assert.throws(() => parseGithubContext(), /malformed/),
  );
});

test('closed resolved PR data is rejected', () => {
  withContext(
    { eventPayload: { pull_request: pullRequest(), number: 1052 }, gatePr: compactClassifierPr({ state: 'closed' }) },
    () => assert.throws(() => parseGithubContext(), /incomplete/),
  );
});

test('PR number mismatch is rejected', () => {
  withContext(
    { eventPayload: { pull_request: pullRequest(), number: 1052 }, gatePr: compactClassifierPr({ number: 9999 }) },
    () => assert.throws(() => parseGithubContext(), /differs/),
  );
});

test('head SHA mismatch is rejected', () => {
  const head = { sha: 'other-sha', ref: 'cursor/issue-1048', repo: { full_name: REPO } };
  withContext(
    { eventPayload: { pull_request: pullRequest(), number: 1052 }, gatePr: compactClassifierPr({ head }) },
    () => assert.throws(() => parseGithubContext(), /differs/),
  );
});

test('repository mismatch is rejected', () => {
  const head = { sha: HEAD_SHA, ref: 'cursor/issue-1048', repo: { full_name: 'fork/Pension-Data' } };
  withContext(
    { eventPayload: { pull_request: pullRequest(), number: 1052 }, gatePr: compactClassifierPr({ head }) },
    () => assert.throws(() => parseGithubContext(), /differs/),
  );
});
