'use strict';

const INCIDENT_RUNBOOKS = [
  {
    id: 'source_map_breakage',
    path: 'docs/runbooks/source-map-breakage.md#source-map-breakage',
    patterns: [
      /\bsource[_\s-]?map[_\s-]?breakage\b/i,
      /\bsource[_\s-]?map\b.*\b(lint|schema|break|fail|error|invalid)\b/i,
      /\bSCHEMA_[A-Z0-9_]+\b/i,
      /\bURL_[A-Z0-9_]+\b/i,
      /\bDUPLICATE_[A-Z0-9_]+\b/i,
    ],
  },
  {
    id: 'revised_file_mismatch',
    path: 'docs/runbooks/revised-file-mismatch.md#revised-file-mismatch',
    patterns: [
      /\brevised[_\s-]?file[_\s-]?mismatch\b/i,
      /\bsupersession\b/i,
      /\bplan[_\s-]?period\b.*\bmismatch\b/i,
    ],
  },
  {
    id: 'parser_fallback_exhaustion',
    path: 'docs/runbooks/parser-fallback-exhaustion.md#parser-fallback-exhaustion',
    patterns: [
      /\bparser[_\s-]?fallback[_\s-]?exhaustion\b/i,
      /\bfallback\b.*\bexhaust/i,
      /\bmissing\b.*\brequired fields?\b/i,
    ],
  },
  {
    id: 'parser_output_validation_failure',
    path: 'docs/runbooks/parser-output-validation-failure.md#parser-output-validation-failure',
    patterns: [
      /\bparser[_\s-]?output[_\s-]?validation[_\s-]?failure\b/i,
      /\b(schema_invalid|numeric_out_of_range|provenance_(?:missing|invalid))\b/i,
      /\bvalidation\b.*\bblocked\b.*\bpromotion\b/i,
    ],
  },
  {
    id: 'parser_low_confidence_output',
    path: 'docs/runbooks/parser-low-confidence-output.md#parser-low-confidence-output',
    patterns: [
      /\bparser[_\s-]?low[_\s-]?confidence[_\s-]?output\b/i,
      /\blow[_\s-]?confidence\b.*\bhigh[_\s-]?priority[_\s-]?review\b/i,
      /\bparser\b.*\bconfidence\b.*\bqueue\b/i,
    ],
  },
  {
    id: 'anomaly_flood',
    path: 'docs/runbooks/anomaly-flood.md#anomaly-flood',
    patterns: [
      /\banomaly[_\s-]?flood\b/i,
      /\banomaly\b.*\b(spike|surge|flood)\b/i,
      /\bqueue depth\b/i,
    ],
  },
];

function toSearchText(parts) {
  if (Array.isArray(parts)) {
    return parts.filter(Boolean).join(' ');
  }
  return String(parts || '');
}

function detectIncidentRunbooks(parts) {
  const text = toSearchText(parts);
  return INCIDENT_RUNBOOKS.filter((entry) => entry.patterns.some((pattern) => pattern.test(text)));
}

function buildIncidentRunbookSection(parts) {
  const matches = detectIncidentRunbooks(parts);
  if (matches.length === 0) {
    return [];
  }

  const lines = ['', '### Incident Runbooks'];
  for (const match of matches) {
    lines.push(`- \`${match.id}\` -> \`${match.path}\``);
  }
  return lines;
}

module.exports = {
  INCIDENT_RUNBOOKS,
  detectIncidentRunbooks,
  buildIncidentRunbookSection,
};
