/*
 * Resource shapes of the HTTP API (/api/v1, spec section 12.1), written by hand until internal/api/openapi.yaml
 * exists (M3 Task 2). This file is the seam: once the OpenAPI document lands, generate its types (for instance
 * openapi-typescript into src/api/openapi.d.ts) and turn each interface below into an alias of the generated schema,
 * `export type Pipeline = components['schemas']['Pipeline']`, so the rest of the UI keeps importing from here.
 * JSON keys are lowerCamelCase (D18); times are RFC 3339 strings; money is USD as a number.
 */

export type PipelineState = 'active' | 'paused' | 'disabled';
export type RunState = 'running' | 'succeeded' | 'completedWithErrors' | 'failed' | 'cancelled';
export type DeliveryState =
  'pending' | 'leased' | 'awaitingReview' | 'done' | 'filtered' | 'failedWillRetry' | 'dead' | 'cancelled';

export interface Page<T> {
  items: T[];
  total: number;
  next?: string;
}

export interface RunSummary {
  id: string;
  state: RunState;
  startedAt: string;
  endedAt?: string;
}

export interface Pipeline {
  name: string;
  description?: string;
  state: PipelineState;
  revision: string;
  stepCount: number;
  lastRun?: RunSummary;
  itemsToday: number;
  costTodayUsd: number;
  openDeadLetters: number;
}

export interface Revision {
  id: string;
  pipeline: string;
  hash: string;
  appliedAt: string;
  current: boolean;
}

export interface Run extends RunSummary {
  pipeline: string;
  revision: string;
  trigger: string;
  itemsIn: number;
  itemsOut: number;
  deadLetters: number;
  costUsd: number;
}

export interface StepReport {
  step: string;
  delivered: number;
  succeeded: number;
  failed: number;
  filtered: number;
  p95Ms: number;
  costUsd: number;
}

export interface RunReport {
  run: Run;
  steps: StepReport[];
}

export interface Stream {
  name: string;
  pipeline: string;
  producer: string;
  consumers: string[];
  itemCount: number;
}

export interface Item {
  id: string;
  pipeline: string;
  stream: string;
  runId: string;
  createdAt: string;
  payload: unknown;
}

export interface LineageNode {
  id: string;
  stream: string;
  step: string;
}

export interface Lineage {
  item: string;
  parents: LineageNode[];
  children: LineageNode[];
}

export interface Delivery {
  id: string;
  itemId: string;
  step: string;
  state: DeliveryState;
  attempt: number;
  startedAt?: string;
  endedAt?: string;
  error?: string;
  costUsd?: number;
}

export interface DeadLetterGroup {
  fingerprint: string;
  pipeline: string;
  step: string;
  error: string;
  count: number;
  firstSeen: string;
  lastSeen: string;
  itemIds: string[];
}

export interface MetricsQuery {
  metric: string;
  pipeline?: string;
  groupBy?: 'step' | 'action' | 'run' | 'label';
  from: string;
  to: string;
}

export interface MetricSeries {
  metric: string;
  group: string;
  points: { t: string; v: number }[];
}

export interface ReasonCode {
  code: string;
  label: string;
}

export interface InboxEntry {
  id: string;
  itemId: string;
  pipeline: string;
  step: string;
  inbox: string;
  title: string;
  createdAt: string;
  deadline?: string;
  sample: boolean;
  editable: string[];
  reasons: ReasonCode[];
}

export type ReviewDecision =
  { decision: 'approve'; patch?: unknown } | { decision: 'reject'; reason: string; note?: string };

export interface SampleDecision {
  agree: boolean;
  reason?: string;
}

export interface Insight {
  id: string;
  kind: string;
  pipeline: string;
  step?: string;
  title: string;
  state: 'open' | 'acknowledged' | 'muted';
  createdAt: string;
}

export interface Lesson {
  pipeline: string;
  step: string;
  rules: string[];
  examples: string[];
  cites: string[];
}

export interface NodeInfo {
  id: string;
  role: 'engine' | 'node';
  state: string;
  version: string;
  labels: Record<string, string>;
  slots: number;
  leases: number;
}
