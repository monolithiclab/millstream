/*
 * Resource shapes of the HTTP API (/api/v1, spec section 12.1). They are generated: `make ui-types` runs
 * openapi-typescript on internal/api/openapi.yaml into openapi.d.ts (committed; make test-ui fails when it is stale).
 * This file aliases its schemas, so the rest of the UI imports resource types from here, never from openapi.d.ts.
 * JSON keys are lowerCamelCase (D18); times are RFC 3339 strings; money is USD as a number.
 */
import type { components } from './openapi';

type Schemas = components['schemas'];

export type RunState = Schemas['RunState'];
export type DeliveryState = Schemas['DeliveryState'];
export type ReviewState = Schemas['ReviewState'];
export type SampleState = Schemas['SampleState'];
export type InsightState = Schemas['InsightState'];

export type Pipeline = Schemas['Pipeline'];
export type Step = Schemas['Step'];
export type Trigger = Schemas['Trigger'];
export type Import = Schemas['Import'];
export type Revision = Schemas['Revision'];
export type Graph = Schemas['Graph'];
export type Stream = Schemas['Stream'];

export type RunSummary = Schemas['RunSummary'];
export type Run = Schemas['Run'];
export type RunPage = Schemas['RunPage'];
export type RunReport = Schemas['RunReport'];
export type Figure = Schemas['Figure'];

export type ArtifactRef = Schemas['ArtifactRef'];
export type Item = Schemas['Item'];
export type Tombstone = Schemas['Tombstone'];
export type ItemPage = Schemas['ItemPage'];
export type Lineage = Schemas['Lineage'];
export type LineageNode = Schemas['LineageNode'];
export type Delivery = Schemas['Delivery'];
export type Attempt = Schemas['Attempt'];

export type DeadLetterGroup = Schemas['DeadLetterGroup'];
export type DeadLetter = Schemas['DeadLetter'];
export type DeadLetterSelection = Schemas['DeadLetterSelection'];
export type Count = Schemas['Count'];

export type TriggerRequest = Schemas['TriggerRequest'];
export type RollbackRequest = Schemas['RollbackRequest'];
export type ReplayRequest = Schemas['ReplayRequest'];

export type MetricSeries = Schemas['MetricSeries'];

export type Session = Schemas['Session'];

export type Review = Schemas['Review'];
export type ReviewPage = Schemas['ReviewPage'];
export type ReviewDecision = Schemas['ReviewDecision'];
export type Sample = Schemas['Sample'];
export type SamplePage = Schemas['SamplePage'];
export type SampleDecision = Schemas['SampleDecision'];

export type Insight = Schemas['Insight'];
export type StepLessons = Schemas['StepLessons'];
export type Nodes = Schemas['Nodes'];
export type Node = Schemas['Node'];
export type DeploymentEngine = Schemas['DeploymentEngine'];

// The data of the server-sent events.
export type Event = Schemas['Event'];
export type PipelineSnapshot = Schemas['PipelineSnapshot'];
export type LogLine = Schemas['LogLine'];
export type SnapshotRequired = Schemas['SnapshotRequired'];
export type Gap = Schemas['Gap'];
