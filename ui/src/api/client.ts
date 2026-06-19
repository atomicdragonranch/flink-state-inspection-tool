const BASE_URL = "";

interface ApiResponse<T> {
  data: T;
  partialRead?: boolean;
  partialReadCause?: string;
  error?: string;
  stackTrace?: string;
}

export class ApiError extends Error {
  public readonly stackTrace?: string;

  constructor(message: string, stackTrace?: string) {
    super(message);
    this.name = "ApiError";
    this.stackTrace = stackTrace;
  }
}

async function fetchApi<T>(
  url: string,
  options?: RequestInit
): Promise<ApiResponse<T>> {
  const response = await fetch(`${BASE_URL}${url}`, {
    headers: {
      "Content-Type": "application/json"
    },
    ...options
  });

  const json: ApiResponse<T> = await response.json();

  if (!response.ok || json.error) {
    throw new ApiError(
      json.error || `HTTP ${response.status}`,
      json.stackTrace
    );
  }

  return json;
}

// --- Types ---

export interface OperatorInfo {
  uid: string;
  name: string;
  keyedStates: string[];
  operatorStates: string[];
}

export interface OperatorDiscovery {
  operators: OperatorInfo[];
  localPath?: string;
}

export type SnapshotType = "checkpoint" | "savepoint";

export interface CheckpointInfo {
  jobId: string | null;
  shortJobId: string;
  path: string;
  chkNumber: number;
  modificationTime: number;
  formattedTime: string;
  type: SnapshotType;
  displayLabel: string;
  valid: boolean;
}

export interface InspectResult {
  operatorName: string;
  operatorUid: string;
  entryCount: number;
  keysOnly: boolean;
  columns?: string[];
  entries: Record<string, unknown>[];
}

export interface FieldChange {
  fieldName: string;
  oldValue: string | null;
  newValue: string | null;
}

export interface DiffEntryData {
  key: string;
  json1: Record<string, unknown> | null;
  json2: Record<string, unknown> | null;
  fieldChanges?: FieldChange[];
}

export interface DiffResultData {
  operatorName: string;
  label1: string;
  label2: string;
  added: DiffEntryData[];
  removed: DiffEntryData[];
  modified: DiffEntryData[];
  unchangedCount: number;
  totalKeys: number;
  partialRead?: {
    checkpoint1: boolean;
    checkpoint2: boolean;
  };
}

// --- Auto-detection ---

export interface DetectedSource {
  type: string;
  container: string;
  path: string;
  label: string;
}

export async function detectSources(): Promise<DetectedSource[]> {
  const res = await fetchApi<DetectedSource[]>("/api/sources/detect");
  return res.data;
}

// --- Cache ---

export interface CacheEntry {
  sourcePath: string;
  localPath: string;
  cachedAt: number;
  sizeBytes: number;
}

export async function listCache(): Promise<CacheEntry[]> {
  const res = await fetchApi<CacheEntry[]>("/api/cache/list");
  return res.data;
}

export async function deleteCache(localPath: string): Promise<void> {
  await fetchApi<boolean>("/api/cache/delete", {
    method: "POST",
    body: JSON.stringify({ localPath })
  });
}

// --- Documentation ---

export async function getDoc(name: string): Promise<string> {
  const res = await fetchApi<string>(`/api/docs/${encodeURIComponent(name)}`);
  return res.data;
}

// --- Operator discovery ---

export async function discoverOperators(
  path: string
): Promise<OperatorDiscovery> {
  const res = await fetchApi<OperatorDiscovery>("/api/operators/discover", {
    method: "POST",
    body: JSON.stringify({ path })
  });
  return res.data;
}

// --- Snapshot discovery ---

export async function discoverCheckpoints(
  path: string,
  perJob?: number,
  totalLimit?: number
): Promise<CheckpointInfo[]> {
  const res = await fetchApi<CheckpointInfo[]>("/api/checkpoints/discover", {
    method: "POST",
    body: JSON.stringify({ path, perJob, totalLimit })
  });
  return res.data;
}

export async function discoverSavepoints(
  path: string,
  totalLimit?: number
): Promise<CheckpointInfo[]> {
  const res = await fetchApi<CheckpointInfo[]>("/api/savepoints/discover", {
    method: "POST",
    body: JSON.stringify({ path, totalLimit })
  });
  return res.data;
}

// --- Inspection ---

export interface InspectRequest {
  path: string;
  operatorUid?: string;
  stateName?: string;
  keyFilter?: string;
  keysOnly?: boolean;
}

export async function inspectKeyed(
  req: InspectRequest
): Promise<ApiResponse<InspectResult>> {
  return fetchApi<InspectResult>("/api/inspect/keyed", {
    method: "POST",
    body: JSON.stringify(req)
  });
}

export async function inspectBroadcast(
  req: InspectRequest
): Promise<ApiResponse<InspectResult>> {
  return fetchApi<InspectResult>("/api/inspect/broadcast", {
    method: "POST",
    body: JSON.stringify(req)
  });
}

// --- Diff ---

export interface DiffRequest {
  path1: string;
  path2: string;
  operatorUid?: string;
  stateName?: string;
  keyFilter?: string;
}

export async function diffKeyed(
  req: DiffRequest
): Promise<ApiResponse<DiffResultData>> {
  return fetchApi<DiffResultData>("/api/diff/keyed", {
    method: "POST",
    body: JSON.stringify(req)
  });
}

export async function diffBroadcast(
  req: DiffRequest
): Promise<ApiResponse<DiffResultData>> {
  return fetchApi<DiffResultData>("/api/diff/broadcast", {
    method: "POST",
    body: JSON.stringify(req)
  });
}
