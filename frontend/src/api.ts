const API_BASE = "http://localhost:8000";

interface ViewResponse {
  allowed: boolean;
  columns: string[];
  rows: Record<string, unknown>[];
}

interface AuditEntry {
  role: string;
  table: string;
  columns: string[];
  allowed: boolean;
  timestamp: string;
}

export async function fetchView(
  role: string,
  table: string,
): Promise<ViewResponse> {
  const res = await fetch(`${API_BASE}/view?role=${role}&table=${table}`);
  return res.json();
}

export async function requestColumn(
  role: string,
  table: string,
  column: string,
): Promise<{ allowed: boolean; rows: Record<string, unknown>[] }> {
  const res = await fetch(
    `${API_BASE}/request-column?role=${role}&table=${table}&column=${column}`,
  );
  return res.json();
}

export async function fetchAuditLog(): Promise<{ entries: AuditEntry[] }> {
  const res = await fetch(`${API_BASE}/audit-log`);
  return res.json();
}

export async function fetchMart(
  reportName: string,
): Promise<{ columns: string[]; rows: Record<string, unknown>[] }> {
  const res = await fetch(`${API_BASE}/marts/${reportName}`);
  return res.json();
}
