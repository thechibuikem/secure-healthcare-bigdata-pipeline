import React, { useState, useEffect } from "react";
import { fetchView, requestColumn, fetchAuditLog, fetchMart } from "./api.ts";

const TABLE = "patients";
const BLOCKED_TEST_COLUMN = "SSN";

type Role = "clinician" | "analyst";
type TabType = "access" | "reports";

interface RowData {
  [key: string]: unknown;
}

interface ViewState {
  columns: string[];
  rows: RowData[];
}

interface BlockedResultState {
  allowed: boolean;
  rows: unknown[];
}

interface AuditEntry {
  allowed: boolean;
  role: string;
  table: string;
  columns: string[];
  timestamp: string;
}

interface DataTableProps {
  columns: string[];
  rows: RowData[];
  loading?: boolean;
}

const DataTable: React.FC<DataTableProps> = ({ columns, rows, loading }) => {
  if (loading) {
    return <p className="no-data">Fetching data...</p>;
  }

  if (!rows.length) {
    return <p className="no-data">No data.</p>;
  }

  return (
    <div className="data-table-container">
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, rowIndex) => (
            <tr key={rowIndex}>
              {columns.map((col) => (
                <td key={col}>{String(row[col] ?? "")}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const AccessDemo: React.FC = () => {
  const [role, setRole] = useState<Role>("clinician");
  const [view, setView] = useState<ViewState>({ columns: [], rows: [] });
  const [loading, setLoading] = useState<boolean>(false);
  const [blockedResult, setBlockedResult] = useState<BlockedResultState | null>(
    null,
  );
  const [log, setLog] = useState<AuditEntry[]>([]);

  const refreshLog = () => {
    fetchAuditLog().then((data) => setLog(data.entries));
  };

  useEffect(() => {
    setLoading(true);
    setView({ columns: [], rows: [] });

    fetchView(role, TABLE)
      .then((data) => {
        setView(data);
        setLoading(false);
      })
      .catch(() => setLoading(false));

    refreshLog();
  }, [role]);

  const tryBlockedColumn = async () => {
    const result = await requestColumn(role, TABLE, BLOCKED_TEST_COLUMN);
    setBlockedResult(result);
    refreshLog();
  };

  return (
    <div className="section">
      <div className="control-group">
        <label className="control-label">
          Role:
          <select
            value={role}
            onChange={(e) => setRole(e.target.value as Role)}
            className="select-input"
          >
            <option value="clinician">clinician</option>
            <option value="analyst">analyst</option>
          </select>
        </label>
      </div>

      <section>
        <h3 className="section-title">
          Current view of <span className="mono-text">"{TABLE}"</span>
        </h3>
        <DataTable columns={view.columns} rows={view.rows} loading={loading} />
      </section>

      <section>
        <h3 className="section-title">
          Restricted Column Test ({BLOCKED_TEST_COLUMN})
        </h3>
        <button onClick={tryBlockedColumn} className="btn">
          Request {BLOCKED_TEST_COLUMN}
        </button>
        {blockedResult && (
          <div className="result-box">
            <span
              className={
                blockedResult.allowed ? "status-allowed" : "status-denied"
              }
            >
              {blockedResult.allowed ? "ALLOWED" : "DENIED"}
            </span>
            <span>
              {blockedResult.allowed
                ? JSON.stringify(blockedResult.rows)
                : "Blocked - this role cannot view this column."}
            </span>
          </div>
        )}
      </section>

      <section>
        <h3 className="section-title">Audit Log Ledger</h3>
        <div className="audit-ledger">
          {log.length === 0 ? (
            <p className="no-data" style={{ padding: "8px 14px" }}>
              No audit records found.
            </p>
          ) : (
            log.map((entry, i) => (
              <div key={i} className="audit-entry">
                <div>
                  <span
                    className={
                      entry.allowed ? "status-allowed" : "status-denied"
                    }
                  >
                    {entry.allowed ? "ALLOWED" : "DENIED"}
                  </span>
                  <span>
                    {entry.role} → {entry.table} (
                    {entry.columns.join(", ") || "none"})
                  </span>
                </div>
                <span className="audit-timestamp">{entry.timestamp}</span>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
};

const ReportsTab: React.FC = () => {
  const [report, setReport] = useState<string>("condition_prevalence");
  const [data, setData] = useState<ViewState>({ columns: [], rows: [] });
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    setLoading(true);
    setData({ columns: [], rows: [] });

    fetchMart(report)
      .then((res) => {
        setData(res);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [report]);

  return (
    <div className="section-group">
      <div className="control-group">
        <label className="control-label">
          Report:
          <select
            value={report}
            onChange={(e) => setReport(e.target.value)}
            className="select-input"
          >
            <option value="condition_prevalence">condition_prevalence</option>
            <option value="encounter_volume">encounter_volume</option>
            <option value="medication_trend">medication_trend</option>
          </select>
        </label>
      </div>
      <DataTable columns={data.columns} rows={data.rows} loading={loading} />
    </div>
  );
};

export default function App() {
  const [tab, setTab] = useState<TabType>("access");

  return (
    <div className="app-container">
      <header className="app-header">
        <h1 className="app-title">Secure Healthcare Pipeline — Demo</h1>
        <div className="nav-tabs">
          <button
            onClick={() => setTab("access")}
            className={`tab-btn ${tab === "access" ? "active" : "inactive"}`}
          >
            Access demo
          </button>
          <button
            onClick={() => setTab("reports")}
            className={`tab-btn ${tab === "reports" ? "active" : "inactive"}`}
          >
            Reports
          </button>
        </div>
      </header>
      <main>{tab === "access" ? <AccessDemo /> : <ReportsTab />}</main>
    </div>
  );
}
