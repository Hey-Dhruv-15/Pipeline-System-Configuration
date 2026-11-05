import { useEffect, useMemo, useRef, useState } from "react";

const REFRESH_MS = 5000;

function formatValue(value) {
  if (value === null || value === undefined || value === "") return "N/A";
  if (typeof value === "number") return new Intl.NumberFormat().format(value);
  const parsed = Date.parse(value);
  if (!Number.isNaN(parsed) && String(value).includes("T")) return new Date(parsed).toLocaleString();
  return String(value);
}

function statusTone(status) {
  if (status === "running") return "is-green";
  if (status === "processing") return "is-amber";
  if (status === "error") return "is-red";
  return "is-blue";
}

function SectionTitle({ kicker, title, copy }) {
  return (
    <div className="section-title">
      <div className="panel-kicker">{kicker}</div>
      <h2>{title}</h2>
      {copy ? <p>{copy}</p> : null}
    </div>
  );
}

function DataTable({ rows, maxColumns = 5, maxRows = 6 }) {
  if (!rows?.length) return <div className="empty-note">No records available.</div>;
  const columns = Object.keys(rows[0]).slice(0, maxColumns);
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr>
        </thead>
        <tbody>
          {rows.slice(0, maxRows).map((row, index) => (
            <tr key={index}>
              {columns.map((column) => <td key={column}>{formatValue(row[column])}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Modal({ open, title, onClose, children }) {
  if (!open) return null;
  return (
    <div className="modal-backdrop" onClick={onClose} role="presentation">
      <div className="modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="modal-header">
          <div className="modal-title">{title}</div>
          <button className="modal-close" onClick={onClose} type="button">Close</button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}

function App() {
  const [snapshot, setSnapshot] = useState(null);
  const [error, setError] = useState("");

  const [pulseStage, setPulseStage] = useState("");
  const lastRunIdRef = useRef(null);
  const traceRetryTimerRef = useRef(null);

  const [selectedFile, setSelectedFile] = useState("");
  const [trace, setTrace] = useState(null);
  const [traceError, setTraceError] = useState("");
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceTab, setTraceTab] = useState("silver");

  useEffect(() => {
    let active = true;
    async function load() {
      try {
        const response = await fetch("/api/snapshot");
        if (!response.ok) throw new Error(`Snapshot request failed with ${response.status}`);
        const payload = await response.json();
        if (active) {
          setSnapshot(payload);
          setError("");
        }
      } catch (err) {
        if (active) setError(err.message);
      }
    }
    load();
    const timer = setInterval(load, REFRESH_MS);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, []);

  useEffect(() => {
    const runId = snapshot?.overview?.run_id;
    if (!runId) return;

    if (lastRunIdRef.current && lastRunIdRef.current !== runId) {
      const stages = (snapshot?.pipeline_flow || []).map((stage) => stage.stage);
      if (stages.length) {
        let index = 0;
        const timers = [];
        const tick = () => {
          setPulseStage(stages[index]);
          index += 1;
          if (index < stages.length) {
            timers.push(setTimeout(tick, 420));
          } else {
            timers.push(setTimeout(() => setPulseStage(""), 650));
          }
        };
        timers.push(setTimeout(tick, 150));
        return () => timers.forEach(clearTimeout);
      }
    }

    lastRunIdRef.current = runId;
  }, [snapshot?.overview?.run_id]);

  const processedFilesPreview = useMemo(() => {
    const processed = snapshot?.datasets?.find((item) => item.name === "processed_files");
    return processed?.preview || [];
  }, [snapshot]);

  async function openFileTrace(fileName, { silent = false } = {}) {
    if (!silent) {
      setSelectedFile(fileName);
      setTrace(null);
      setTraceTab("silver");
    }

    setTraceError("");
    setTraceLoading(true);

    if (traceRetryTimerRef.current) {
      clearTimeout(traceRetryTimerRef.current);
      traceRetryTimerRef.current = null;
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);

    try {
      const response = await fetch(`/api/file-trace?file_name=${encodeURIComponent(fileName)}&limit=250`, {
        signal: controller.signal,
      });

      let payload = null;
      try {
        payload = await response.json();
      } catch (_) {
        payload = null;
      }

      if (!response.ok) {
        const message = payload?.detail || `Trace request failed with ${response.status}`;

        if (String(message).toLowerCase().includes("not been processed")) {
          setTraceError("Not processed yet — waiting for Airflow/Spark to heal this file. Retrying in ~5s.");
          traceRetryTimerRef.current = setTimeout(() => {
            if (selectedFile === fileName) openFileTrace(fileName, { silent: true });
          }, 5000);
          return;
        }

        throw new Error(message);
      }

      setTrace(payload);
    } catch (err) {
      const msg = err?.name === "AbortError" ? "Trace request timed out — try again in a few seconds." : err.message;
      setTraceError(msg);
    } finally {
      clearTimeout(timeout);
      setTraceLoading(false);
    }
  }

  function closeFileTrace() {
    if (traceRetryTimerRef.current) {
      clearTimeout(traceRetryTimerRef.current);
      traceRetryTimerRef.current = null;
    }
    setSelectedFile("");
    setTrace(null);
    setTraceError("");
    setTraceLoading(false);
  }

  if (error) return <div className="app-shell"><div className="error-card">Dashboard failed to load: {error}</div></div>;
  if (!snapshot) return <div className="app-shell"><div className="loading-card">Loading live healthcare pipeline?</div></div>;

  return (
    <div className="app-shell">
      <header className="hero">
        <div>
          <div className="eyebrow">Event-Driven Self-Healing Healthcare Pipeline</div>
          <h1>Lakehouse Monitoring Dashboard</h1>
          <p>
            Real-time view of backend file generation, Airflow triggers, Spark processing, self-healing
            quality operations, and Bronze to Silver to Gold movement.
          </p>
        </div>
        <div className="hero-meta">
          <span>Refresh every {REFRESH_MS / 1000}s</span>
          <span>Mode: {snapshot.mode}</span>
          <span>Data root: {snapshot.data_root}</span>
        </div>
      </header>

      <section className="overview-grid">
        {[
          ["Pipeline Health", snapshot.health_gauge],
          ["Run Id", snapshot.overview.run_id],
          ["Status", snapshot.overview.status],
          ["Processed Files", snapshot.overview.processed_files],
          ["Pending Files", snapshot.overview.pending_files],
        ].map(([label, value]) => (
          <article className="metric-card" key={label}>
            <div className="metric-label">{label}</div>
            <div className="metric-value">{formatValue(value)}</div>
          </article>
        ))}
      </section>

      <section className="split-grid">
        <article className="panel">
          <SectionTitle kicker="Raw Zone" title="Data Source Monitor" copy="Incoming files generated by the backend simulator." />
          <div className="file-stream">
            {snapshot.raw_monitor.incoming_files.map((file) => (
              <button
                className="file-pill is-clickable"
                key={file}
                type="button"
                onClick={() => openFileTrace(file)}
                title="Click to view this file through the pipeline"
              >
                {file}
              </button>
            ))}
          </div>
          <div className="mini-stats">
            <div><span>New files</span><strong>{formatValue(snapshot.raw_monitor.new_files_detected)}</strong></div>
            <div><span>Pending files</span><strong>{formatValue(snapshot.raw_monitor.pending_files)}</strong></div>
          </div>
          <div className="distribution">
            {Object.entries(snapshot.raw_monitor.file_types).map(([key, value]) => (
              <div key={key}><span>{key}</span><strong>{formatValue(value)}</strong></div>
            ))}
          </div>
        </article>

        <article className="panel">
          <SectionTitle kicker="Airflow" title="Pipeline Trigger" copy="Event-driven monitoring state from the actual DAG configuration." />
          <div className="info-list">
            <div><span>DAG</span><strong>{snapshot.airflow.dag}</strong></div>
            <div><span>Schedule</span><strong>{snapshot.airflow.schedule}</strong></div>
            <div><span>Trigger</span><strong>{snapshot.airflow.trigger_condition}</strong></div>
            <div><span>Last DAG run</span><strong>{snapshot.airflow.last_run}</strong></div>
            <div><span>Reason</span><strong>{snapshot.airflow.trigger_reason}</strong></div>
            <div><span>Next check</span><strong>{snapshot.airflow.next_check}</strong></div>
          </div>
        </article>
      </section>

      <section className="panel">
        <SectionTitle kicker="Architecture" title="Pipeline Flow" copy="Live status across backend, orchestration, processing, and medallion layers." />
        <div className="flow-grid">
          {snapshot.pipeline_flow.map((stage) => (
            <div className={`flow-node ${pulseStage === stage.stage ? "is-pulsing" : ""}`} key={stage.stage}>
              <div className={`status-dot ${statusTone(stage.status)}`}></div>
              <div className="flow-label">{stage.stage}</div>
              <div className="flow-status">{stage.status}</div>
            </div>
          ))}
        </div>
      </section>

      <section className="tri-grid">
        <article className="panel">
          <SectionTitle kicker="Bronze" title="Raw Data Storage" copy="Raw file ingestion from the current run history." />
          <div className="mini-stats">
            <div><span>Files ingested</span><strong>{formatValue(snapshot.bronze.files_ingested)}</strong></div>
            <div><span>Raw records stored</span><strong>{formatValue(snapshot.bronze.raw_records_stored)}</strong></div>
          </div>
          <DataTable rows={snapshot.bronze.table} />
        </article>

        <article className="panel">
          <SectionTitle kicker="Self-Healing" title="Data Quality Engine" copy="Operations executed by the Spark quality pipeline." />
          <div className="healing-grid">
            {snapshot.self_healing.map((item) => (
              <div className="healing-card" key={item.key}>
                <span>{item.label}</span>
                <strong>{formatValue(item.count)}</strong>
              </div>
            ))}
          </div>
        </article>

        <article className="panel">
          <SectionTitle kicker="Silver" title="Clean Patient Records" copy="Validated outputs from the current pipeline state." />
          <div className="mini-stats">
            <div><span>Clean records</span><strong>{formatValue(snapshot.silver.clean_records)}</strong></div>
            <div><span>Acceptance rate</span><strong>{formatValue(snapshot.silver.acceptance_rate)}%</strong></div>
            <div><span>Reject rate</span><strong>{formatValue(snapshot.silver.reject_rate)}%</strong></div>
          </div>
        </article>
      </section>

      <section className="split-grid">
        <article className="panel danger-panel">
          <SectionTitle kicker="Rejects" title="Reject Quarantine" copy="Records isolated by validation and self-healing rules." />
          <div className="danger-badge">Data Quality Protection Active</div>
          <div className="mini-stats">
            <div><span>Rejected rows</span><strong>{formatValue(snapshot.reject_quarantine.count)}</strong></div>
          </div>
          <DataTable rows={snapshot.reject_quarantine.preview} />
        </article>

        <article className="panel">
          <SectionTitle kicker="Gold" title="Healthcare Insights" copy="Aggregated analytics and patient segmentation outputs." />
          <div className="gold-block">
            <h3>Age Distribution</h3>
            <DataTable rows={snapshot.gold.age_distribution} />
          </div>
          <div className="gold-block">
            <h3>Patient Segments</h3>
            <DataTable rows={snapshot.gold.segments} />
          </div>
        </article>
      </section>

      <section className="split-grid">
        <article className="panel">
          <SectionTitle kicker="Metadata" title="Pipeline Monitoring" copy="Status, parser activity, processed files, and run metadata." />
          <div className="distribution">
            {Object.entries(snapshot.metadata_monitoring.parser_activity).map(([key, value]) => (
              <div key={key}><span>{key}</span><strong>{formatValue(value)}</strong></div>
            ))}
          </div>

          <h3>Recent Processed Files</h3>
          {processedFilesPreview.length ? (
            <div className="processed-list">
              {processedFilesPreview.slice(0, 10).map((row, index) => (
                <button
                  className="processed-row"
                  type="button"
                  key={`${row.file_name || "file"}-${index}`}
                  onClick={() => openFileTrace(row.file_name)}
                >
                  <span className="processed-file">{row.file_name}</span>
                  <span className="processed-meta">{formatValue(row.processed_at || row.file_modified_at || "")}</span>
                </button>
              ))}
            </div>
          ) : (
            <div className="empty-note">No processed files yet.</div>
          )}

          <h3>Run Status</h3>
          <DataTable rows={snapshot.metadata_monitoring.run_status} />
        </article>

        <article className="panel">
          <SectionTitle kicker="Alerts" title="Pipeline Alerts" copy="Severity-driven operational events from the live pipeline." />
          <div className="alerts-list">
            {snapshot.alerts.map((alert, index) => (
              <div className="alert-item" key={index}>
                <div className="alert-topline">
                  <span>{formatValue(alert.severity)}</span>
                  <span>{formatValue(alert.alert_type)}</span>
                  <span>{formatValue(alert.recorded_at)}</span>
                </div>
                <div className="alert-message">{formatValue(alert.message)}</div>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="split-grid">
        <article className="panel summary-panel">
          <SectionTitle kicker="Business View" title="Healthcare Data Pipeline Summary" />
          <p className="summary-copy">{snapshot.business_summary}</p>
        </article>

        <article className="panel">
          <SectionTitle kicker="Real-Time Feed" title="Activity Log" copy="Recent backend and pipeline actions." />
          <div className="activity-list">
            {snapshot.activity_feed.map((event, index) => (
              <div className="activity-row" key={index}>
                <span>{formatValue(event.time)}</span>
                <strong>{event.source}</strong>
                <span>{event.message}</span>
              </div>
            ))}
          </div>
        </article>
      </section>

      <Modal
        open={Boolean(selectedFile)}
        title={selectedFile ? `File View: ${selectedFile}` : "File View"}
        onClose={closeFileTrace}
      >
        {traceLoading ? (
          <div className="empty-note">Loading file trace…</div>
        ) : traceError ? (
          <div className="error-card">{traceError}</div>
        ) : trace ? (
          <div>
            <div className="tab-bar">
              {[
                ["raw", "Raw"],
                ["bronze", "Bronze"],
                ["silver", "Silver"],
                ["rejects", "Rejects"],
              ].map(([key, label]) => (
                <button
                  key={key}
                  type="button"
                  className={`tab-button ${traceTab === key ? "is-active" : ""}`}
                  onClick={() => setTraceTab(key)}
                >
                  {label}
                </button>
              ))}
            </div>

            {traceTab === "raw" ? (
              trace.raw_text ? <pre className="raw-pre">{trace.raw_text}</pre> : <div className="empty-note">Raw content not available.</div>
            ) : null}

            {traceTab === "bronze" ? (
              <DataTable rows={trace.bronze_records} maxColumns={8} maxRows={10} />
            ) : null}

            {traceTab === "silver" ? (
              <DataTable rows={trace.silver_records} maxColumns={8} maxRows={10} />
            ) : null}

            {traceTab === "rejects" ? (
              <DataTable rows={trace.reject_records} maxColumns={8} maxRows={10} />
            ) : null}
          </div>
        ) : (
          <div className="empty-note">Select a file to view details.</div>
        )}
      </Modal>
    </div>
  );
}

export default App;
