import { useEffect, useRef, useState } from "react";
import { QRCodeSVG } from "qrcode.react";
import { api, Connection } from "../api/client";

function bytesHuman(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

function QRModal({ connId, label, onClose }: { connId: number; label: string; onClose: () => void }) {
  const [qr, setQR] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    const poll = async () => {
      try {
        const code = await api.getQR(connId);
        if (code) {
          setQR(code);
        } else if (qr !== null) {
          // QR disappeared — phone connected successfully
          setConnected(true);
          if (pollRef.current) clearInterval(pollRef.current);
        }
      } catch {
        // gateway briefly unreachable; keep polling
      }
    };
    poll();
    pollRef.current = setInterval(poll, 3000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [connId]);

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        {connected ? (
          <>
            <h3>Phone connected!</h3>
            <p className="muted">The WhatsApp session for <strong>{label}</strong> is now active.</p>
            <button onClick={onClose}>Close</button>
          </>
        ) : (
          <>
            <h3>Link phone — {label}</h3>
            <p className="muted">Open WhatsApp → Settings → Linked Devices → Link a Device</p>
            {qr ? (
              <div className="qr-wrap">
                <QRCodeSVG value={qr} size={240} level="L" />
              </div>
            ) : (
              <p className="muted">Waiting for QR code…</p>
            )}
            <p className="muted" style={{ fontSize: "0.8rem" }}>QR rotates every ~20s — scan quickly</p>
            <button onClick={onClose}>Cancel</button>
          </>
        )}
      </div>
    </div>
  );
}

export default function Dashboard() {
  const [conns, setConns] = useState<Connection[]>([]);
  const [gatewayUp, setGatewayUp] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [linking, setLinking] = useState(false);
  const [newLabel, setNewLabel] = useState("");
  const [qrModal, setQRModal] = useState<{ id: number; label: string } | null>(null);

  const load = async () => {
    try {
      const res = await api.connections();
      setConns(res.connections);
      setGatewayUp(res.gateway_up);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  };

  useEffect(() => {
    load();
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, []);

  const onDisconnect = async (id: number) => {
    await api.disconnect(id);
    setTimeout(load, 800);
  };

  const onLinkPhone = async () => {
    const label = newLabel.trim();
    if (!label) return;
    setLinking(true);
    try {
      const { id } = await api.linkPhone(label);
      setNewLabel("");
      setQRModal({ id, label });
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setLinking(false);
    }
  };

  return (
    <div>
      <h2>Active Connections</h2>

      {!gatewayUp && (
        <div className="banner warn">Gateway unreachable — showing last known DB state.</div>
      )}
      {error && <div className="banner warn">{error}</div>}

      <div className="card" style={{ display: "flex", alignItems: "center", gap: "0.75rem", padding: "1rem" }}>
        <input
          type="text"
          placeholder="Phone label (e.g. support-line-1)"
          value={newLabel}
          onChange={(e) => setNewLabel(e.target.value)}
          onKeyDown={(e) => { if (e.key === "Enter") onLinkPhone(); }}
          style={{ flex: 1, padding: "0.5rem 0.75rem", borderRadius: 6, border: "1px solid #444", background: "#1e1e2e", color: "inherit" }}
        />
        <button onClick={onLinkPhone} disabled={linking || !newLabel.trim()}>
          {linking ? "Linking…" : "Link Phone"}
        </button>
      </div>

      <div className="card">
        <table>
          <thead>
            <tr>
              <th>Label</th>
              <th>Phone (JID)</th>
              <th>Status</th>
              <th>Pod</th>
              <th>Last seen</th>
              <th>Bytes today</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {conns.map((c) => (
              <tr key={c.id}>
                <td>{c.label}</td>
                <td>{c.jid || <span className="muted">—</span>}</td>
                <td>
                  <span className={`badge ${c.status}`}>{c.status}</span>
                </td>
                <td className="muted">{c.pod_id || "—"}</td>
                <td className="muted">
                  {c.last_seen ? new Date(c.last_seen).toLocaleTimeString() : "—"}
                </td>
                <td>{bytesHuman(c.bytes_today)}</td>
                <td style={{ display: "flex", gap: "0.5rem" }}>
                  {c.status === "qr" && (
                    <button onClick={() => setQRModal({ id: c.id, label: c.label })}>
                      Show QR
                    </button>
                  )}
                  {c.status !== "disconnected" && (
                    <button className="danger" onClick={() => onDisconnect(c.id)}>
                      Disconnect
                    </button>
                  )}
                </td>
              </tr>
            ))}
            {conns.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  No connections yet. Use "Link Phone" above to add one.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {qrModal && (
        <QRModal
          connId={qrModal.id}
          label={qrModal.label}
          onClose={() => { setQRModal(null); load(); }}
        />
      )}
    </div>
  );
}
