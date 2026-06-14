import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { api, BytesPoint, MessageHourPoint } from "../api/client";

interface HourRow {
  hour: string;
  inbound: number;
  outbound: number;
}

// Pivot the per-direction rows into a per-hour shape for the chart.
function pivot(rows: MessageHourPoint[]): HourRow[] {
  const byHour = new Map<string, HourRow>();
  for (const r of rows) {
    const label = new Date(r.hour).toLocaleTimeString([], { hour: "2-digit" });
    const cur = byHour.get(label) ?? { hour: label, inbound: 0, outbound: 0 };
    if (r.direction === "inbound") cur.inbound += r.count;
    else cur.outbound += r.count;
    byHour.set(label, cur);
  }
  return Array.from(byHour.values());
}

export default function Analytics() {
  const [hours, setHours] = useState<HourRow[]>([]);
  const [bytes, setBytes] = useState<BytesPoint[]>([]);

  const load = async () => {
    const [m, b] = await Promise.all([api.messagesPerHour(), api.bytesPerMessage()]);
    setHours(pivot(m.data));
    setBytes(
      b.data.map((d, i) => ({ ...d, idx: i } as BytesPoint & { idx: number }))
    );
  };

  useEffect(() => {
    load();
    const t = setInterval(load, 10000);
    return () => clearInterval(t);
  }, []);

  const totalMsgs = hours.reduce((a, h) => a + h.inbound + h.outbound, 0);
  const totalBytes = bytes.reduce((a, b) => a + b.bytes, 0);

  return (
    <div>
      <h2>Analytics (today)</h2>
      <div className="grid" style={{ marginBottom: 24 }}>
        <div className="stat">
          <div className="label">Messages today</div>
          <div className="value">{totalMsgs}</div>
        </div>
        <div className="stat">
          <div className="label">Bytes today</div>
          <div className="value">{(totalBytes / 1024).toFixed(1)} KB</div>
        </div>
        <div className="stat">
          <div className="label">Avg bytes/msg</div>
          <div className="value">
            {bytes.length ? Math.round(totalBytes / bytes.length) : 0}
          </div>
        </div>
      </div>

      <div className="card">
        <h3>Messages per hour</h3>
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={hours}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a3441" />
            <XAxis dataKey="hour" stroke="#8b98a5" />
            <YAxis stroke="#8b98a5" allowDecimals={false} />
            <Tooltip contentStyle={{ background: "#1a212b", border: "1px solid #2a3441" }} />
            <Bar dataKey="inbound" fill="#25d366" name="Inbound" />
            <Bar dataKey="outbound" fill="#4493f8" name="Outbound" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="card">
        <h3>Bytes per message</h3>
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={bytes}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2a3441" />
            <XAxis dataKey="id" stroke="#8b98a5" />
            <YAxis stroke="#8b98a5" />
            <Tooltip contentStyle={{ background: "#1a212b", border: "1px solid #2a3441" }} />
            <Line type="monotone" dataKey="bytes" stroke="#25d366" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
