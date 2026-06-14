// Typed API client for the management backend.

const BASE = import.meta.env.VITE_API_BASE ?? "";

export interface Connection {
  id: number;
  label: string;
  jid: string;
  status: string;
  pod_id: string;
  last_seen: string | null;
  bytes_today: number;
  bytes_in: number;
  bytes_out: number;
  live: boolean;
}

export interface ConnectionsResponse {
  gateway_up: boolean;
  connections: Connection[];
  count: number;
}

export interface MessageHourPoint {
  hour: string;
  direction: string;
  count: number;
}

export interface BytesPoint {
  id: number;
  jid: string;
  direction: string;
  bytes: number;
  created_at: string;
}

async function getJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${path} -> ${res.status}`);
  return res.json() as Promise<T>;
}

export const api = {
  connections: () => getJSON<ConnectionsResponse>("/api/connections"),
  messagesPerHour: () => getJSON<{ data: MessageHourPoint[] }>("/api/analytics/messages"),
  bytesPerMessage: () => getJSON<{ data: BytesPoint[] }>("/api/analytics/bytes"),

  linkPhone: async (label: string): Promise<{ id: number; label: string }> => {
    const res = await fetch(`${BASE}/api/connections`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ label }),
    });
    if (!res.ok) throw new Error(`link phone -> ${res.status}`);
    return res.json();
  },

  getQR: async (id: number): Promise<string | null> => {
    const res = await fetch(`${BASE}/api/connections/${id}/qr`);
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`get qr -> ${res.status}`);
    const data = await res.json();
    return data.qr ?? null;
  },

  disconnect: async (id: number) => {
    const res = await fetch(`${BASE}/api/connections/${id}/disconnect`, { method: "POST" });
    if (!res.ok) throw new Error(`disconnect ${id} -> ${res.status}`);
    return res.json();
  },
};
