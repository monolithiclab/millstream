// A random request id, making an action idempotent (spec section 12.1): crypto.randomUUID where the browser offers it,
// which is in a secure context only (HTTPS or loopback), else a version 4 UUID from getRandomValues, which every
// context has.
export function requestId(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const b = crypto.getRandomValues(new Uint8Array(16));
  b.set([((b[6] ?? 0) & 0x0f) | 0x40], 6);
  b.set([((b[8] ?? 0) & 0x3f) | 0x80], 8);
  const h = Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
