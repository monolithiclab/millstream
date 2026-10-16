import { describe, expect, it, vi } from 'vitest';
import { requestId } from './requestId';

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe('requestId', () => {
  it('uses randomUUID where the browser offers it', () => {
    vi.spyOn(crypto, 'randomUUID').mockReturnValue('11111111-2222-4333-8444-555555555555');
    expect(requestId()).toBe('11111111-2222-4333-8444-555555555555');
  });

  it('falls back to getRandomValues outside a secure context', () => {
    vi.stubGlobal('crypto', { getRandomValues: (b: Uint8Array) => b.fill(0xff) });
    expect(requestId()).toBe('ffffffff-ffff-4fff-bfff-ffffffffffff');
    expect(requestId()).toMatch(uuid);
  });
});
