import { afterEach, describe, expect, it, vi } from 'vitest';
import { readStored, writeStored } from './storage';

describe('storage', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('reads and writes both storages', () => {
    writeStored('k', 'v');
    writeStored('k', 's', 'sessionStorage');
    expect(readStored('k')).toBe('v');
    expect(readStored('k', 'sessionStorage')).toBe('s');
  });

  it('survives storage that throws', () => {
    const throwing = {
      getItem: () => {
        throw new Error('blocked');
      },
      setItem: () => {
        throw new Error('blocked');
      },
    };
    vi.stubGlobal('localStorage', throwing);
    expect(() => writeStored('k', 'v')).not.toThrow();
    expect(readStored('k')).toBeNull();
  });
});
