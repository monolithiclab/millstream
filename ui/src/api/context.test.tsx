import { renderHook } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { useApi, useEventSourceFactory } from './context';

describe('ApiProvider', () => {
  it('refuses a read outside the provider', () => {
    // React reports the error it rethrows, to the console and as a window error event: both are expected here.
    vi.spyOn(console, 'error').mockImplementation(() => undefined);
    const quiet = (event: ErrorEvent) => event.preventDefault();
    window.addEventListener('error', quiet);
    try {
      expect(() => renderHook(() => useApi())).toThrow('useApi outside ApiProvider');
      expect(() => renderHook(() => useEventSourceFactory())).toThrow('useApi outside ApiProvider');
    } finally {
      window.removeEventListener('error', quiet);
    }
  });
});
