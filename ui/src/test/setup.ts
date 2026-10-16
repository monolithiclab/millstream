import '@testing-library/jest-dom/vitest';
import { cleanup } from '@testing-library/react';
import { afterEach, beforeEach, vi } from 'vitest';
import { FakeEventSource } from '../live/fakeEventSource';
import { resetLoaded } from '../screens/ScreenPage';

beforeEach(() => {
  window.localStorage.clear();
  window.sessionStorage.clear();
  document.documentElement.removeAttribute('data-theme');
  FakeEventSource.reset();
  resetLoaded();
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});
