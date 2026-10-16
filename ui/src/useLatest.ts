import { useLayoutEffect, useRef } from 'react';

// The latest value in a ref, for listeners registered once that must call the current callback. It is updated in
// a layout effect, during the commit, so a native event arriving before passive effects run never sees a stale one.
export function useLatest<T>(value: T) {
  const ref = useRef(value);
  useLayoutEffect(() => {
    ref.current = value;
  });
  return ref;
}
