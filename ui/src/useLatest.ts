import { useEffect, useRef } from 'react';

// The latest value in a ref, for listeners registered once that must call the current callback.
export function useLatest<T>(value: T) {
  const ref = useRef(value);
  useEffect(() => {
    ref.current = value;
  });
  return ref;
}
