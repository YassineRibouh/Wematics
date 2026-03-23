import { useCallback, useEffect, useRef, useState } from "react";

export function useAsync(asyncFn, deps = [], immediate = true) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(immediate);
  const [error, setError] = useState(null);
  const isMountedRef = useRef(true);
  const runIdRef = useRef(0);

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  const run = useCallback(async () => {
    const runId = ++runIdRef.current;
    if (isMountedRef.current) {
      setLoading(true);
      setError(null);
    }
    try {
      const result = await asyncFn();
      if (isMountedRef.current && runId === runIdRef.current) {
        setData(result);
      }
      return result;
    } catch (err) {
      if (isMountedRef.current && runId === runIdRef.current) {
        setError(err);
      }
      throw err;
    } finally {
      if (isMountedRef.current && runId === runIdRef.current) {
        setLoading(false);
      }
    }
  }, deps);

  useEffect(() => {
    if (immediate) {
      run().catch(() => {});
    }
  }, [run, immediate]);

  return { data, loading, error, run, setData };
}
