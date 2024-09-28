import { useEffect, useState } from "react";

// Custom hook to detect changes in ref value
export function useRefValueListener(
  ref: React.RefObject<any>,
  callback: (value: any) => void
) {
  const [currentValue, setCurrentValue] = useState(ref.current);

  useEffect(() => {
    if (currentValue !== ref.current) {
      callback(ref.current);
      setCurrentValue(ref.current);
    }
  }, [ref, currentValue, callback]);
}