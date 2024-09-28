import { useState, useEffect, useRef } from "react";
import { useReshape } from "./useReshape";
import { IGraphSize } from "../types/ProfileGraphDashboard";


export function useGraphSize() {
  const [graphSize, setGraphSize] = useState<IGraphSize>({
    width: 0,
    height: window.innerHeight / 2,
  });


  const profileRef = useRef<HTMLDivElement>(null);

  const { reshapeDOM } = useReshape();

  const handleResize = () => {
    if (profileRef?.current) {
      setGraphSize({
        width: profileRef.current.offsetWidth - 408,
        height: window.innerHeight,
      });
    }
  };

  useEffect(() => {
    handleResize();
    reshapeDOM(() => {
      handleResize();
    });
  }, []);

  return { graphSize, profileRef, handleResize };
}
