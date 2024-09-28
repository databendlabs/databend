import { Edge, FlowAnalysisGraph, FlowAnalysisGraphConfig, IGraph } from "@ant-design/charts";
import { debounce, isEqual } from "lodash-es";
import { memo, useEffect, useMemo, useState } from "react";

import { IGraphSize, IOverview, Profile } from "../types/ProfileGraphDashboard";

import { mapEdgesLineWidth, getEdges, getDealData } from "../utills";
import { useFlowAnalysisGraphConfig } from "../hooks/useFlowAnalysisGraphConfig";

export interface EdgeWithLineWidth extends Edge {
  lineWidth: number;
  _value: number;
}

const CacheFlowAnalysisGraph = ({
  plainData,
  graphSize,
  graphRef,
  overviewInfoCurrent,
  onReady,
}: {
  plainData: Profile[];
  graphSize: IGraphSize;
  graphRef: React.RefObject<IGraph>;
  overviewInfoCurrent: React.RefObject<IOverview | undefined>;
  onReady: (graph: IGraph) => void;
}) => {
  const [renderKey, setRenderKey] = useState(0);

  const handleResetView = () => {
    const graph = graphRef?.current;
    if (graph) {
      graph.fitView();
      graph.refresh();
    }
  };

  const edgesWithLineWidth = mapEdgesLineWidth(getEdges(plainData) as EdgeWithLineWidth[]);
  const data = useMemo(() => {
    return {
      nodes: getDealData(plainData),
      edges: edgesWithLineWidth,
    };
  }, [plainData, edgesWithLineWidth]);

  const config: FlowAnalysisGraphConfig = useFlowAnalysisGraphConfig({
    graphSize,
    onReady,
    data,
    graphRef,
    overviewInfoCurrent,
    handleResetView,
    edgesWithLineWidth,
  });

  // Debounce the rendering to reduce the number of renders
  const debouncedRender = debounce(() => {
    setRenderKey(prevKey => prevKey + 1);
  }, 300);

  useEffect(() => {
    debouncedRender();
    return () => {
      debouncedRender.cancel();
    };
  }, [plainData, graphSize]);

  return <FlowAnalysisGraph key={renderKey} {...config} />;
};

export default memo(CacheFlowAnalysisGraph, (pre, next) => {
  const isPlainDataEqual = isEqual(pre.plainData, next.plainData);
  const isGraphSizeEqual = pre.graphSize === next.graphSize;
  const isGraphSizeHeightEqual = pre.graphSize.height === next.graphSize.height;
  return isPlainDataEqual && isGraphSizeEqual && isGraphSizeHeightEqual;
});


