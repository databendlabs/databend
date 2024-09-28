import { useCallback } from "react";
import { calculateNodeOffsets, setNodeActiveState } from "../utills";
import { IGraph } from "@ant-design/charts";
import { IErrors, IOverview, IStatisticsDesc, Profile } from "../types/ProfileGraphDashboard";

export function useNodeSelection(
  graphRef: React.RefObject<IGraph>,
  plainData: Profile[],
  setSelectedNodeId: React.Dispatch<React.SetStateAction<string>>,
  setOverviewInfo: React.Dispatch<React.SetStateAction<IOverview | undefined>>,
): {
  handleNodeSelection: (nodeId: string) => void;
  setOverInfo: (data: IOverview) => void;
} {
  const centerNodeInView = useCallback((nodeId) => {
    if (!graphRef.current) return;
    const graph: IGraph = graphRef.current;
    const nodes = graph?.getNodes();
    const node = nodes?.find(n => n?._cfg?.id === nodeId);
    if (node) {
      const { offsetX, offsetY } = calculateNodeOffsets(graph, node);
      setNodeActiveState(graph, node, true);
      graph?.moveTo(offsetX, offsetY);
    }
  }, [graphRef]);

  const setOverInfo = useCallback((data: IOverview) => {
    const {
      totalTime,
      totalTimePercent,
      cpuTime,
      waitTime,
      cpuTimePercent,
      waitTimePercent,
      labels,
      statisticsDescArray,
      errors,
      name,
    } = data;
    setOverviewInfo({
      cpuTime,
      waitTime,
      totalTime,
      totalTimePercent,
      cpuTimePercent,
      waitTimePercent,
      labels,
      statisticsDescArray: statisticsDescArray as IStatisticsDesc[],
      errors: errors as unknown as IErrors[],
      name,
    });
  }, [setOverviewInfo]);

  const handleNodeSelection = useCallback((nodeId: string) => {

    const selectedItem = plainData?.find(item => item.id === nodeId);
    setSelectedNodeId(nodeId);

    if (selectedItem) {
      setOverInfo(selectedItem as unknown as IOverview);
      centerNodeInView(selectedItem.id);
    }
  }, [plainData, setSelectedNodeId, setOverInfo, centerNodeInView]);

  return { handleNodeSelection, setOverInfo };
}
