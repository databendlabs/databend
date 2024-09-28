import { useCallback, useEffect, useState } from "react";
import { ALL_NODE_ID } from "../constants";
import { useRefValueListener } from "./useRefValueListener";
import { IGraph } from "@ant-design/charts";
import { IOverview, Profile } from "../types/ProfileGraphDashboard";

export function useGraphEvents(
  graphRef: React.RefObject<IGraph>,
  plainData: Profile[],
  setOverInfo: React.Dispatch<React.SetStateAction<IOverview>>,
  setSelectedNodeId: React.Dispatch<React.SetStateAction<string>>,
  profileWrapRefCanvas: React.MutableRefObject<HTMLCanvasElement>,
  profileWrapRef: React.RefObject<HTMLDivElement>,
  overviewInfoCurrent: React.RefObject<IOverview | undefined>,
  setOverviewInfo: React.Dispatch<React.SetStateAction<IOverview | undefined>>,
) {

  const [graph, setGraph] = useState(graphRef.current);

  useRefValueListener(graphRef, (graph: IGraph) => {
    setGraph(graph);
  });

  const getAllNodes = useCallback(() => {
    return graph?.getNodes();
  }, [graph]);

  const setNodeActive = useCallback(( node) => {
    graph?.setItemState(node, "highlight", true);
  }, [graph]);

  const clearNodeActive = useCallback(() => {
    getAllNodes()?.forEach(n => {
      graph?.clearItemStates(n);
    });
  }, [graph, getAllNodes]);

  useEffect(() => {
    if (!graph) return;

    const handleNodeClick = (evt) => {
      const modal = evt.item._cfg.model;
      setOverInfo({
        ...plainData.find(item => item.id === modal.id),
      } as IOverview);
      setSelectedNodeId(modal.id);

      const nodes = getAllNodes();
      const id = evt.item._cfg.id;
      const node = nodes?.find(node => node?._cfg?.id === id);
      nodes
        ?.filter(node => node?._cfg?.id !== id)
        .forEach(n => {
          graph?.clearItemStates(n);
        });

      setNodeActive(node);
    };

    const handleNodeMouseLeave = () => {
      if (!profileWrapRefCanvas.current) {
        profileWrapRefCanvas.current = document.getElementsByTagName("canvas")[0];
      }
      profileWrapRefCanvas.current.style.cursor = "move";
    };

    const handleCanvasClick = () => {
      setSelectedNodeId(ALL_NODE_ID);
      setOverviewInfo(overviewInfoCurrent.current || undefined);
      clearNodeActive();
    };

    const handleCanvasDragStart = () => {
      if (profileWrapRef?.current) {
        profileWrapRef.current.style.userSelect = "none";
      }
    };

    const handleCanvasDragEnd = () => {
      if (profileWrapRef?.current) {
        profileWrapRef.current.style.userSelect = "unset";
      }
    };

    graph.on("node:click", handleNodeClick);
    graph.on("node:mouseleave", handleNodeMouseLeave);
    graph.on("canvas:click", handleCanvasClick);
    graph.on("canvas:dragstart", handleCanvasDragStart);
    graph.on("canvas:dragend", handleCanvasDragEnd);

    return () => {
      graph.off("node:click", handleNodeClick);
      graph.off("node:mouseleave", handleNodeMouseLeave);
      graph.off("canvas:click", handleCanvasClick);
      graph.off("canvas:dragstart", handleCanvasDragStart);
      graph.off("canvas:dragend", handleCanvasDragEnd);
    };
  }, [graph, plainData, setOverInfo, setSelectedNodeId, getAllNodes, setNodeActive, clearNodeActive, profileWrapRefCanvas, profileWrapRef, overviewInfoCurrent, setOverviewInfo]);
}