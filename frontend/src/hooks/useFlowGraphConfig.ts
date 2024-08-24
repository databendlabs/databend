import { useEffect, useRef } from "react";
import { FlowAnalysisGraphConfig } from "@ant-design/graphs";
import { G6 } from "@ant-design/charts";
import { GraphData } from "../types/ProfileGraphDashboard";

interface UseFlowGraphConfigParams {
  graphData: GraphData;
  onNodeClick: (nodeId: string) => void;
}

export const useFlowGraphConfig = ({
  graphData,
  onNodeClick,
}: UseFlowGraphConfigParams): FlowAnalysisGraphConfig => {
  const graphInstanceRef = useRef<any>(null);

  useEffect(() => {
    if (graphInstanceRef.current) {
      const graph = graphInstanceRef.current;

      graph.on("node:click", (evt: any) => {
        const clickedNode = evt.item.getModel();
        onNodeClick(clickedNode.id);
      });

      graph.on("canvas:click", () => {
        onNodeClick("all");
      });
    }
  }, [onNodeClick]);

  return {
    data: graphData,
    nodeCfg: {
      size: [310, 80],
      customContent: (item: any, group: any, cfg: any) => {
        const { startX, startY } = cfg;
        const { name, progress, title } = item;

        if (name) {
          group?.addShape("text", {
            attrs: {
              textBaseline: "top",
              x: startX + 10,
              y: startY + 10,
              text: name,
              fill: "black",
              fontWeight: "bold",
            },
            name: "name",
          });
        }

        if (progress !== undefined) {
          group?.addShape("text", {
            attrs: {
              textBaseline: "top",
              x: startX + 280,
              y: startY + 10,
              text: `${progress}%`,
              fill: "black",
              textAlign: "right",
            },
            name: "progress",
          });
        }

        if (title) {
          group?.addShape("text", {
            attrs: {
              textBaseline: "top",
              x: startX + 10,
              y: startY + 30,
              text: title,
              fill: "gray",
            },
            name: "title",
          });
        }

        group?.addShape("rect", {
          attrs: {
            x: startX + 10,
            y: startY + 60,
            width: 280,
            height: 10,
            fill: "lightgray",
            radius: [5, 5, 5, 5],
          },
          name: "progress-bg",
        });

        if (progress < 5) {
          group?.addShape("circle", {
            attrs: {
              x: startX + 10,
              y: startY + 65,
              r: progress,
              fill: "rgb(28, 130, 242)",
            },
            name: "progress-circle",
          });
        } else {
          group?.addShape("rect", {
            attrs: {
              x: startX + 10,
              y: startY + 60,
              width: (280 * progress) / 100,
              height: 10,
              fill: "rgb(28, 130, 242)",
              radius: [5, 5, 5, 5],
            },
            name: "progress-bar",
          });
        }

        return 5;
      },
      anchorPoints: [
        [0.5, 0],
        [0.5, 1],
      ],
      items: {
        containerStyle: {
          fill: "#fff",
        },
        padding: 0,
      },
      nodeStateStyles: {
        hover: {
          stroke: "rgb(28, 130, 242)",
          lineWidth: 2,
        },
        selected: {
          stroke: "rgb(28, 130, 242)",
          lineWidth: 2,
        },
      },
      style: {
        stroke: "#cccccc",
        radius: 8,
      },
      title: {
        containerStyle: {
          fill: "transparent",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        },
        style: {
          fill: "#000",
          fontSize: 12,
        },
      },
    },
    layout: {
      rankdir: "BT",
      ranksepFunc: () => 12,
    },
    autoFit: true,
    edgeCfg: {
      type: "polyline",
      endArrow: {
        path: G6.Arrow.triangle(10, 10, 0),
        fill: "#999",
      },
      style: {
        stroke: "#999",
        lineWidth: 1,
      },
    },
    behaviors: ["drag-canvas", "zoom-canvas", "drag-node", "click-select"],
    onReady: (graph) => {
      graphInstanceRef.current = graph;
    },
  };
};
