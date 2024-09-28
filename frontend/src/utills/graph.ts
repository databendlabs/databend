import { IGraph, INode } from "@ant-design/charts";
import { EdgeWithLineWidth } from "../components/FlowAnalysisGraph";
import { OUTPUT_ROWS } from "../constants";
import { Profile } from "../types/ProfileGraphDashboard";

/**
 * calculate node offsets
 * @param graph
 * @param node
 * @returns
 */
export function calculateNodeOffsets(graph: IGraph, node: INode) {
  const zoom: number = graph?.getZoom() || 1;
  const x: number = node?._cfg?.bboxCache?.x || 0;
  const y: number = node?._cfg?.bboxCache?.y || 0;
  const width: number = graph?.getWidth() || 0;
  const height: number = graph?.getHeight() || 0;
  return {
    offsetX: width / 2 - x * zoom,
    offsetY: height / 2 - y * zoom + 20,
  };
}

/**
 * set node active state
 * @param graph
 * @param node
 * @param clear
 */
export function setNodeActiveState(graph: IGraph, node: INode, clear = false) {
  if (clear) {
    graph.getNodes().forEach(n => graph.clearItemStates(n));
  }
  graph.setItemState(node as any, "highlight", true);
}

/**
 * format rows
 * @param value
 * @returns
 */
export function formatRows(value: number): string {
  if (value < 1000) return value.toString();
  if (value >= 1000 && value < 1000000) return (value / 1000).toFixed(1) + "K";
  if (value >= 1000000 && value < 1000000000) return (value / 1000000).toFixed(1) + "M";
  return (value / 1000000000).toFixed(1) + "B";
}

/**
 * get text width
 * @param text
 * @param font
 * @returns
 */
export function getTextWidth(text: string, font: string = '12px Arial'): number {
  const canvas = document.createElement('canvas');
  const context = canvas.getContext('2d');
  if (!context) return 0;
  context.font = font;
  const metrics = context.measureText(text);
  return metrics.width;
}

/**
 * map edges line width
 * @param edges
 * @returns
 */
export function mapEdgesLineWidth(edges: EdgeWithLineWidth[]) {
  if (!edges || edges.length <= 2) return edges;

  // Define the minimum and maximum lineWidth
  const minLineWidth = 1;
  const maxLineWidth = 4.5;

  // Extract values and calculate min and max in a single pass
  let minValue = Infinity;
  let maxValue = -Infinity;
  const values: number[] = [];

  edges.forEach((edge: EdgeWithLineWidth) => {
    if (edge?._value !== undefined) {
      values.push(edge?._value);
      if (edge?._value < minValue) minValue = edge?._value;
      if (edge?._value > maxValue) maxValue = edge?._value;
    }
  });

  if (values.length === 0) return edges;

  const range = maxValue - minValue;
  const scale = range === 0 ? 0 : (maxLineWidth - minLineWidth) / range;

  edges.forEach(edge => {
    if (edge._value !== undefined) {
      edge.lineWidth = minLineWidth + (edge._value - minValue) * scale;
    } else {
      edge.lineWidth = minLineWidth;
    }
  });

  return edges;
}

/**
 * create rounded rect path
 * @param x
 * @param y
 * @param width
 * @param height
 * @param radius
 * @returns
 */
export const createRoundedRectPath = (x: number, y: number, width: number, height: number, radius: number) => [
  ["M", x + radius, y],
  ["l", width - radius * 2, 0],
  ["a", radius, radius, 0, 0, 1, radius, radius],
  ["l", 0, height - radius * 2],
  ["a", radius, radius, 0, 0, 1, -radius, radius],
  ["l", -width + radius * 2, 0],
  ["a", radius, radius, 0, 0, 1, -radius, -radius],
  ["l", 0, -height + radius * 2],
  ["a", radius, radius, 0, 0, 1, radius, -radius],
  ["Z"],
];

/**
 * get deal data for graph
 * @param plainData
 * @returns
 */
export const getDealData = (plainData: Profile[]) => {
  if (!plainData || plainData.length === 0) return [];

  let outputRowsIndex = -1;

  // Find the index of OUTPUT_ROWS once
  for (const node of plainData) {
    const { statisticsDescArray } = node;
    if (statisticsDescArray && outputRowsIndex === -1) {
      outputRowsIndex = statisticsDescArray.findIndex(item => item._type === OUTPUT_ROWS);
      if (outputRowsIndex !== -1) break;
    }
  }

  // If OUTPUT_ROWS index is not found, set it to 0
  if (outputRowsIndex === -1) outputRowsIndex = 0;

  return plainData.map(node => {
    const { title, name, id, statisticsDescArray } = node;
    const outputRows = statisticsDescArray?.[outputRowsIndex]?._value || 0;

    return {
      totalTime: node.totalTime,
      id: id,
      outputRows,
      value: {
        title: (name?.length >= 26 ? name.slice(0, 26) + "..." : name || "  ") + ` [${id}]`,
        items: [{ text: title || "  " }],
      },
    };
  });
};


/**
 * get edges for graph
 * @param plainData
 * @returns
 */
export const getEdges = (plainData: Profile[]): EdgeWithLineWidth[] => {
  if (!plainData || plainData.length === 0) return [];

  let outputRowsIndex = -1;

  // Find the index of OUTPUT_ROWS once
  for (const node of plainData) {
    const { statisticsDescArray } = node;
    if (statisticsDescArray && outputRowsIndex === -1) {
      outputRowsIndex = statisticsDescArray.findIndex(item => item._type === OUTPUT_ROWS);
      if (outputRowsIndex !== -1) break;
    }
  }

  // If OUTPUT_ROWS index is not found, set it to 0
  if (outputRowsIndex === -1) outputRowsIndex = 0;

  return plainData.map(node => {
    const { statisticsDescArray, parent_id, id } = node;
    const outputRows = statisticsDescArray?.[outputRowsIndex]?._value || 0;
    const nodeInfo = { source: parent_id, target: id };

    return outputRows <= 0
      ? nodeInfo
      : { ...nodeInfo, value: formatRows(outputRows), _value: outputRows };
  }) as unknown as EdgeWithLineWidth[];
};
