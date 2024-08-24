export interface NodeItem {
  name: string;
  title: string;
  progress: number;
  text: string;
}

export interface Node {
  id: string;
  value: {
    items: NodeItem[];
  };
}

export interface Edge {
  source: string;
  target: string;
}

export interface GraphData {
  nodes: Node[];
  edges: Edge[];
}

export interface ProfileData {
  id: string;
  totalExecutionTime: number;
  cpuTimePercentage: number;
  ioTimePercentage: number;
}

export interface StatisticsItem {
  name: string;
  value: number | string;
  unit: string;
}

export interface StatisticsData {
  id: string;
  statistics: StatisticsItem[];
}

export interface AttributeItem {
  name: string;
  value: string[];
}

export interface AttributeData {
  id: string;
  labels: AttributeItem[];
}

export interface StatisticsDesc {
  desc: string;
  display_name: string;
  index: number;
  unit: string;
  plain_statistics: boolean;
}

export interface Label {
  name: string;
  value: string[];
}

export interface Metric {
  name: string;
  labels: Record<string, string>;
  value: Record<string, number | number[]>;
}

export interface Profile {
  id: number;
  name: string;
  parent_id: number | null;
  title: string;
  labels: Label[];
  statistics: number[];
  metrics?: Record<string, Metric[]>;
}

export interface MessageResponse {
  result: string;
}