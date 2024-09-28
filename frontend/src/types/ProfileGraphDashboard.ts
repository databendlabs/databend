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
  lineWidth: any;
  _value: undefined;
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
  waitTimePercent: string;
  cpuTimePercent: string;
  totalTimePercent: string;
  statisticsDescArray: { _type: string; desc: string; display_name: string | undefined; index: number; unit: ("NanoSeconds" | "Bytes" | "Rows" | "Count" | "MillisSeconds") | undefined; plain_statistics: boolean | undefined; _value: number; }[];
  waitTime: number;
  cpuTime: number;
  totalTime: number;
  id?: string;
  name: string;
  parent_id: number | string | null;
  title: string;
  labels: Label[];
  statistics: number[];
  metrics?: Record<string, Metric[]>;
  errors: string[];
}

export interface MessageResponse {
  result: string;
}

export type TUnit = "NanoSeconds" | "MillisSeconds" | "Bytes" | "Rows" | "Count";
export interface IStatisticsDesc {
  _type: string;
  desc: string;
  index: number;
  _value: any;
  display_name: string;
  displayName?: string;
  plain_statistics?: boolean;
  unit?: TUnit;
}

export interface IErrors {
  backtrace: string;
  detail: string;
  message: string;
  _errorType: string;
}

export interface IOverview {
  cpuTime: number;
  waitTime: number;
  totalTime: number;
  isTotalBigerZero?: boolean;
  totalTimePercent?: string;
  cpuTimePercent?: string;
  waitTimePercent?: string;
  id?: string;
  labels?: { name: string; value: any[] }[];
  statisticsDescArray?: IStatisticsDesc[];
  errors?: IErrors[];
  name?: string;
}

export interface IGraphSize {
  width: number;
  height: number;
}
