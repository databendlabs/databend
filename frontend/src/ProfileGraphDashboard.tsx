import React, { useRef, useState } from "react";
import { Layout } from "antd";
import { IGraph } from "@ant-design/charts";

import MostExpensiveNodes from './components/MostExpensiveNodes';
import ProfileOverview from './components/ProfileOverview';
import ProfileOverviewNode from './components/ProfileOverviewNode';
import Statistics from './components/Statistics';
import Attributes from './components/Attributes';
import FlowAnalysisGraph from './components/FlowAnalysisGraph';

import { useProfileData } from "./hooks/useProfileData";
import { useGraphSize } from "./hooks/useGraphSize";
import { useGraphEvents } from "./hooks/useGraphEvents";
import { useNodeSelection } from "./hooks/useNodeSelection";

import { IGraphSize, IOverview, Profile } from "./types/ProfileGraphDashboard";
import { ALL_NODE_ID } from "./constants";

import "./css/ProfileGraphDashboard.css";

const { Content, Sider } = Layout;

function ProfileGraphDashboard() {
  const [selectedNodeId, setSelectedNodeId] = useState<string>(ALL_NODE_ID);

  const profileWrapRefCanvas = useRef<HTMLCanvasElement|null>(null);

  const profileWrapRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<IGraph | null>(null);

  const { graphSize, profileRef, handleResize } = useGraphSize();

  const {
    plainData,
    rangeData,
    statisticsData,
    labels,
    overviewInfo,
    isLoading,
    setOverviewInfo,
    setIsLoading,
    overviewInfoCurrent,
  } = useProfileData();


  const { handleNodeSelection, setOverInfo } = useNodeSelection(graphRef, plainData, setSelectedNodeId, setOverviewInfo);

  useGraphEvents(
    graphRef,
    plainData,
    setOverInfo as React.Dispatch<React.SetStateAction<IOverview>>,
    setSelectedNodeId,
    profileWrapRefCanvas as React.MutableRefObject<HTMLCanvasElement>,
    profileWrapRef, overviewInfoCurrent, setOverviewInfo);

  return (
    <Layout>
      <Layout
        ref={profileRef}
        className="bg-white w-full rounded-lg"
        >
        <Content className="p-6 w-full flex">
        <GraphContent
            isLoading={isLoading}
            plainData={plainData}
            graphSize={graphSize}
            graphRef={graphRef as React.MutableRefObject<IGraph>}
            handleResize={handleResize}
            overviewInfoCurrent={overviewInfoCurrent}
            setIsLoading={setIsLoading}
            profileWrapRef={profileWrapRef}
            profileWrapRefCanvas={profileWrapRefCanvas}
          />
          <SidebarContent
            rangeData={rangeData}
            plainData={plainData}
            selectedNodeId={selectedNodeId}
            handleNodeSelection={handleNodeSelection}
            overviewInfo={overviewInfo}
            statisticsData={statisticsData}
            labels={labels}
            graphSize={graphSize}
          />
        </Content>
      </Layout>
    </Layout>
  );
}

function GraphContent({
  isLoading,
  plainData,
  graphSize,
  graphRef,
  handleResize,
  overviewInfoCurrent,
  setIsLoading,
  profileWrapRef,
  profileWrapRefCanvas,
}: {
  isLoading: boolean;
  plainData: Profile[];
  graphSize: IGraphSize;
  graphRef: React.MutableRefObject<IGraph>;
  handleResize: () => void;
  overviewInfoCurrent: React.RefObject<IOverview | undefined>;
  setIsLoading: React.Dispatch<React.SetStateAction<boolean>>;
  profileWrapRef: React.RefObject<HTMLDivElement>;
  profileWrapRefCanvas: React.RefObject<HTMLCanvasElement>;
}) {
  return (
    <div ref={profileWrapRef} className="flex-1 flex justify-center items-center h-screen">
      {isLoading ? (
        <div className="w-full h-full">loading...</div>
      ) : (
        <FlowAnalysisGraph
          plainData={plainData}
          graphSize={graphSize}
          graphRef={graphRef}
          overviewInfoCurrent={overviewInfoCurrent}
          onReady={(graph: IGraph) => {
            if (isLoading) {
              graph.fitView();
              graph.refresh();
              setIsLoading(false);
            }
            graphRef.current = graph;
            graph.setMaxZoom(2);
            graph.setMinZoom(0.5);
          }}
        />
      )}
    </div>
  );
}

function SidebarContent({
  rangeData,
  plainData,
  selectedNodeId,
  handleNodeSelection,
  overviewInfo,
  statisticsData,
  labels,
  graphSize,
}) {
  return (
    <Sider width={308} style={{ background: "#fff" }}>
      <div className="overflow-y-auto" style={{ height: graphSize.height }}>
        <MostExpensiveNodes
          data={rangeData}
          plainData={plainData}
          selectedNodeId={selectedNodeId}
          handleNodeSelection={handleNodeSelection}
        />
        {selectedNodeId !== ALL_NODE_ID ? (
          <>
            <ProfileOverviewNode overviewInfo={overviewInfo} />
            <Statistics statisticsData={statisticsData.find((stat) => stat.id === selectedNodeId)!} />
            <Attributes attributesData={labels.find((label) => label.id === selectedNodeId)?.labels!} />
          </>
        ) : (
          <ProfileOverview queryDuration={overviewInfo?.totalTime || 0} overviewInfo={overviewInfo} />
        )}
      </div>
    </Sider>
  );
}

export default ProfileGraphDashboard;
