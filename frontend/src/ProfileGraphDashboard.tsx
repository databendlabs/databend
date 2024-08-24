import React, { useEffect, useState } from "react";
import { Layout } from "antd";

import { FlowAnalysisGraph } from "@ant-design/graphs";

import MostExpensiveNodes from './components/MostExpensiveNodes';
import ProfileOverview from './components/ProfileOverview';
import ProfileOverviewNode from './components/ProfileOverviewNode';
import Statistics from './components/Statistics';
import Attributes from './components/Attributes';

import { useFlowGraphConfig } from './hooks/useFlowGraphConfig';

import { GraphData, ProfileData, StatisticsData, AttributeData, StatisticsDesc, Profile, MessageResponse } from './types/ProfileGraphDashboard';

import "./less/ProfileGraphDashboard.css";

const { Content, Sider } = Layout;

function ProfileGraphDashboard() {
  const [graphData, setGraphData] = useState<GraphData>({ nodes: [], edges: [] });

  const [profileDataArray, setProfileDataArray] = useState<ProfileData[]>([]);

  const [selectedNodeId, setSelectedNodeId] = useState<string>("all");

  const [statisticsData, setStatisticsData] = useState<StatisticsData[]>([]);

  const [labels, setLabels] = useState<AttributeData[]>([]);

  const handleNodeSelection = (nodeId: string) => {
    setSelectedNodeId(nodeId);
  };

  const flowGraphConfig = useFlowGraphConfig({
    graphData,
    onNodeClick: handleNodeSelection,
  });


  useEffect(() => {
    const fetchMessage = async () => {
      try {
        const response: Response = await fetch("/api/message");
        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }
        const result: MessageResponse = await response.json();

        const data_json: {
            profiles: Profile[];
            statistics_desc: StatisticsDesc[];
        } = JSON.parse(result?.result);
        // setGraphData(data);
        const all_statistics_sum = data_json.profiles
          .map((profile: any) => profile.statistics[0] + profile.statistics[1])
          .reduce((acc: number, curr: number) => acc + curr, 0);

        const all_cpu_time = data_json.profiles
          .map((profile: any) => profile.statistics[0])
          .reduce((acc: number, curr: number) => acc + curr, 0);

        const all_io_time = data_json.profiles
          .map((profile: any) => profile.statistics[1])
          .reduce((acc: number, curr: number) => acc + curr, 0);

        const allprofileData = {
            id: "all",
            totalExecutionTime: parseFloat(
                (all_statistics_sum / 1_000_00).toFixed(2)
            ),
            cpuTimePercentage: parseFloat(
                ((all_cpu_time / all_statistics_sum) * 100).toFixed(2)
            ),
            ioTimePercentage: parseFloat(
                ((all_io_time / all_statistics_sum) * 100).toFixed(2)
            ),
        }

        const profileDataArray = data_json.profiles.map((profile: any) => {
            const totalExecutionTime = profile.statistics[0] + profile.statistics[1];
            const cpuTimePercentage = profile.statistics[0] / totalExecutionTime;
            const ioTimePercentage = profile.statistics[1] / totalExecutionTime;
            return{
                id: profile.id.toString(),
                totalExecutionTime: 0,
                cpuTimePercentage: parseFloat(
                    (cpuTimePercentage*100).toFixed(2)
                ),
                ioTimePercentage: parseFloat(
                (ioTimePercentage*100).toFixed(2)
                ),
            }
        });

        profileDataArray.push(allprofileData);

        setProfileDataArray(profileDataArray);

        const statisticsDesc = data_json.statistics_desc;
        const statsArray = data_json.profiles.map((profile: any) => {
            const statistics = Object.entries(statisticsDesc).map(([key, value]) => {
                const index = value.index;
                return {
                  name: value.display_name || key,
                  desc: value.desc,
                  value: profile.statistics[index],
                  unit: value.unit,
                };
            });
            return {statistics, id: profile.id.toString()};
        });

        setStatisticsData(statsArray);

        const nodes = data_json.profiles.map((profile: any) => ({
          id: profile.id.toString(),
          value: {
            items: [
              {
                id: profile.id.toString(),
                name: profile.name,
                title: profile.title,
                progress: Math.round(
                  (profile.statistics[0] / all_statistics_sum) * 100
                ),
                cpuTime: profile.statistics[0],
                ioTime: profile.statistics[1],
                text: profile.title,
              },
            ],
          },
        }));

        const edges = data_json.profiles
          .filter((profile: any) => profile.parent_id !== null)
          .map((profile: any) => ({
            target: profile.parent_id.toString(),
            source: profile.id.toString(),
          }));
        const data = {
          nodes,
          edges,
        };
        setGraphData(data);

        const labels = data_json.profiles.map((profile: any) => {
            return {
                labels: profile.labels,
                id: profile.id.toString(),
            }
        });
        setLabels(labels);
      } catch (error) {
        console.error("Error fetching message:", error);
      }
    };

    fetchMessage();
  }, []);

  return (
    <Layout
      style={{
        minHeight: "100vh",
        padding: "28px 24px 0",
        borderRadius: "8px",
      }}
    >
      <Layout
        style={{ background: "#fff", width: "100%", borderRadius: "8px" }}
      >
        <Content style={{ padding: "0 24px", width: "100%", display: "flex" }}>
          <div style={{ flex: 1 }}>
          <FlowAnalysisGraph {...flowGraphConfig} />
          </div>
          <Sider width={308} style={{ background: "#fff" }}>
            <MostExpensiveNodes
                data={graphData}
                selectedNodeId={selectedNodeId}
                handleNodeSelection={handleNodeSelection}
                />
            {selectedNodeId !== "all" ? (
                <>
                    <ProfileOverviewNode
                        profileData={profileDataArray.find((profile) => profile.id === selectedNodeId)!}
                        />
                    <Statistics
                        statisticsData={statisticsData.find((stat) => stat.id === selectedNodeId)!}
                        />
                    <Attributes
                        attributesData={labels.find((label) => label.id === selectedNodeId)?.labels!}
                        />
                </>
            ) : (
                 <ProfileOverview profileData={profileDataArray.find((profile) => profile.id === "all")!} />
            )}
          </Sider>
        </Content>
      </Layout>
    </Layout>
  );
}

export default ProfileGraphDashboard;
