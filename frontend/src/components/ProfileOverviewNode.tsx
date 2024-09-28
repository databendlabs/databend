import React from 'react';
import { Progress } from 'antd';
import { IOverview } from '../types/ProfileGraphDashboard';

interface ProfileOverviewNodeProps {
  overviewInfo?: IOverview;
}

const ProfileOverviewNode: React.FC<ProfileOverviewNodeProps> = ({ overviewInfo }) => {
  const cpuTimePercent = parseFloat(overviewInfo?.cpuTimePercent || "0");
  return (
    <div className="expensive-nodes-card">
      <div className="expensive-nodes-card-header">
        <h2>Profile Overview</h2>
      </div>
      <div className="expensive-nodes-progress">
        <Progress
          percent={cpuTimePercent}
          trailColor="rgba(255, 152, 0)"
          strokeColor="rgb(28, 130, 242)"
          status="active"
          showInfo={false}
        />
        <div className="expensive-nodes-percentage">
          {overviewInfo?.totalTimePercent}
        </div>
      </div>
      <div className="expensive-nodes-node">
        <div className="expensive-nodes-node-name">
          <span
            className="custom-dot"
            style={{ backgroundColor: "rgb(28, 130, 242)" }}
          />
          CPU Time
        </div>
        <div className="expensive-nodes-node-percentage">
          {overviewInfo?.cpuTimePercent}
        </div>
      </div>
      <div className="expensive-nodes-node">
        <div className="expensive-nodes-node-name">
          <span
            className="custom-dot"
            style={{ backgroundColor: "rgb(255, 152, 0)" }}
          />
          I/O Time
        </div>
        <div className="expensive-nodes-node-percentage">
          {overviewInfo?.waitTimePercent}
        </div>
      </div>
    </div>
  );
};

export default ProfileOverviewNode;
