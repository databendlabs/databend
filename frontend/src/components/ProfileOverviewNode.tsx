import React from 'react';
import { ProfileData } from '../types/ProfileGraphDashboard';
import { Progress } from 'antd';

interface ProfileOverviewNodeProps {
  profileData: ProfileData;
}

const ProfileOverviewNode: React.FC<ProfileOverviewNodeProps> = ({ profileData }) => {
  return (
    <div className="expensive-nodes-card">
      <div className="expensive-nodes-card-header">
        <h2>Profile Overview</h2>
      </div>
      <div className="expensive-nodes-progress">
        <Progress
          percent={profileData?.cpuTimePercentage}
          trailColor="rgba(255, 152, 0)"
          strokeColor="rgb(28, 130, 242)"
          status="active"
          showInfo={false}
        />
        <div className="expensive-nodes-percentage">
          {profileData?.cpuTimePercentage}%
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
          {profileData?.cpuTimePercentage}%
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
          {profileData?.ioTimePercentage}%
        </div>
      </div>
    </div>
  );
};

export default ProfileOverviewNode;
