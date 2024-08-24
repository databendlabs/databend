import React from 'react';
import { ProfileData } from '../types/ProfileGraphDashboard';

interface ProfileOverviewProps {
  profileData: ProfileData;
}

const ProfileOverview: React.FC<ProfileOverviewProps> = ({ profileData }) => {
  return (
    <div className="expensive-nodes-card">
      <div className="expensive-nodes-card-header">
        <h2>
          Profile Overview <span>(Finished)</span>
        </h2>
      </div>
      <div className="expensive-nodes-node">
        <div className="expensive-nodes-node-name">Total Execution Time</div>
        <div className="expensive-nodes-node-percentage">
          ({profileData?.totalExecutionTime}ms) 100%
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

export default ProfileOverview;
