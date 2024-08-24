import React from 'react';
import { StatisticsData } from '../types/ProfileGraphDashboard';

interface StatisticsProps {
  statisticsData: StatisticsData;
}

const Statistics: React.FC<StatisticsProps> = ({ statisticsData }) => {
  return (
    <div className="expensive-nodes-card">
      <div className="expensive-nodes-card-header">
        <h2>Statistics</h2>
      </div>
      {statisticsData?.statistics?.slice(2)?.map((item, index) => (
        Boolean(item.value) && (
          <div key={index} className="expensive-nodes-node">
            <div className="expensive-nodes-node-name">{item.name}</div>
            <div className="expensive-nodes-node-percentage">{item.value} {item.unit}</div>
          </div>
        )
      ))}
    </div>
  );
};

export default Statistics;
