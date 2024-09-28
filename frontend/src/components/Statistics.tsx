import React, { useMemo } from 'react';
import { StatisticsData } from '../types/ProfileGraphDashboard';

interface StatisticsProps {
  statisticsData: StatisticsData;
}

const Statistics: React.FC<StatisticsProps> = ({ statisticsData }) => {
  const statistics = useMemo(() => statisticsData?.statistics?.slice(2), [statisticsData]);

  const showStatistics = useMemo(() => statistics?.filter((item) => Boolean(item.value)).length > 0, [statistics]);

  return (
    <>
    {showStatistics && (
    <div className="expensive-nodes-card">
      <div className="expensive-nodes-card-header">
        <h2>Statistics</h2>
      </div>
      {statistics?.map((item, index) => (
        Boolean(item.value) && (
          <div key={index} className="expensive-nodes-node">
            <div className="expensive-nodes-node-name">{item.name}</div>
            <div className="expensive-nodes-node-percentage">{item.value} {item.unit}</div>
          </div>
        )
      ))}
    </div>)}
    </>
  )
}

export default Statistics;
