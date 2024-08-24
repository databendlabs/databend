import React from 'react';
import { GraphData } from '../types/ProfileGraphDashboard';

interface Props {
  data: GraphData;
  selectedNodeId: string | null;
  handleNodeSelection: (nodeId: string) => void;
}

const MostExpensiveNodes: React.FC<Props> = ({ data, selectedNodeId, handleNodeSelection }) => {
  const sortedNodes = [...data.nodes].sort(
    (a, b) => b.value.items[0].progress - a.value.items[0].progress
  );

  return (
    <div className="expensive-nodes-card">
      <div className="expensive-nodes-card-header">
        <h2>Most Expensive Nodes <span>({sortedNodes.length})</span></h2>
      </div>
      {sortedNodes.map((node) => (
        <div
          key={node.id}
          className={`expensive-nodes-node ${selectedNodeId === node.id ? "selected" : ""}`}
          onClick={() => handleNodeSelection(node.id)}
        >
          <div className="expensive-nodes-node-name">
            {node.value.items[0].name} [{node.id}]
          </div>
          <div className="expensive-nodes-node-percentage">
            {node.value.items[0].progress}%
          </div>
        </div>
      ))}
    </div>
  );
};

export default MostExpensiveNodes;
