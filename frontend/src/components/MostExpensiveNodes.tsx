import React from 'react';
import { Profile } from '../types/ProfileGraphDashboard';
interface MostExpensiveNodesProps {
  data: Profile[];
  plainData: Profile[];
  selectedNodeId: string | null;
  handleNodeSelection: (nodeId: string) => void;
}

const MostExpensiveNodes: React.FC<MostExpensiveNodesProps> = ({ data, plainData, selectedNodeId, handleNodeSelection }) => {

  return (
    <div className="mt-5 bg-white shadow-md w-77 box-border rounded-lg p-6 border border-gray-200">
        <div className="mb-2">
        <h2>Most Expensive Nodes <span>({data?.length} of {plainData?.length})</span></h2>
      </div>
     {data?.map((node) => (
        <div
          key={node.id}
          className={`expensive-nodes-node ${selectedNodeId === node.id ? "selected" : ""}`}
          onClick={() => handleNodeSelection(node?.id!)}
        >
          <div className="expensive-nodes-node-name">
            {node?.name} [{node?.id}]
          </div>
          <div className="expensive-nodes-node-percentage">
          {node?.totalTimePercent}
          </div>
        </div>
      ))}
    </div>
  );
};

export default MostExpensiveNodes;
