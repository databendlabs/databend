import React from 'react';
import { AttributeItem } from '../types/ProfileGraphDashboard';

interface AttributesProps {
  attributesData: AttributeItem[];
}

const Attributes: React.FC<AttributesProps> = ({ attributesData }) => {
  return (
    <>
    {attributesData?.length > 0 && (
    <div className="expensive-nodes-card">
      <div className="expensive-nodes-card-header">
        <h2>Attributes</h2>
      </div>
      {attributesData?.map((item, index) => (
        <div key={index}>
          <div className="expensive-nodes-node-title">{item.name}</div>
            <div className="expensive-nodes-node block">
              {item?.value?.map((value, idx) => (
                <div className="expensive-nodes-node-name" key={idx}>
                  {value}
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    )}
    </>
  );
};

export default Attributes;
