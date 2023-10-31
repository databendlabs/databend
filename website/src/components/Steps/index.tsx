import React from 'react';

function Steps({ steps }) {
  return (
    <div>
      {steps.map((content, index) => (
         <div className="step-container">
         <div className="step-number">{index+1}.</div>
         <div className="step-content" dangerouslySetInnerHTML={{ __html: content }}></div>
       </div>
      ))}
    </div>
  );
}

export default Steps;