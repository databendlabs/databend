import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';

import ProfileGraphDashboard from './ProfileGraphDashboard';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    <ProfileGraphDashboard />
  </React.StrictMode>
);

