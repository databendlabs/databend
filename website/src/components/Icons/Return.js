import React from 'react';
import PropTypes from 'prop-types';

const Return = props => {
  const { color, size, ...otherProps } = props;
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 20 20"
      preserveAspectRatio="xMidYMid meet"
      fill="none"
      stroke={color}
      strokeWidth="1"
      strokeLinecap="round"
      strokeLinejoin="round"
      {...otherProps}
    >
      <path d="M5.722 4L3 6.333l2.722 2.723"></path>
      <path d="M3 6.333h8.942c2.677 0 4.95 2.186 5.054 4.861.11 2.827-2.225 5.25-5.054 5.25h-6.61"></path>
    </svg>
  );
};

Return.propTypes = {
  color: PropTypes.string,
  size: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
};

Return.defaultProps = {
  color: 'currentColor',
  size: '24'
};

export default Return;
