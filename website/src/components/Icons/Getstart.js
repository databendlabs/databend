import React from 'react';
import PropTypes from 'prop-types';

const Getstart = props => {
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
      <path d="M14.9 16.3l1.187-3.264a.7.7 0 00-.345-.865l-5.429-2.714a.7.7 0 00-.626 0L4.258 12.17a.7.7 0 00-.345.865L5.1 16.3"></path>
      <path d="M13.85 6.5h-7.7a.7.7 0 00-.7.7v4.2l4.257-1.965a.7.7 0 01.586 0L14.55 11.4V7.2a.7.7 0 00-.7-.7z"></path>
      <path d="M11.75 6.5V3.7a.7.7 0 00-.7-.7h-2.1a.7.7 0 00-.7.7v2.8"></path>
      <path d="M10 12.8v2.8"></path>
      <path d="M3 17c1.4 0 1.4-.7 2.45-.7 1.05 0 1.05.7 2.1.7s1.225-.7 2.45-.7 1.4.7 2.45.7c1.05 0 1.05-.7 2.1-.7s1.05.7 2.45.7"></path>
    </svg>
  );
};

Getstart.propTypes = {
  color: PropTypes.string,
  size: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
};

Getstart.defaultProps = {
  color: 'currentColor',
  size: '24'
};

export default Getstart;
