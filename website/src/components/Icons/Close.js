import React from 'react';
import PropTypes from 'prop-types';

const Close = props => {
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
      <path d="M5 5l10 10"></path>
      <path d="M5 15L15 5"></path>
    </svg>
  );
};

Close.propTypes = {
  color: PropTypes.string,
  size: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
};

Close.defaultProps = {
  color: 'currentColor',
  size: '24'
};

export default Close;
