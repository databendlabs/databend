import React from 'react';
import PropTypes from 'prop-types';

const Label = props => {
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
      <path d="M4.75 3h10.5v14L10 13.3 4.75 17V3z"></path>
      <path d="M15.25 3H4.75v4.2h10.5V3z"></path>
    </svg>
  );
};

Label.propTypes = {
  color: PropTypes.string,
  size: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
};

Label.defaultProps = {
  color: 'currentColor',
  size: '24'
};

export default Label;
