import React from 'react';
import PropTypes from 'prop-types';

const Github = props => {
  const { color, size, ...otherProps } = props;
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 20 20"
      preserveAspectRatio="xMidYMid meet"
      stroke="none"
      fill={color}
      strokeWidth="1"
      strokeLinecap="round"
      strokeLinejoin="round"
      {...otherProps}
    >
      <path
        fillRule="evenodd"
        clipRule="evenodd"
        d="M10.177 3A7.175 7.175 0 003 10.177a7.172 7.172 0 004.907 6.81c.36.062.494-.153.494-.342 0-.17-.01-.735-.01-1.336-1.802.332-2.269-.44-2.413-.844-.08-.206-.43-.843-.735-1.013-.251-.135-.61-.467-.01-.476.566-.009.97.52 1.104.736.646 1.085 1.678.78 2.09.592.063-.467.252-.78.458-.96-1.597-.18-3.265-.798-3.265-3.544 0-.78.278-1.426.735-1.929-.072-.179-.323-.915.072-1.901 0 0 .601-.189 1.974.735a6.66 6.66 0 011.794-.242c.61 0 1.22.08 1.794.242 1.373-.933 1.974-.735 1.974-.735.395.986.144 1.722.072 1.901.457.503.735 1.14.735 1.93 0 2.754-1.677 3.364-3.274 3.543.26.224.484.655.484 1.328 0 .96-.009 1.731-.009 1.973 0 .189.135.413.494.341a7.189 7.189 0 004.89-6.809A7.175 7.175 0 0010.176 3z"
        
      ></path>
    </svg>
  );
};

Github.propTypes = {
  color: PropTypes.string,
  size: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
};

Github.defaultProps = {
  color: 'currentColor',
  size: '24'
};

export default Github;
