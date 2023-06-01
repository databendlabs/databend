import React from 'react';
import PropTypes from 'prop-types';

const Avatar = props => {
  const { color, size, ...otherProps } = props;
  return (
    <svg t="1685499830379" className="icon" {...otherProps} viewBox="0 0 1024 1024" version="1.1" xmlns="http://www.w3.org/2000/svg" p-id="6003" width={size}><path d="M0.00001 512.077A511.923 511.923 0 1 0 511.92301 0 511.974 511.974 0 0 0 0.00001 512.077z" fill="#FFFFFF" p-id="6004"></path><path d="M887.49001 857.89c-13.697-71.82-139.895-140.459-253.165-177.96-5.54-1.846-40.014-17.339-18.417-82.798 56.43-57.815 99.214-150.924 99.214-242.597 0-140.82-93.827-214.742-202.891-214.742s-202.635 73.82-202.635 214.742c0 91.98 42.784 185.45 99.317 243.162 22.059 57.712-17.34 79.207-25.65 82.08-107.73 38.834-232.903 107.73-246.702 177.96a511.307 511.307 0 1 1 887.49-346.635 507.87 507.87 0 0 1-136.56 346.788" fill="#B8D4FF" p-id="6005"></path></svg>
  );
};

Avatar.propTypes = {
  color: PropTypes.string,
  size: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
};

Avatar.defaultProps = {
  size: '24'
};

export default Avatar;
