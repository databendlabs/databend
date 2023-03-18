import React from 'react';
import PropTypes from 'prop-types';

const Getstart = props => {
  const { color, size, ...otherProps } = props;
  return (
    <svg 
    width={size} viewBox="0 0 23 26" fill="none" {...otherProps} xmlns="http://www.w3.org/2000/svg">
    <path d="M15.1188 4.2296C16.6163 2.40077 16.2817 0.403024 16.1161 0.237357C15.9504 0.0716908 13.787 0.403024 12.2895 2.40077C10.9577 4.2296 10.9577 6.22734 11.1266 6.39301C11.2955 6.55868 13.4557 6.39301 15.1188 4.2296ZM18.7797 13.5459C18.6141 10.8855 21.1088 9.22234 21.6058 8.88775V8.72209C21.6058 8.72209 19.4424 6.06168 16.285 6.22734C14.2905 6.39301 13.29 7.39026 11.9614 7.39026C10.2982 7.39026 8.80075 6.22734 6.97191 6.22734C5.47442 6.22734 1.15084 7.55917 0.819509 13.2113C0.488176 18.8667 4.47717 24.1876 5.97467 25.1848C7.47216 26.182 8.46941 25.8507 10.1326 25.0191C10.9642 24.5189 13.6246 24.3532 15.1221 25.3505C16.9509 26.0164 19.6113 25.5161 22.6063 18.8635C22.2717 18.8667 18.9454 18.0352 18.7797 13.5459Z" fill="white"/>
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
