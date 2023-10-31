import React, { useState } from "react";
import PropTypes from "prop-types";
import styles from './styles.module.scss';
import clsx from "clsx";

const Tooltip = ({ content, children, style, contentStyle }) => {
  const [showTooltip, setShowTooltip] = useState(false);

  const handleMouseEnter = () => setShowTooltip(true);
  const handleMouseLeave = () => setShowTooltip(false);

  return (
    <div style={{ position: "relative", ...style }}>
      <div
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        style={{ display: "inline-block" }}
      >
        {children}
      </div>
      <div
        className={clsx(styles.tooltip,  showTooltip && styles.visible)}
        style={{
          ...contentStyle
        }}
      >
        {content}
      </div>
    </div>
  );
};

Tooltip.propTypes = {
  content: PropTypes.string.isRequired,
  children: PropTypes.element.isRequired,
  style: PropTypes.object,
  contentStyle: PropTypes.object
};

export default Tooltip;
