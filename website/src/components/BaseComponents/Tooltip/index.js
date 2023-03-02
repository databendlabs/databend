import React, { useState } from "react";
import PropTypes from "prop-types";

const Tooltip = ({ content, children }) => {
  const [showTooltip, setShowTooltip] = useState(false);

  const handleMouseEnter = () => setShowTooltip(true);
  const handleMouseLeave = () => setShowTooltip(false);

  return (
    <div style={{ position: "relative" }}>
      <div
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        style={{ display: "inline-block" }}
      >
        {children}
      </div>
      {showTooltip && (
        <div
          style={{
            position: "absolute",
            zIndex: 1,
            top: "-40px",
            left: "50%",
            transform: "translateX(-50%)",
            backgroundColor: "var(--color-bg-5)",
            color: "#fff",
            padding: "4px 12px",
            borderRadius: "6px",
          }}
        >
          {content}
        </div>
      )}
    </div>
  );
};

Tooltip.propTypes = {
  content: PropTypes.string.isRequired,
  children: PropTypes.element.isRequired,
};

export default Tooltip;
