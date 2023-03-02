import React from "react";
import PropTypes from "prop-types";
import Tooltip from "../Tooltip";
import styles from "./index.module.scss";

const Avatar = ({ name, url, image_url }) => {
  return (
    <Tooltip content={name}>
      <a href={url} className={styles.Avatar}>
        <img src={`${image_url}`} width="100%" />
      </a>
    </Tooltip>
  );
};

const AvatarGroup = ({ contributors }) => {
  return (
    <>
      <div className={styles.AvatarGroup}>
        {contributors.map(({ name, url, image_url }, index) => (
          <Avatar key={index} name={name} url={url} image_url={image_url} />
        ))}
      </div>
    </>
  );
};

AvatarGroup.propTypes = {
  contributors: PropTypes.arrayOf(
    PropTypes.shape({
      name: PropTypes.string.isRequired,
      url: PropTypes.string.isRequired,
      image_url: PropTypes.string.isRequired,
    })
  ).isRequired,
};

export default AvatarGroup;
