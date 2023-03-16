import React from "react";
import PropTypes from "prop-types";
import Tooltip from "../Tooltip";
import styles from "./index.module.scss";

const Avatar = ({ name }) => {
  let url = "http://github.com/" + name;
  let image_url = "http://github.com/" + name + ".png";
  if (name == "mergify[bot]") {
    url = "http://github.com/app/mergify[bot]";
    image_url = "https://avatars.githubusercontent.com/in/10562";
  }
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
        {contributors.map(({ name }, index) => (
          <Avatar key={index} name={name} />
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
