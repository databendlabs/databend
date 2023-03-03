import React from "react";
import styles from "./styles.module.scss";
import clsx from "clsx";
import { Github, Twitter, Slack } from "../Icons";
import Link from "@docusaurus/Link";

const Connectwithus = () => {
  const items = [
    {
      name: "GitHub Discussions",
      icon: <Github />,
      des: "Feature requests, bug reports, and contributions",
      link: "https://github.com/datafuselabs/databend/discussions",
    },
    {
      name: "Follow our twitter",
      icon: <Twitter />,
      des: "Feature requests, bug reports, and contributions",
      link: "https://twitter.com/DatabendLabs",
    },
    {
      name: "Slack Channel",
      icon: <Slack />,
      des: "Chat with the community",
      link: "https://link.databend.rs/join-slack",
    },
  ];

  return (
    <div className={clsx(styles.Connectwithus)}>
      <h2>🎈Connect With Us</h2>
      <p>
        We'd love to hear from you. Feel free to run the code and see if
        Databend works for you. Submit an issue with your problem if you need
        help.
      </p>
      <p>
        <a href="https://github.com/datafuselabs/">DatafuseLabs Community</a> is
        open to everyone who loves data warehouses. Please join the community
        and share your thoughts.
      </p>
      <ul className={clsx(styles.Connecttype)}>
        {items.map((item, index) => {
          return (
            <li key={index}>
              <Link to={item.link}>
              {item.icon}
              <h6>{item.name}</h6>
              <p>{item.des}</p>
              </Link>
            </li>
          );
        })}
      </ul>
    </div>
  );
};

export default Connectwithus;
