import React from "react";
import styles from "./styles.module.scss";
import clsx from "clsx";
import { GitHub, Twitter, Slack } from "../Icons";
import Link from "@docusaurus/Link";

const Connectwithus = () => {
  const items = [
    {
      name: "GitHub Discussions",
      icon: <GitHub />,
      des: "Feature requests, bug reports, and contributions",
      link: "https://github.com/datafuselabs/databend/discussions",
    },
    {
      name: "Follow us on Twitter",
      icon: <Twitter />,
      des: "Stay in the know",
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
        Databend is a cutting-edge, open-source cloud-native warehouse built with Rust,
        designed to handle massive-scale analytics.
      </p>
      <p>
        Join the <a href="https://github.com/datafuselabs/databend">Databend Community</a> to try, get help, and contribute!
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
