import React from "react";
import styles from "./styles.module.scss";
import clsx from "clsx";
import WeeklyCover from "./WeeklyCover";
import DefaultCover from "./DefaultCover";

const BlogList = (metadatas) => {
  const items = metadatas.metadatas.items.map((item) => {
    const metadata = item.content.metadata;
    function innerTagAction(e, permalink) {
      e.stopPropagation();
      e.preventDefault();
      window.open(permalink, '_self')
    }
    return (
      <li
        className={clsx("post-list-item", styles.postListItem)}
        key={metadata.permalink}
      >
        <a href={metadata.permalink}>
          {metadata.frontMatter.cover_url ? (
            <img
              src={
                require(`/img/blog/${metadata.frontMatter.cover_url}`).default
              }
            ></img>
          ) : metadata.tags.length > 0 &&
            metadata.tags.some((item) => item.label === "weekly") ? (
            <WeeklyCover title={metadata.title} />
          ) : (
            <DefaultCover title={metadata.title} />
          )}
          <div className={clsx("tags", styles.Tags)}>
            {metadata.tags.map((tag, index) => {
              return (
                <span className={clsx("tag", styles.Tag)} key={index}>
                  <span onClick={(e)=> innerTagAction(e, tag.permalink)}># {tag.label}</span>
                </span>
              );
            })}
          </div>
          <div
            className={clsx("post-list-item-title", styles.postListItemTitle)}
          >
            <h4>{metadata.title}</h4>
            <p
              className={clsx(
                "post-list-item-description",
                styles.postListDesc
              )}
            >
              {metadata.description}
            </p>
          </div>
          <p
            className={clsx("post-list-item-description", styles.postListDesc)}
          >
            By{" "}
            <span onClick={(e)=> innerTagAction(e, metadata.authors[0].url)}>
              <strong>{metadata.authors[0].name}</strong>
            </span>{" "}
            on {metadata.formattedDate}
          </p>
        </a>
      </li>
    );
  });
  return <ul className={clsx("post-list", styles.postList)}>{items}</ul>;
};

export default BlogList;
