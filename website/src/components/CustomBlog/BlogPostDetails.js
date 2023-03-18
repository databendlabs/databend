import React from "react";
import styles from "./styles.module.scss";
import clsx from "clsx";
import Layout from "@theme/Layout";
import { ArrowLeft, ArrowRight, Return, Pencil} from "../Icons";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import AvatarGroup from "../BaseComponents/AvatarGroup";
import Connectwithus from "./Connectwithus";

const BlogPostNav = ({ nextPost, prevPost }) => {
  return (
    <div className={styles.PostNav}>
      {prevPost && (
        <Link
          to={prevPost.permalink}
          className={clsx(styles.navLink, styles.prev)}>
          <p><ArrowLeft size={16}/>Previous</p>
          <span>{prevPost.title}</span>
        </Link>
      )}
      {nextPost && (
        <Link
          to={nextPost.permalink}
          className={clsx(styles.navLink, styles.next)}>
          <p>Next <ArrowRight size={16}/></p>
          <span>{nextPost.title}</span>
        </Link>
      )}
    </div>
  );
}

const BlogPostDetails = (props) => {
  const metadata = props.content.metadata;
  const BlogPostContent = props.content;
  return (
    <Layout title={`${metadata.title} | Blog`}>
      <section className={clsx("blog-page", styles.blogPage, styles.blogPost)}>
        <div className={clsx("post-content", styles.postContent)}>
          <div className={clsx("content-container", styles.ContentContainer)}>
            <div
              className={clsx("post-content-title", styles.postContentTitle)}
            >
              <Link
                to="/blog"
                className={clsx(
                  "post-content-title-return",
                  styles.postContentTitleReturn
                )}
              >
              <Return size={16} />
              Return
              </Link>
              <h1>{metadata.title}</h1>
              <p className={clsx(styles.Date)}>
                <p>{metadata.formattedDate} · {Math.round(metadata.readingTime)} min read </p>
              </p>
              <a
                href={metadata.authors[0].url}
                className={clsx(styles.blogAuthors)}
              >
                <img
                  src={metadata.authors[0].image_url}
                  width="40px"
                  height="40px"
                ></img>
                <h6>{metadata.authors[0].name}</h6>
              </a>
              <p>{metadata.description}</p>
              {metadata.frontMatter.cover_url ? (
                <img
                  className={clsx(styles.CoverImg)}
                  src={useBaseUrl(
                    `/img/blog/${metadata.frontMatter.cover_url}`
                  )}
                />
              ) : (
                ""
              )}
              <hr></hr>
            </div>
            <div className={clsx("content", styles.Content)}>
              <BlogPostContent />
              {metadata.frontMatter.contributors ? (
                <>
                  <hr />
                  <h2>
                    🎉 Contributors{" "}
                    <div className={styles.AvatarCount}>
                      <span>
                        {metadata.frontMatter.contributors.length} contributors
                      </span>
                    </div>
                  </h2>
                  <p>
                    Thanks a lot to the contributors for their excellent work
                    this week.
                  </p>
                  <AvatarGroup
                    contributors={metadata.frontMatter.contributors}
                  />
                </>
              ) : (
                <></>
              )}
              <Connectwithus/>
              <div className={styles.tagsBox}>
              <h5>Tags:</h5>
              {metadata.tags.map((tag,index) => {
                  return (
                    <a href={tag.permalink} className={clsx(styles.Tag)} key={index}>
                      #{tag.label}
                    </a>
                  );
                })}
              </div>
              <div className={styles.tagsBox}>
              <h5><a href={metadata.editUrl}><Pencil size={20}/>Edit this page</a></h5>
              </div>
              <hr/>
               <BlogPostNav prevPost={metadata.prevItem} nextPost={metadata.nextItem}/>
            </div>
          </div>
        </div>
      </section>
    </Layout>
  );
};

export default BlogPostDetails;
