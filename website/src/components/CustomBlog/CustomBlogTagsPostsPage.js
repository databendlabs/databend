import React from 'react';
import Layout from '@theme/Layout';
import BlogList from './BlogList';
import clsx from 'clsx';
import styles from './styles.module.scss';
import PopularTags from './PopularTags';
import Link from '@docusaurus/Link';

function CustomBlogTagsPostsPage(props) {

  return (
    <Layout
      title={`Posts tagged as "${props.tag.label}"`}
      description={`Blog posts tagged as "${props.tag.label}"`}
    >
     <section className={clsx('blog-page',styles.blogPage)}>
     <Link to={'/blog'}>
        <h1 className={clsx('blog-page-title',styles.blogPageTitle)}>Blog</h1>
        </Link>
        <PopularTags props={props}/>
        <h3 className={clsx('blog-page-list-title',styles.blogPageListTitle)}>Posts tagged as "# {props.tag.label}"</h3>
        <BlogList metadatas={props}/>
        </section>
    </Layout>
  );
}

export default CustomBlogTagsPostsPage;
