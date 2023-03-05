import React from 'react';
import Layout from '@theme/Layout';
import styles from './styles.module.scss';
import clsx from 'clsx';
import BlogList from './BlogList';
import PopularTags from './PopularTags';
import Link from '@docusaurus/Link';
import Tooltip from '../BaseComponents/Tooltip';

function CustomBlogListPage(props) {

  return (
    <Layout>
      <section className={clsx('blog-page',styles.blogPage)}>
        <Link to={'/blog'}>
        <h1 className={clsx('blog-page-title',styles.blogPageTitle)}>Blog</h1>
        </Link>
       <PopularTags/>
       <h3 className={clsx('blog-page-list-title',styles.blogPageListTitle)}>Blog Posts</h3>
       <BlogList metadatas={props}/>
       </section>
    </Layout>
     
  );
}

export default CustomBlogListPage;
