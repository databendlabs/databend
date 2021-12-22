import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Texty from 'rc-texty';
import 'rc-texty/assets/index.css';
import styles from './index.module.scss';

function HomepageHeader() {
    const {siteConfig} = useDocusaurusContext();
    return (
      <>
        <section className={clsx('hero hero--primary', styles.heroBanner, styles.bannerItemHeight)}>
          <div className={clsx('container', styles.container)}>
            <h1 className={clsx('hero__title', styles.heroTitle)}><Texty>{siteConfig.title}</Texty></h1>
            <p className={clsx('hero__subtitle', styles.heroSubTitle)}>{siteConfig.tagline}</p>
            <Link
              className={clsx("button button--secondary button--lg", styles.link)}
              to="/user">
              Quick Start
            </Link>
            <Link
              className={clsx("button button--secondary button--lg", styles.link)}
              to="/overview/architecture">
              Architecture
            </Link>
            {/* <ArrowDownSvg className={styles.arrow}></ArrowDownSvg> */}
          </div>
        </section>
      </>
    );
}

export default function Home(): JSX.Element {
    const {siteConfig} = useDocusaurusContext();
    return (
      <Layout
        title={`Databend - A Modern Real-Time Data Processing & Analytics DBMS with Cloud-Native Architecture`}
        description={`A Modern Real-Time Data Processing & Analytics DBMS with Cloud-Native Architecture`}>
        <HomepageHeader/>
      </Layout>
    );
}
