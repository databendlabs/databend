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
              to="/doc/about">
                Documentation
            </Link>
            <Link
              className={clsx("button button--secondary button--lg", styles.link)}
              to="/doc/category/performance">
                Performance
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
        title={`Databend - Activate your Object Storage for sub-second analytics`}
        description={`A modern Elasticity and Performance Cloud Data Warehouse, activate your Object Storage for sub-second analytics`}>
        <HomepageHeader/>
      </Layout>
    );
}
