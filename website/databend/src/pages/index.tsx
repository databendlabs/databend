import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import ArrowDownSvg from '@site/static/img/home/arow_down.svg';
import ArchitectureImgSvg from '@site/static/img/home/architecture.svg';
import ArchitecturePhoneImgSvg from '@site/static/img/home/architecturePhone.svg';
import styles from './index.module.scss';

function HomepageHeader() {
    const {siteConfig} = useDocusaurusContext();
    return (
      <>
        <section className={clsx('hero hero--primary', styles.heroBanner, styles.bannerItemHeight)}>
          <div className={clsx('container', styles.container)}>
            <h1 className={clsx('hero__title', styles.heroTitle)}>{siteConfig.title}</h1>
            <p className={clsx('hero__subtitle', styles.heroSubTitle)}>{siteConfig.tagline}</p>
            <Link
              className={clsx("button button--secondary button--lg", styles.link)}
              to="/user/index">
              Quick start
            </Link>
            <Link
              className={clsx("button button--secondary button--lg", styles.link)}
              to="/overview/performance">
              Performance
            </Link>
            <ArrowDownSvg className={styles.arrow}></ArrowDownSvg>
          </div>
        </section>
        <section className={clsx(styles.bannerItemHeight, styles.section)}>
            <div className={clsx('container', styles.homeContainer)}>
              <h1 className="hero__title">How it works</h1>
              <div className={styles.architecture}>
                <div data-pc>
                  <ArchitectureImgSvg className={styles.architectureSvg}></ArchitectureImgSvg>
                </div>
                <div data-phone>
                  <ArchitecturePhoneImgSvg></ArchitecturePhoneImgSvg>
                </div>
                <div className={styles.productFeature}>
                  <div className={styles.description}>
                    <ul>
                      <li> An elastic and reliable Cloud Data Warehouse</li>
                      <li> Offers Blazing Fast Query and combines Elasticity, Simplicity</li>
                      <li> Low cost of the Cloud</li>
                      <li> Built to make the Data Cloud easy</li>
                    </ul>
                  </div>
                </div>
              </div>
            </div>
        </section>
      </>
    );
}

export default function Home(): JSX.Element {
    const {siteConfig} = useDocusaurusContext();
    return (
      <Layout
        title={`Hello from ${siteConfig.title}`}
        description={`${siteConfig.title} - ${siteConfig.tagline}`}>
        <HomepageHeader/>
      </Layout>
    );
}
