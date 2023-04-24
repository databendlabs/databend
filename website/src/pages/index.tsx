import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.scss';
import * as icons from "../components/Icons"
import useGetReleases from '@site/src/hooks/useGetReleases';
import JoinCommunity from '../components/JoinCommunity';

function HomepageHeader() {
    const {siteConfig} = useDocusaurusContext();
    const { GitHub,Getstart,Book } = icons;
    const { formatStargazersCount } = useGetReleases();
    const community = [
      {
        'icon':'GitHub',
        'star': formatStargazersCount,
        'title': 'GitHub',
        'link': 'https://github.com/datafuselabs/databend'
      },
      {
        'icon':'Slack',
        'title': 'Slack',
        'link': 'https://link.databend.rs/join-slack'
      },
      {
        'icon':'Twitter',
        'title': 'Twitter',
        'link': 'https://twitter.com/DatabendLabs'
      },
      {
        'icon':'YouTube',
        'title': 'YouTube',
        'link': 'https://www.youtube.com/@DatabendLabs'
      },
    ]
    
    return (
      <div className={clsx('home-page', styles.homePage)}>
        <section className={clsx(styles.heroBanner, styles.bannerItemHeight)}>
          <div className={clsx('hero-container', styles.heroContainer)}>
            <GitHub size={48} color='var(--color-text-0)'/>
            <h2 className={clsx('title', styles.title)}>The Future of <br/> <span>Cloud Data Analytics</span></h2>
            <p className={clsx('subtitle', styles.subtitle)}>{siteConfig.tagline}</p>
            <div className={clsx('action-group', styles.actionGroup)}>
            <Link
              className={clsx("button", styles.Button,styles.Primary)}
              to="/doc/">
                <Book size={20}/>
                What is Databend
            </Link>
              <Link
                  className={clsx("button", styles.Button)}
                  to="/doc/guides/">
                  <Getstart size={20}/>
                  Tutorials
              </Link>
            </div>
            <JoinCommunity />
            <hr/>
            <div className={clsx('cloud-banner', styles.cloudBanner)}>
              <div style={{textAlign:'center'}}>
              <h5>🎉 Databend Cloud Now Available</h5>
              <p>Register now and get a $200 coupon.</p>
              </div>
              <Link
              className={clsx("button", styles.Button, styles.White)}
              to="/doc/cloud">
               🚀 Start your trial
            </Link>
            </div>
          </div>
        </section>
      </div>
    );
}

export default function Home(): JSX.Element {
    const {siteConfig} = useDocusaurusContext();
    return (
      <Layout
        title={`Databend - The Future of Cloud Data Analytics.`}
        description={`The modern cloud data warehouse that empowers your object storage(S3, Azure Blob, or MinIO) for real-time analytics`}>
        <HomepageHeader/>
      </Layout>
    );
}
