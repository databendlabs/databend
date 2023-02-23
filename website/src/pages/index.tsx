import React, { useState, useEffect } from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.scss';
import * as icons from "../components/Icons"

const community = [
  {
    'icon':'Github',
    'star': '5.3k',
    'title': 'Github',
    'link': 'https://github.com/datafuselabs/databend'
  },
  {
    'icon':'Slack',
    'title': 'Join us slack chanel',
    'link': 'https://link.databend.rs/join-slack'
  },
  {
    'icon':'Twitter',
    'title': 'Follow our twitter',
    'link': 'https://twitter.com/Datafuse_Labs'
  },
  {
    'icon':'Youtube',
    'title': 'Youtube',
    'link': 'https://www.youtube.com/@databend8089'
  },
]
function GithubStars({ repo }) {
  const [stars, setStars] = useState(null);
  function convertToK(number) {
    if (number < 1000) {
      return number.toString();
    }
    const k = number / 1000;
    return k.toFixed(k < 10 ? 1 : 0) + " K";
  }
  useEffect(() => {
    async function fetchStars() {
      const url = `https://api.github.com/repos/${repo}`;
      const response = await fetch(url);
      const data = await response.json();
      setStars(data.stargazers_count);
    }
    fetchStars();
  }, [repo]);

  return (
    <div>
      {stars !== null ? (
        <span>🌟 {convertToK(stars)} stars</span>
      ) : (
        <span>Loading...</span>
      )}
    </div>
  );
}

function HomepageHeader() {
    const {siteConfig} = useDocusaurusContext();
    const { Github,Getstart,Book } = icons
    
    return (
      <>
        <section className={clsx(styles.heroBanner, styles.bannerItemHeight)}>
          <div className={clsx('hero-container', styles.heroContainer)}>
            <Github size={48} color='var(--color-text-0)'/>
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
                  Open source quickstart
              </Link>
            </div>
            <hr/>
            <div className={clsx('cloud-banner', styles.cloudBanner)}>
              <div style={{textAlign:'center'}}>
              <h5>🎉 Databend Cloud is now available.</h5>
              <p>No Operations Required, Ready-to-Use. Register Now and Get $200 Discount.</p>
              </div>
              <Link
              className={clsx("button", styles.Button, styles.White)}
              to="/doc/cloud">
               🚀 Start your trial
            </Link>
            </div>
            <div className={clsx('community', styles.Community)}>
              <h6>Join our growing community</h6>
              <div className={clsx('community-group', styles.CommunityGroup)}>
                {community.map((item,index)=>{
                  const Icon = icons[item.icon]
                  return <Link to={item.link}>
                    <div className={clsx('community-item', styles.communityItem)}><div className={clsx('icon', styles.Icon)}><Icon size={24}/></div><h6>{item.title}</h6>{item.star?<span className={clsx('tag', styles.tag)}><GithubStars repo={'datafuselabs/databend'} />
</span>:''}</div>
                  </Link>
                })}
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
        title={`Databend - The Future of Cloud Data Analytics.`}
        description={`The modern cloud data warehouse that empowers your object storage(S3, Azure Blob, or MinIO) for real-time analytics`}>
        <HomepageHeader/>
      </Layout>
    );
}
