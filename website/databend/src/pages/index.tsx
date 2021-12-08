import React from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import styles from './index.module.css';

function HomepageHeader() {
    const {siteConfig} = useDocusaurusContext();
    return (
        <header className={clsx('hero hero--primary', styles.heroBanner)}>
            <div className="container">
                <h1 className="hero__title">{siteConfig.title}</h1>
                <p className="hero__subtitle">{siteConfig.tagline}</p>
                <Link
                    className="button button--secondary button--lg"
                    to="/user/index">
                    Quick start
                </Link>
                <Link
                    className="button button--secondary button--lg"
                    to="/overview/performance">
                    Performance
                </Link>
            </div>
        </header>
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
