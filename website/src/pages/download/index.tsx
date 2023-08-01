// Copyright 2023 Datafuse Labs.
import React, { FC, ReactElement } from 'react';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm'
import { timeFormatAgo } from '@site/src/utils/tools';
import styles from './styles.module.scss';
import useGetReleases from '@site/src/hooks/useGetReleases';
import Card from '@site/src/components/BaseComponents/Card';
import Tag from '@site/src/components/BaseComponents/Tag';
import { Apple, Linux } from '@site/src/components/Icons';
import Ubuntu from '@site/src/components/Icons/Ubuntu';

const Releases: FC = (): ReactElement => {
  const { 
    releasesList, 
    tagName
  } = useGetReleases();
  const { filterBody, assets: latestAssets, published_at, prerelease} = releasesList[0];
  function Icons({isApple, size = 24, isUbuntu}): ReactElement {
    return (
      <>
        {
          isApple
          ? <Apple size={size}/>
          : <>
            {
              isUbuntu
              ? <Ubuntu size={size}></Ubuntu>
              : <Linux size={size}/>
            }
            </>
        }
      </>
    )
  }
  return (
    <Layout
      title={`Databend - Activate your Object Storage for real-time analytics`}
      description={`A modern Elasticity and Performance Cloud Data Warehouse, activate your Object Storage(S3, Azure Blob, or MinIO) for real-time analytics`}>
      <div className={styles.wholePage}>
        <div className={styles.download}>Download</div>
        <div className={styles.latest}>Latest Version: {tagName}</div>
        <Card className={styles.latestBlock}>
          <div className={styles.latestVersion}>
            <div className={styles.topTag}>
              <span className={styles.version}>{tagName}</span>
              <Tag>Latest</Tag>
            </div>
            <div className={styles.updateTime}>{timeFormatAgo(published_at)}</div>
            <div className={styles.nowAssets}>
              {
                latestAssets?.map((asset, index)=> {
                  const { isApple, browser_download_url, osTypeDesc, formatSize, isUbuntu } = asset;
                  return (
                    <Card 
                      href={browser_download_url}
                      isDownload 
                      key={index} 
                      className={clsx(styles.nowItem, !isApple && styles.nowItemLinux, isUbuntu && styles.nowItemLinuxUbuntu)} 
                      padding={[8, 16]}>
                      <Icons isApple={isApple} isUbuntu={isUbuntu}></Icons>
                      <div className={styles.right}>
                        <div>{osTypeDesc}</div>
                        <div>Size: {formatSize}</div>
                      </div>
                    </Card>
                  )
                })
              }
            </div>
          </div>
          <div className={styles.submitRecord}>
            <ReactMarkdown remarkPlugins={[remarkGfm]}>{filterBody}</ReactMarkdown>
          </div>
        </Card>
        <div className={styles.historyArea}>
          <div className={styles.historyTitle}>
            <div>History Versions</div>
            <div>This page only displays the most recent 20 versions. For earlier versions, please refer to <a target='_blank' href='https://github.com/datafuselabs/databend/releases'>GitHub</a>.</div>
          </div>
          <div className={styles.listWrap}>
            {
              releasesList.slice(1)?.map((release, index)=> {
                return (
                  <Card 
                    key={index}
                    className={styles.listItem} padding={[12, 24]}>
                    <div>
                      <div className={styles.leftDesc}>
                        <div className={styles.tagName}>{release?.tag_name}</div>
                      </div>
                      <div>{timeFormatAgo(release?.published_at)}</div>
                    </div>
                    <div className={styles.downArea}>
                      {
                        release.assets?.map((asset, ind)=> {
                          const { isApple, browser_download_url, osType, isUbuntu} =  asset;
                          return (
                            <Card  
                              key={ind}
                              href={browser_download_url}
                              className={clsx(styles.button, !isApple && styles.buttonLinux, isUbuntu && styles.buttonLinuxUbuntu)} 
                              padding={[5, 12]}>
                              <Icons size={12} isApple={isApple} isUbuntu={isUbuntu}></Icons>
                              <span>{osType}</span>
                            </Card>
                          )
                        })
                      }
                    </div>
                  </Card>
                )
              }) 
            }
          </div>
        </div>
      </div>
    </Layout >
  );
};
export default Releases;
