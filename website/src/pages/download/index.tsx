// Copyright 2022 Datafuse Labs.
import React, { FC, ReactElement, useState } from 'react';
import Table from 'rc-table';
import { getLatest } from '@site/src/plugins/releaseVersion';
import styles from './styles.module.scss';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import clsx from 'clsx';
interface IRow {
  name: string;
  tagName: string;
  osType: string;
}
const Releases: FC = (): ReactElement=> {
  const [releaseData, setReleaseData] = useState<IRow[]>([]);
  const DOWN_LINK = 'https://github.com/datafuselabs/databend/releases/download';
  const columns = [
    {
      title: 'Arch',
      dataIndex: 'name',
      key: 'name',
      render(name:string, record: IRow){
        const {tagName, osType} = record;
        return <div>
          <div className={styles.osType}>{osType}</div>
          <div className={styles.name}>{name}</div> 
          {/* <div  href={``}>{name}</div> */}
        </div>
      }
    },
    {
      title: 'tagName',
      dataIndex: 'tagName',
      key: 'tagName',
      render(tagName:string) {
        return <div className={styles.tagName}>{tagName}</div>
      }
    },
    {
      title: 'download',
      dataIndex: 'osType',
      key: 'osType',
      render(osType: string, record: IRow) {
        const {tagName, name} = record;
        return <a className={clsx('button button--secondary', styles.download)} href={`${DOWN_LINK}/${tagName}/${name}`}>Download</a>
      }
    }
  ];
  return (
    <Layout 
      title={`Databend - Activate your Object Storage for real-time analytics`}
      description={`A modern Elasticity and Performance Cloud Data Warehouse, activate your Object Storage(S3, Azure Blob, or MinIO) for real-time analytics`}>
      <div className={styles.tableWarp}>
        <div className={styles.table}>
          <>
            {
              getLatest((tagName: string)=> {
                const data = [
                  { name: `databend-${tagName}-aarch64-apple-darwin.tar.gz`, tagName, osType: 'macOS (ARM, 64-bit)'},
                  { name: `databend-${tagName}-aarch64-unknown-linux-gnu.tar.gz`, tagName, osType:'Ubuntu Linux (ARM, 64-bit)'},
                  { name: `databend-${tagName}-aarch64-unknown-linux-musl.tar.gz`, tagName, osType: 'Linux Generic(ARM, 64-bit)'},
                  { name: `databend-${tagName}-x86_64-apple-darwin.tar.gz`, tagName, osType: 'macOS (x86, 64-bit)'},
                  { name: `databend-${tagName}-x86_64-unknown-linux-gnu.tar.gz`, tagName, osType: 'Ubuntu Linux (x86, 64-bit)'},
                  { name: `databend-${tagName}-x86_64-unknown-linux-musl.tar.gz`, tagName, osType: 'Linux Generic(x86, 64-bit)'},
                ];
                setReleaseData(data);
              })
            }
            <Table showHeader={false} rowKey="name" columns={columns} data={releaseData} />
            <a target={'_blank'} className={styles.prev} href="https://github.com/datafuselabs/databend/releases">Looking for previous GA versions?</a>
          </>
        </div>
      </div>
    </Layout>
  );
};
export default Releases;