// Copyright 2022 Datafuse Labs.
import React, { FC, ReactElement, useState } from 'react';
import Table from 'rc-table';
import { getLatest } from '@site/src/plugins/releaseVersion';
import styles from './styles.module.scss';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import { useLocalStorageState } from 'ahooks';
import bytes from 'bytes';

interface IRow {
  name: string;
  sort?: number;
  tagName?: string;
  osType?: string;
  size: number;
  browser_download_url?: string;
}
const LINUX_GENERIC_X86 = 'Linux Generic(x86, 64-bit)';
const LINUX_GENERIC_ARM = 'Linux Generic(ARM, 64-bit)';
const MAC_X86 = 'macOS (x86, 64-bit)';
const MAC_ARM = 'macOS (ARM, 64-bit)';
const Releases: FC = (): ReactElement=> {
  const [cacheTagName] = useLocalStorageState<string>('global-cache-tag-name');
  const tagName = cacheTagName as string || 'v0.8.25';
  const DOWNLOAD_LINK = 'https://repo.databend.rs/databend/';
  const [releaseData, setReleaseData] = useState<IRow[]>([
    { 
      name: `databend-${tagName}-aarch64-unknown-linux-musl.tar.gz`, 
      tagName, 
      osType: LINUX_GENERIC_ARM, 
      size: 0,
      browser_download_url: `${DOWNLOAD_LINK}${tagName}/${tagName}-aarch64-unknown-linux-musl.tar.gz`
    },
    { 
      name: `databend-${tagName}-x86_64-unknown-linux-musl.tar.gz`, 
      tagName, 
      osType: LINUX_GENERIC_X86, 
      size: 0, 
      browser_download_url: `${DOWNLOAD_LINK}${tagName}/${tagName}-aarch64-apple-darwin.tar.gz`
    },
    { 
      name: `databend-${tagName}-aarch64-apple-darwin.tar.gz`,
      tagName,
      osType: MAC_ARM, 
      size: 0,
      browser_download_url: `${DOWNLOAD_LINK}${tagName}/${tagName}-aarch64-apple-darwin.tar.gz`
    },
    { 
      name: `databend-${tagName}-x86_64-apple-darwin.tar.gz`, 
      tagName, 
      osType: MAC_X86, 
      size: 0,
      browser_download_url: `${DOWNLOAD_LINK}${tagName}/${tagName}-x86_64-apple-darwin.tar.gz`
    }
  ]);
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
      title: 'Size',
      dataIndex: 'size',
      key: 'size',
      render(size: number) {
        return <div className={styles.tagName}>{size>0 && bytes.format(size, {thousandsSeparator: ',', decimalPlaces: 1})}</div>
      }
    },
    {
      title: 'download',
      dataIndex: 'osType',
      key: 'osType',
      render(o: string, record: IRow) {
        const {tagName, name} = record;
        return <a className={clsx('button button--secondary', styles.download)} href={`${DOWNLOAD_LINK}${tagName}/${name}`}>Download</a>
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
              getLatest((releaseData: {assets: IRow[], tag_name: string})=> {
                const { assets, tag_name } = releaseData || {};
                const reslut = assets
                ?.filter((item)=> {
                  item.tagName = tag_name;
                  if (item?.name?.includes('-apple-')) {
                    item.sort = 1;
                    if (item?.name?.includes('x86')) {
                      item.osType = MAC_X86;
                    } else {
                      item.osType = MAC_ARM;
                    }
                  } else {
                    item.sort = 0;
                    if (item?.name?.includes('x86')) {
                      item.osType = LINUX_GENERIC_X86;
                    } else {
                      item.osType = LINUX_GENERIC_ARM;
                    }
                  }
                  const opName = item.name;
                  return !opName?.includes('linux-gnu') && !opName?.includes('testsuites');
                })
                ?.sort((a, b)=> {
                  return a.sort - b.sort;
                })
                setReleaseData(reslut);
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