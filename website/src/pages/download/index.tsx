// Copyright 2022 Datafuse Labs.
import React, { FC, ReactElement, useState } from 'react';
import Table from 'rc-table';
import styles from './styles.module.scss';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import { useMount } from 'ahooks';
import bytes from 'bytes';
import axios from 'axios';

interface IRow {
  name: string;
  sort?: number;
  tagName?: string;
  osType?: string;
  size: number;
}
const LINUX_GENERIC_X86 = 'Linux Generic(x86, 64-bit)';
const LINUX_GENERIC_ARM = 'Linux Generic(ARM, 64-bit)';
const MAC_X86 = 'macOS (x86, 64-bit)';
const MAC_ARM = 'macOS (ARM, 64-bit)';
const Releases: FC = (): ReactElement=> {
  const DOWNLOAD_LINK = 'https://repo.databend.rs/databend/';
  useMount(()=> {
    getRelease();
  });
  const [releaseData, setReleaseData] = useState<IRow[]>([
    { 
      name: `databend-aarch64-unknown-linux-musl.tar.gz`, 
      tagName: '', 
      osType: LINUX_GENERIC_ARM, 
      size: 0
    },
    { 
      name: `databend-x86_64-unknown-linux-musl.tar.gz`, 
      tagName: '', 
      osType: LINUX_GENERIC_X86, 
      size: 0
    },
    { 
      name: `databend-aarch64-apple-darwin.tar.gz`,
      tagName: '', 
      osType: MAC_ARM, 
      size: 0
    },
    { 
      name: `databend-x86_64-apple-darwin.tar.gz`, 
      tagName: '', 
      osType: MAC_X86, 
      size: 0
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
  async function getRelease() {
    const res = await axios.get(`${DOWNLOAD_LINK}releases.json`); 
    const data = res?.data;
    if(data && data?.length > 0){
      const releaseData = data[0];
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
        return !opName?.includes('linux-gnu') && !opName?.includes('testsuites') && !opName?.includes('sha256sums');
      })
      ?.sort((a, b)=> {
        return a.sort - b.sort;
      })
      setReleaseData(reslut);
    }
  }
  return (
    <Layout 
      title={`Databend - Activate your Object Storage for real-time analytics`}
      description={`A modern Elasticity and Performance Cloud Data Warehouse, activate your Object Storage(S3, Azure Blob, or MinIO) for real-time analytics`}>
      <div className={styles.tableWarp}>
        <div className={styles.table}>
          <>
            <Table showHeader={false} rowKey="name" columns={columns} data={releaseData} />
            <a target={'_blank'} className={styles.prev} href="https://github.com/datafuselabs/databend/releases">Looking for previous GA versions?</a>
          </>
        </div>
      </div>
    </Layout>
  );
};
export default Releases;