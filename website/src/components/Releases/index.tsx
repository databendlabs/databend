// Copyright 2022 Datafuse Labs.
import { FC, ReactElement, useState } from 'react';
import Tooltip from 'rc-tooltip';
import Table from 'rc-table';
import 'rc-tooltip/assets/bootstrap.css';
import { getLatest } from '@site/src/plugins/releaseVersion';
import styles from './styles.module.scss';
import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
interface IRow {
  name: string;
  tagName: string;
}
const Releases: FC = (): ReactElement=> {
  const [releaseData, setReleaseData] = useState<IRow[]>([]);
  const DOWN_LINK = 'https://github.com/datafuselabs/databend/releases/download';
  const columns = [
    {
      title: 'Assets',
      dataIndex: 'name',
      key: 'name',
      render(name:string, record: IRow){
        const tagName = record?.tagName;
        return <a download href={`${DOWN_LINK}/${tagName}/${name}`}>{name}</a>
      }
    },
  ];
  return (
    <>
      {
      getLatest((tagName: string)=> {
        const data = [
          { name: `databend-${tagName}-aarch64-apple-darwin.tar.gz`, tagName},
          { name: `databend-${tagName}-aarch64-unknown-linux-gnu.tar.gz`, tagName},
          { name: `databend-${tagName}-aarch64-unknown-linux-musl.tar.gz`, tagName},
          { name: `databend-${tagName}-x86_64-apple-darwin.tar.gz`, tagName},
          { name: `databend-${tagName}-x86_64-unknown-linux-gnu.tar.gz`, tagName},
          { name: `databend-${tagName}-x86_64-unknown-linux-musl.tar.gz`, tagName},
        ];
        setReleaseData(data);
        })
      }
     <Tooltip
      showArrow={false}
      overlayClassName={styles.toolTip}
      placement="right" 
      trigger={['hover']} 
      overlay={<Table rowKey="name" columns={columns} data={releaseData} />}>
      <Link className={clsx("button button--secondary button--lg", styles.link)}>Releases</Link>
    </Tooltip>
  </>
  );
};
export default Releases;