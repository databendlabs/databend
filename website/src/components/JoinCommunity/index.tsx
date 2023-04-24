// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement } from 'react';
import Link from '@docusaurus/Link';
import * as icons from "../Icons"
import useGetReleases from '@site/src/hooks/useGetReleases';
import clsx from 'clsx';
import styles from './styles.module.scss';
interface TProps {
  titleAlign?: 'start' | 'end' | 'left' | 'right' | 'center' | 'justify' | 'match-parent';
  maxWidth?: number;
  justifyContent?: 'center' | 'flex-start' | 'flex-end';
}

const JoinCommunity:  FC<TProps> = ({titleAlign, maxWidth, justifyContent}): ReactElement=> {
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
    <div className={clsx('community', styles.Community)}>
      <h6 style={{textAlign: titleAlign}}>Join our growing community</h6>
      <div className={clsx('community-group', styles.CommunityGroup)} style={{maxWidth: maxWidth+'px', justifyContent}}>
        {community.map((item,index)=>{
          const Icon = icons[item.icon]
          return <Link to={item.link} key={index}>
            <div className={clsx('community-item', styles.communityItem)}><div className={clsx('icon', styles.Icon)}><Icon size={24}/></div><h6>{item.title}</h6>{item.star?<span className={clsx('tag', styles.tag)}>🌟 {item.star} Stars</span>:''}</div>
          </Link>
        })}
      </div>
  </div>
  );
};
JoinCommunity.defaultProps = {
  titleAlign: 'center',
  maxWidth: 720,
  justifyContent: 'center'
}
export default JoinCommunity;