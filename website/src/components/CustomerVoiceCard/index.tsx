// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement } from 'react';
import styles from './styles.module.scss';
import Avatar from '@site/static/img/avatar/default-avatar.jpeg';
import clsx from 'clsx';
interface IProps {
  name: string;
  position: string;
  description: string;
  date: string;
  avatar?: string;
  linkForDate?: string;
}
const CustomerVoiceCard: FC<IProps> = ({ name, position, description, date, avatar, linkForDate }): ReactElement=> {
  return (
    <div className={styles.card}>
      <div className={styles.top}>
        <a><img className={styles.avatar} src={avatar}/></a>
        <div style={{flex: 1}}>
          <div className={styles.name}>{name}</div>
          <div className={styles.position}>{position}</div>
        </div>
      </div>
      <div className={styles.description}>
        {description}
      </div>
      {
        linkForDate
        ? <a className={clsx(styles.date, styles.linkDate)} target='_blank' href={linkForDate}>{date}</a>
        : <div className={styles.date}>{date}</div>
      }
      
    </div>
  );
};
CustomerVoiceCard.defaultProps = {
  avatar: Avatar
}
export default CustomerVoiceCard;