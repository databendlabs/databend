// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement } from 'react';
import styles from './styles.module.scss';
import { Close } from '../Icons';
import { useSessionStorageState } from 'ahooks';
const TryCloudCard: FC = (): ReactElement=> {
  const [hidden, setHiddenFlag] = useSessionStorageState('DATABEND_TOC_CARD', {
    defaultValue: ''
  });
  const closeCard = ()=> {
    setHiddenFlag('closed');
  }
  return (
    <>
      {
        !hidden &&
        <div className={styles.card}>
          <div className={styles.header}>
            <h6>Try Databend Cloud for FREE</h6>
            <span onClick={closeCard} className={styles.close}><Close size={20}/></span>
          </div>
          <div className={styles.desc}>
            Low-cost, fast analytics, easy data ingestion, and elastic scaling. 
          </div>
          <a href="https://www.databend.com/apply/?r=doc-card" className={styles.button}>Try it today</a>
        </div>
      }
    </>
  );
};
export default TryCloudCard;