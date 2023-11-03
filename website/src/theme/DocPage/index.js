import React from 'react';
import DocPage from '@theme-original/DocPage';
import styles from './styles.module.scss';

export default function DocPageWrapper(props) {
  return (
    <>
      <div className={styles.fakeBg}></div>
      <div className={styles.cell}></div>
      <DocPage {...props} />
    </>
  );
}
