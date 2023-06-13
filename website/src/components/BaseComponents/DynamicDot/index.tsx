import React, { FC, memo, ReactElement } from 'react';
import styles from './index.module.scss';
const DynamicDot: FC= (): ReactElement => {
  return (
    <span className={styles.dot}>. . .</span> 
  );
};
export default memo(DynamicDot);
