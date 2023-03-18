// Copyright 2023 Datafuse Labs.
import { FC, ReactElement } from 'react';
import styles from './styles.module.scss';
import React from 'react';
import clsx from 'clsx';
import { ICommonProps } from '@site/src/types';

const Tag: FC<ICommonProps> = ({children, className}): ReactElement=> {
  return (
  <span className={clsx(styles.tag, className)}>{children}</span>
  );
};
export default Tag;