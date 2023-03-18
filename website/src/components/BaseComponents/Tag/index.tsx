// Copyright 2023 Datafuse Labs.
import { FC, ReactElement, ReactNode } from 'react';
import styles from './styles.module.scss';
import React from 'react';
import clsx from 'clsx';
interface IProps {
  children: ReactNode;
  className?: string;
}
const Tag: FC<IProps> = ({children, className}): ReactElement=> {
  return (
  <span className={clsx(styles.tag, className)}>{children}</span>
  );
};
export default Tag;