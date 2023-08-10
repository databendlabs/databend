// Copyright 2023 Datafuse Labs.
import React from 'react';
import { FC, ReactElement } from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';
import { ICommonProps } from '@site/src/types';
interface IProps extends ICommonProps{
  href?: string;
  isDownload?: boolean;
  padding?: number[]; 
  onClick?: ()=> void;
}
const Card: FC<IProps> = ({children, padding, className, href, isDownload, style, onClick}): ReactElement=> {
  const props = {
    style:{padding: `${padding[0]}px ${padding[1]}px`, ...style},
    className: clsx(styles.wrap, className)
  }
  return (
    <>
      {
        href
        ? <a onClick={onClick} download={isDownload} href={href} {...props}>{children}</a>
        : <div onClick={onClick} {...props}>{children}</div>
      }
    </>
  );
};
Card.defaultProps = {
  padding: [28, 24],
  isDownload: false
}
export default Card;