// Copyright 2023 Datafuse Labs.
import React from 'react';
import { FC, ReactElement, ReactNode } from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';
interface IProps {
  children: ReactNode;
  padding?: number[];
  className?: string;
  href?: string;
  isDownload?: boolean;
  style?: React.CSSProperties;
}
const Card: FC<IProps> = ({children, padding, className, href, isDownload, style}): ReactElement=> {
  const props = {
    style:{padding: `${padding[0]}px ${padding[1]}px`, ...style},
    className: clsx(styles.wrap, className)
  }
  return (
    <>
      {
        href
        ? <a download={isDownload} href={href} {...props}>{children}</a>
        : <div {...props}>{children}</div>
      }
    </>
  );
};
Card.defaultProps = {
  padding: [28, 24],
  isDownload: false
}
export default Card;