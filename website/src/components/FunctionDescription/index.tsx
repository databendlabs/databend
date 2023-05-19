// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement, useEffect } from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';
import Tag from '@site/src/components/BaseComponents/Tag';
interface IProps {
  description: string;
  includesEETip?: boolean;
}

const FunctionDescription: FC<IProps> = ({ description, includesEETip }): ReactElement=> {
  useEffect(() => {
    const h1 = document?.querySelector('.theme-doc-markdown')?.querySelector('header')?.firstChild as HTMLElement;
    if (h1) {
      h1?.classList?.add('DOCITEM-PAGE-FUNCTION-DESCRIPTION-DOM');
    }
  }, [])
  return (
    <div className={clsx(styles.description, includesEETip && styles.descriptionIncludesEE)}>
      <Tag>{description}</Tag>
    </div>
  );
};
FunctionDescription.defaultProps = {
  includesEETip: false
}

export default FunctionDescription;