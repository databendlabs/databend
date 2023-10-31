// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement } from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';
import EnterPriseSvg from '../../icons/enterprise.svg';
interface IProps {
  list?: string[];
}
const DatabendTags: FC<IProps> = ({list}): ReactElement=> {
return (
  <div className={styles.wrap}>
    {
      list?.map((item) =>{
        const isEnterprise = item === 'Enterprise';
        return (
          <span key={item} className={clsx(styles.plain, isEnterprise && styles.Enterprise)}>
            {
              isEnterprise && <EnterPriseSvg></EnterPriseSvg>
            }
            <span>{item}</span>
          </span>
        )
      })
    }
  </div>
);
};
DatabendTags.defaultProps ={
  list: ['Enterprise']
}
export default DatabendTags;