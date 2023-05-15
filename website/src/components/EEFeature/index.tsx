
// Copyright 2023 DatabendLabs.
import clsx from 'clsx';
import React, { FC, ReactElement, useEffect } from 'react';
import styles from './styles.module.scss';
interface IProps {
  featureTitle?: string;
  featureName: string;
  wholeDesc?: string;

}
const EEFeature: FC<IProps> = ({ featureName, wholeDesc, featureTitle }): ReactElement => {
  function A() {
    return <>please contact <a target='_blank' href={'https://www.databend.com/contact-us'}>Databend Support.</a></>
  }
  useEffect(() => {
    const h1 = document?.querySelector('.theme-doc-markdown')?.querySelector('header')?.firstChild as HTMLElement;
    if (h1) {
      h1?.classList?.add('DOCITEM-PAGE-EE-TIPS-BEFORE-DOM');
    }
  }, [])
  return (
    <div className='DOCITEM-PAGE-EE-TIPS'>
      <div className={clsx(styles.wrap)}>
        <div className={styles.button}>{featureTitle}</div>
        <div className={styles.desc}>
          {
            wholeDesc
              ? <>
                {wholeDesc} <A />
              </>
              : <>
                {featureName} requires Enterprise Edition. To inquire about upgrading, <A />
              </>
          }
        </div>
      </div>
    </div>
  );
};
EEFeature.defaultProps = {
  featureTitle: 'ENTERPRISE EDITION FEATURE'
}
export default EEFeature;