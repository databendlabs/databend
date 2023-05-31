// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement } from 'react';
import styles from './ai.module.scss';
import questions from './preview-question.js';
import LogoSvg from '@site/src/components/BaseComponents/Logo';
interface IProps {
  onAskPreviewQuestion?: (q: string)=> void;
}
const AIExamples: FC<IProps> = ({ onAskPreviewQuestion }): ReactElement=> {
return (
  <div>
    <div className={styles.exampleTitle}>Examples</div>
    <div className={styles.exampleList}>
      {
        questions?.map((item, index)=> {
          return (
            <div onClick={()=> onAskPreviewQuestion(item)} className={styles.item} key={index}>
              <LogoSvg width={26} />
              <span style={{flex: 1, maxWidth: 'calc(100% - 30px)'}}>{item}</span>
            </div>
          )
        })
      }
    </div>
  </div>
  );
};
export default AIExamples;