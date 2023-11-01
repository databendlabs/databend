
// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement, ReactNode } from 'react';
import LinkSvg from '../../icons/link.svg';
import copy from 'copy-to-clipboard';
import Tooltip from '../BaseComponents/Tooltip';
// import { MDXProvider } from '@mdx-js/react';
interface IProps {
  number: number | string;
  children: ReactNode;
  title: string;
}
const StepContent: FC<IProps> = ({number, children, title}): ReactElement=> {
  return (
    <div className="step-container" id={title}>
      <span className="step-number">
        <span>{number}</span> 
        <h3 className='anchor'>
          {title}
          <a href={`#${title}`}>
          <Tooltip content="Copy Link">
            <LinkSvg onClick={()=> copy(decodeURIComponent(window.location?.origin+window.location.pathname+'#'+title))}></LinkSvg>
          </Tooltip>
         </a>
        </h3>
      </span>
      <div className="step-content">{children}</div>
    </div>
  );
};
export default StepContent;