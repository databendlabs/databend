import { FC, ReactElement, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import copy from 'copy-to-clipboard';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { okaidia } from 'react-syntax-highlighter/dist/esm/styles/prism';
import remarkGfm from 'remark-gfm';
import styles from './styles.module.scss';
import React from 'react';
import RightSvg from '../../Icons/Right';

interface IProps {
  textContent: string
}
const AskDatabendMarkdown: FC<IProps> = ({ textContent }): ReactElement=> {
  const [isCopy, setIsCopy] = useState(false);
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      components={{
        code({ inline, className, children, ...props }) {
          const match = /language-(\w+)/.exec(className || '');
          const text =  String(children).replace(/\n$/, '');
          const language = match ? match[1] : 'sql';
          return !inline && language ? (
            <div 
              onMouseLeave={()=> setIsCopy(false)}
              className={styles.codeWrap}>
              <SyntaxHighlighter
                showLineNumbers={true}
                style={okaidia as any}
                language={language}
                PreTag='div'
                {...props}
              >
                {text}
              </SyntaxHighlighter>
              <span
                className={styles.copy}
                onClick={() => {
                  copy(text);
                  setIsCopy(true);
                }}
              >
                {
                  (isCopy)
                    ? <RightSvg />
                    : <>Copy</>
                }
              </span>
            </div>
          ) : (
            <code className={className} {...props}>
              {children}
            </code>
          );
        },
        a: (props: {href: string, children: string[]} | any) => {
          const desc = props?.children[0];
          return (
            <a 
              target="_blank" 
              title={desc} 
              rel="noopener noreferrer" 
              href={props?.href}>
              {desc}
            </a>
          );
        },
        table: ({...props}) => (
          <div style={{overflowX: 'auto', width: '100%'}}>
            <table {...props} />
          </div>
        )
      }}
    >
      {textContent}
    </ReactMarkdown>
  );
};

export default AskDatabendMarkdown;