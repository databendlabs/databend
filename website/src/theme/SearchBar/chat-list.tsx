// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement, useEffect, useRef, useState } from 'react';
import styles from './ai.module.scss';
import Avatar from '@site/src/components/Icons/Avatar';
import { getAnswers } from '@site/src/api';
import LogoSvg from '@site/src/components/BaseComponents/Logo';
import clsx from 'clsx';
import AskDatabendMarkdown from '@site/src/components/BaseComponents/Markdown';
import { useSafeState } from 'ahooks';
import DynamicDot from '@site/src/components/BaseComponents/DynamicDot';
interface IProps {
  question: string;
  onGetResultDone: (result: string) => void;
  onGetting: (getting: boolean, question?: string) => void;
}
interface IListType {
  type: "Q" | "A" | 'ERROR';
  message: string;
}
const ChatList: FC<IProps> = ({ question, onGetResultDone, onGetting }): ReactElement=> {
  const [list, setList] = useSafeState<IListType[]>([]);
  const [isGetting, setIsGetting] = useState(false);
  const listContainerRef = useRef(null);
  useEffect(()=> {
    if(question) {
      setList([
        ...list,
        {
          type: "Q",
          message: question
        }
      ]);
      scrollToBottom(true)
      getResults(question);
    }
  }, [question]);
  async function getResults(question: string) {
    try {
      getting(true);
      const data = await getAnswers(question);
      if ([200, 201]?.includes(data?.status )) {
        const result = data?.data?.result;
        setList(pre=> ([
          ...pre,
          {type: 'A', message: result}
        ]));
        onGetResultDone(result);
        scrollToBottom()
      } else {
        dealError();
      }
    }
    catch {
      dealError();
    } 
    finally {
      getting(false);
    }
  }
  function dealError() {
    setList(pre=> ([
      ...pre,
      {type: 'ERROR', message: 'Oops! Something went wrong'}
    ]));
    onGetResultDone('ERROR');
    scrollToBottom(true)
  }
  function getting(tag: boolean) {
    setIsGetting(tag);
    onGetting && onGetting(tag);
  }
  function scrollToBottom(isAsk = false) {
    setTimeout(()=> {
      const container = listContainerRef.current;
      if (isAsk) {
        container.scrollTop = container.scrollHeight;
      } else {
        const originalScrollTop = container.scrollTop;
        container.scrollTop = originalScrollTop + 60;
      }
    }, 10);
  }
  return (
    <div ref={listContainerRef} className={styles.chatList}>
      {
        list.map((item, index)=> {
          const { message, type } = item;
          const isError = type === 'ERROR';
          const isAnswer = (type === 'A' || isError);
         
          return (
            <div key={index} className={clsx(styles.chatItem, isAnswer && styles.chatItemAnswer)}>
              {
                isAnswer ? <LogoSvg style={{transform: 'scale(1.8)'}} width={26} /> : <Avatar size={26}/>
              }
              {
                isError
                ? <div className={styles.error}>{message}</div>
                : <div style={{flex: 1, maxWidth: 'calc(100% - 38px)'}}>
                    <AskDatabendMarkdown textContent={message} />
                  </div>
              }
              
            </div>
          )
        })
      }
      {
        isGetting && 
        <div className={clsx(styles.chatItem, styles.chatItemAnswer)}>
          <LogoSvg style={{transform: 'scale(1.8)'}} width={26} />
          <span>
            Generating<DynamicDot />
          </span>
        </div>
      }
    </div>
  );
};
export default ChatList;