// Copyright 2023 DatabendLabs.
import { FC, ReactElement, useEffect } from 'react';
import styles from './ai.module.scss';
import React from 'react';
import Avatar from '@site/src/components/Icons/Avatar';
import { getAnswers } from '@site/src/api';
import Logo from '@site/static/img/logo/logo-no-text.svg'
import LogoSvg from '@site/src/components/BaseComponents/Logo';
import clsx from 'clsx';
import AskDatabendMarkdown from '@site/src/components/BaseComponents/Markdown';
import { useSafeState } from 'ahooks';
interface IProps {
  question: string;
  onGetResultDone: (result: string) => void;
  onGetting: (getting: boolean, question?: string) => void;
}
interface IListType {
  type: "Q" | "A";
  message: string;
}
const ChatList: FC<IProps> = ({ question, onGetResultDone, onGetting }): ReactElement=> {
  const [list, setList] = useSafeState<IListType[]>([]);
  useEffect(()=> {
    if(question) {
      setList([
        ...list,
        {
          type: "Q",
          message: question
        }
      ]);
      getResults(question);
    }
  }, [question]);
  async function getResults(question: string) {
    getting(true);
    const data = await getAnswers(question);
    if ([200, 201]?.includes(data?.status )) {
      const result = data?.data?.result;
      setList(pre=> ([
        ...pre,
        {type: 'A', message: result}
      ]));
      onGetResultDone(result);
    } else {
     
    }
    getting(false);
  }
  function getting(tag: boolean) {
    onGetting && onGetting(tag);
  }
  return (
    <div className={styles.chatList}>
      {
        list.map((item, index)=> {
          const { message, type } = item;
          const isAnswer = type === 'A';
          return (
            <div key={index} className={clsx(styles.chatItem, isAnswer && styles.chatItemAnswer)}>
              {
                isAnswer ? <LogoSvg style={{transform: 'scale(1.8)'}} width={26} /> : <Avatar size={26}/>
              }
              <div style={{flex: 1, maxWidth: 'calc(100% - 38px)'}}>
                <AskDatabendMarkdown textContent={message} />
              </div>
            </div>
          )
        })
      }
    </div>
  );
};
export default ChatList;