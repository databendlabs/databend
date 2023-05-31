// Copyright 2023 DatabendLabs.
import { FC, ReactElement, useEffect } from 'react';
import styles from './ai.module.scss';
import React from 'react';
import Return from '@site/src/components/Icons/Return';
import AIExamples from './ai-examples';
import ChatList from './chat-list';
import { useSafeState } from 'ahooks';
interface IProps {
  onReturn: ()=> void;
  initialQuery: string;
}
const AISearch: FC<IProps> = ({ onReturn, initialQuery}): ReactElement=> {
  const INPUT_ID = 'SEARCH_INPUT_AI_ASK_INPUT_ID';
  const [question, setQuestion] = useSafeState('');
  const [value, setValue] = useSafeState('');
  const [isExample, setIsExample] = useSafeState(true);
  const [isGetting, setIsGetting] = useSafeState(false);
  useEffect(()=> {
    if (initialQuery) {
      getResult(initialQuery);
    }
  }, [initialQuery]);
  function askOnKeyDown(e) {
    const code = e.keyCode || e.which;
    if ((e.target as HTMLInputElement)?.id === INPUT_ID && code === 13) {
      if (isGetting) return;
      const value = e.target.value;
      getResult(value)
    }
  }
  function askChange(e) {
    const value = e.target.value;
    setValue(value)
  }
  function askPreviewQuestion(question: string) {
    getResult(question)
  }
  function getResult(value: string) {
    setIsExample(false)
    setQuestion(value)
    setValue('')
  }
  return (
    <div className={styles.aiSearch}>
      <div onClick={()=> onReturn()} style={{display: 'inline-block'}}>
        <span className={styles.topAction}>
          <Return size={16} />Databend AI
        </span>
      </div>
      <div className={styles.wrapCommon}>
        {
          isExample 
          ? <AIExamples onAskPreviewQuestion={askPreviewQuestion}/> 
          : <ChatList 
            onGetting={(getting: boolean)=> setIsGetting(getting)}
            question={question} 
            onGetResultDone={()=> {
              setQuestion('');
            }} />
        }
        <div className={styles.inputWrap}>
          <input 
            onChange={askChange}
            id={INPUT_ID} 
            value={value} 
            autoComplete='off'
            onKeyDown={askOnKeyDown} 
            placeholder={isGetting ? 'Waiting on an answer...' : 'Ask Databend AI a question...' }
            className={styles.aiInput} />
            <div onClick={()=> getResult(value)} className={styles.send}>Ask</div>
        </div>
      </div>
    </div>
  );
};
export default AISearch;