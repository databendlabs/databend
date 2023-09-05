// Copyright 2023 DatabendLabs.
import React, { FC, ReactElement, useEffect, useRef, useState } from 'react';
import styles from './init-modal.module.scss';
import CommonModal from '@site/src/components/BaseComponents/CommonModal';
import { Book } from '@site/src/components/Icons';
import clsx from 'clsx';
import LogoSvg from '@site/src/components/BaseComponents/Logo';
interface IProps {
  visible: boolean;
  onSelect: (index: number)=> void;
  onClose: ()=> void;
  getInputValue: (value: string)=> void;
}
const SearchInitModal: FC<IProps> = ({visible, onSelect, onClose, getInputValue, ...props}): ReactElement=> {
  const [inputValue, setInputValue] = useState('');
  const inputRef = useRef(null);
  const [indecator, setIndecator] = useState<number>(-1);
  const ID = 'POPUP_BANNER_DOC_SEARCH'
  function changeInput(e) {
    const value = e.target.value?.trim();
    setInputValue(value);
    getInputValue(value);
  }
  function InputValueShow({inputValue}: {inputValue: string}) {
    return (
      <>
        {
          inputValue && 
          <span className={styles.inputValue}>
            :<span> {inputValue}</span>
          </span> 
          }
      </>
    )
  }
function ThreePoint() {
  return (
   <>
    {
      !inputValue && <span>...</span>
    }
   </>
  )
}
useEffect(()=> {
  if (inputValue) {
    setIndecator(0);
  }
}, [inputValue]);
useEffect(()=> {
  inputRef?.current?.focus();
}, [visible]);
function dealKeyDownEvent(e) {
  const code = e.keyCode || e.which;
  const isTarget = (e.target as HTMLInputElement).id === ID;
  if (isTarget ) {
    
    if (code === 40 || code === 38) { // up down
      setValue();
    }
    if (code === 13) { // enter
      if (e.target.value === '') {
        onSelect(indecator === -1 ? 0 : indecator);
        getInputValue('');
        return;
      }
      onSelect(indecator);
    }
  }
}
function setValue() {
  let index = indecator + 1;
  if (index > 1) index = 0;
  if (index < 0 ) index = 1;
  setIndecator(index);
}
const items = [
  {
    name: 'Search the docs',
    icon: <Book />
  },
  {
    name: 'Ask Databend AI',
    icon: <LogoSvg style={{transform: 'scale(1.3)'}} width={30} />
  }
]
return (
  <CommonModal 
    onClose={onClose}
    visible={visible}
    width={766} 
    className={styles.modalWrap} {...props}>
    <div>
      <div className={styles.topInput}>
        <input ref={inputRef} autoComplete='off' id={ID} onKeyDown={dealKeyDownEvent} onChange={changeInput} placeholder='Please enter your keywords'></input>
      </div>
      <div className={styles.content}>  
        <div className={styles.title}>Choose to search the documentation or ask Databend AI:</div>
        <div onMouseEnter={()=> {
          setIndecator(-1)
        }} className={styles.items}>
          {
            items?.map((item, index)=> {
              return (
                <div 
                  key={index}
                  onClick={()=> onSelect(index)}
                  className={clsx(indecator === index && styles.itemActive)}>
                  {item.icon}
                  <span>
                    {item.name}<ThreePoint />
                    <InputValueShow inputValue={inputValue}/>
                  </span>
                </div>
              )
            })
          }
        </div>
      </div>
    </div>
  </CommonModal>
);
};
export default SearchInitModal;