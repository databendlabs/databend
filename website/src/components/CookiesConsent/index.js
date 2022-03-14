import React, { useEffect } from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import styles from './styles.module.scss';
import { useState } from 'react';
const COOKIE_KEY = 'COOKIE_KEY'

function CookiesConsent() {
  const [isHidden, setIsHidden] = useState(true);
  const closePopup = ()=>{
    window['ga-disable-G-WBQPTTG4ZG'] = true;
    setCookiesKey();
  }
  const acceptAll = ()=>{ 
    window['ga-disable-G-WBQPTTG4ZG'] = false;
    setCookiesKey();
    
  }
  useEffect(()=>{
    const isAccept = window.localStorage.getItem(COOKIE_KEY);
    if (!isAccept) {
      setIsHidden(false);
    }
  })
  function setCookiesKey() {
    window.localStorage.setItem(COOKIE_KEY, 1);
    setIsHidden(true);
  }
  return (
  <>
   {
    !isHidden && <div className={clsx(styles.consenWrap)}>
      <p>
        <span>We use cookies on our site to provide you with better user experience. You can view our Cookies Policy in </span>
        <Link href="https://databend.com/privacy/"> Privacy Policy</Link> .
      </p>
      <div className={styles.right}>
        <button onClick={acceptAll} className={styles.button}>ACCEPT ALL COOKIES</button>
        <div onClick={closePopup} className={styles.close}>X</div>
      </div>
    </div>
   }
  </>
   
  );
}

export default React.memo(CookiesConsent);
