import React from "react";
import { useMount } from 'ahooks';
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';
import useGetReleases from "@site/src/hooks/useGetReleases";
import { copyToClipboard } from 'copyforjs';

function GetLatest() {
  const { 
    tagName
  } = useGetReleases();
  useMount(()=>{  
    if (ExecutionEnvironment.canUseDOM) {
      setTimeout(()=>{
        setTagName();
        const dom = document.querySelectorAll('.clean-btn')
        for(let button of dom) {
          if (button?.getAttribute('aria-label') === 'Copy code to clipboard') {
            button.addEventListener("click", ()=>{
              setTimeout(()=>{
                let text = button?.previousSibling?.innerText;
                if (button?.parentNode?.previousSibling) {
                  text = button?.parentNode?.previousSibling?.innerText;;
                }
                copyToClipboard(String(text).trim())
               }, 10)
            }, false);
          }
        }
      });
    }
  });
  function setTagName() {
    const name = tagName || 'v1.0.22-nightly';
    const dom = document.querySelectorAll('.variable');
    for (let div of dom){
      if (div.innerHTML === '${version}') {
        div.innerText = name;
      }
    }
  }
  return (<></>)
}
export default GetLatest;