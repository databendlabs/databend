import axios from "axios";
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';
import { useSessionStorageState, useMount, useLocalStorageState } from 'ahooks';
import { copyToClipboard } from 'copyforjs';


const cacheKey = 'global-cache-releases';
const cacheTimeKey = 'global-cache-time';
const cacheTagNameKey = 'global-cache-tag-name';

export function getLatest(){
  let [cacheReleast, setCacheReleast] = useSessionStorageState(cacheKey);
  const [cacheTime, setCacheTime] = useSessionStorageState(cacheTimeKey);
  const [cacheTagName, setCacheTagName] = useLocalStorageState(cacheTagNameKey);
  useMount(()=>{  
    if (ExecutionEnvironment.canUseDOM) {
      setTimeout(()=>{
        const timeStamp = new Date().getTime();
        // It's only cached for an hour
        if (cacheTime && (timeStamp - cacheTime) > 60*60*1000) {
          cacheReleast = null;
          setCacheReleast();
        }
        getData();
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
               }, 1)
            }, false);
          }
        }
      });
    }
  })
  function getData(){
    if (cacheReleast) {
      setText(cacheReleast?.name);
    } else {
      axios.get('https://api.github.com/repos/datafuselabs/databend/releases')
        .then(res=>{
          if(res.data && res.data?.length > 0){
            const data = res.data[0];
            const { name } = data;
            setCacheReleast(data)
            setCacheTime(new Date().getTime())
            setText(name);
            setCacheTagName(name);
          } else {
            setText()
          }
        })
        .catch(()=>{
          setText()
        })
    }
  }
  function setText(text) {
    const dom = document.querySelectorAll('.variable');
    for (let div of dom){
      if (div.innerHTML === '${version}') {
        // when error default value v0.7.39
        div.innerText = text ? text : (cacheTagName || 'v0.7.39');
      }
    }
  }
}
