import axios from "axios";
import ExecutionEnvironment from '@docusaurus/ExecutionEnvironment';
import { useSessionStorageState, useMount } from 'ahooks';

const cacheKey = 'global-cache-releases';
const cacheTimeKey = 'global-cache-time';

export function getLatest(){
  let [cacheReleast, setCacheReleast] = useSessionStorageState(cacheKey);
  const [cacheTime, setCacheTime] = useSessionStorageState(cacheTimeKey);
  useMount(()=>{
    const timeStamp = new Date().getTime();
    // It's only cached for an hour
    if (cacheTime && (timeStamp - cacheTime) > 60*60*1000 ) {
      cacheReleast = null;
      setCacheReleast();
    }
    if (ExecutionEnvironment.canUseDOM) {
      setTimeout(()=>{
        getData();
      })
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
          } else {
            setText()
          }
        })
        .catch(()=>{
          setText()
        })
    }
  }
  // default value v0.7.38
  function setText(text = 'v0.7.38') {
    const dom = document.querySelectorAll('.variable');
    for (let div of dom){
      if (div.innerHTML === '${version}') {
        div.innerText = text;
      }
    }
  }
}
