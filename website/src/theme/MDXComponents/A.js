import React from 'react';
import Link from '@docusaurus/Link';
import LinkSvg from '@site/src/icons/link.svg';
import styles from './styles.module.scss';
import Tooltip from '@site/src/components/BaseComponents/Tooltip';
function hasProtocolPrefix(str) {
  return str.startsWith('http://') || str.startsWith('https://');
}
export default function MDXA(props) {
  if (hasProtocolPrefix(props?.href)) {
    return <Tooltip
              style={{display: 'inline-block'}}
              content={"Open in the new tab"}>
              <span className={styles.linkWrap}>
                <a {...props} target="_blank" rel="noopener noreferrer" />
                <LinkSvg/>
              </span>
            </Tooltip>
  } else {
    return <Link {...props} />;
  }
}