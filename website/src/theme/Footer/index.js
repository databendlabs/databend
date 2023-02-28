/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {useThemeConfig} from '@docusaurus/theme-common';
import useBaseUrl from '@docusaurus/useBaseUrl';
import CookiesConsent from '../../components/CookiesConsent';
import styles from './index.module.scss';
import * as icons from "../../components/Icons"



function Footer() {
  const year = new Date().getFullYear()
  const {footer} = useThemeConfig()
  return (
    <footer className={clsx('footer', styles.footer)}>
        <p>Copyright © {year} The Databend Community. Built with Docusaurus.</p>
        <div className={clsx('footer-items', styles.footerItems)}>
        {footer.links[0].items.map((item,index)=>{
          return <Link to={item.to} key={index}><h6>{item.label}</h6></Link>
        })}<span>|</span>
        {footer.links[1].items.map((item,index)=>{
          const Icon = icons[item.label]
          return<Link to={item.href} key={index}><h6><span className={clsx('icon', styles.icon)}><Icon size={20}/></span>{item.label}</h6></Link>
        })}
        </div>
      <CookiesConsent></CookiesConsent>
    </footer>
  );
}

export default React.memo(Footer);
