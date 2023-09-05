// Copyright 2023 DatabendLabs.
import React, { useState } from 'react';
import { useDoc, useDocsSidebar } from '@docusaurus/theme-common/internal';
import Link from '@docusaurus/Link';
import { useMount } from 'ahooks';
const IndexOverviewList = ()=> {
  const { metadata } = useDoc();
  const siderBars = useDocsSidebar()?.items;
  const [items, setItems] = useState([]);
  useMount(()=> {
    const permalink = metadata?.permalink;
    const targetDoc = findItemByPermalink(siderBars, permalink);
    setItems(targetDoc?.items || []);
  });
  function findItemByPermalink(siderBars, permalink) {
    const sidebar = siderBars.find((item) => item?.href === permalink);
    if (sidebar) {
      return sidebar;
    }
    for (const sidebar of siderBars) {
      if (sidebar?.items?.length > 0) {
        const nestedItem = findItemByPermalink(sidebar?.items || [], permalink);
        if (nestedItem) {
          return nestedItem;
        }
      }
    }
    return null;
  };
  return (
    <>
      {
        items?.length > 0 &&
          <ul>
            {
              items?.map((item)=> {
                return <li key={item?.href}>
                  <Link to={item?.href}>{item.label}</Link>
                </li>
              })
            }
          </ul>
        }
    </>
  );
};
export default IndexOverviewList;
