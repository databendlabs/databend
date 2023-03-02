import React from 'react';
import styles from './styles.module.scss';
import clsx from 'clsx';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

const PopularTags = (props) =>{
    const {siteConfig} = useDocusaurusContext();
    const activeItem = props.props?props.props.tag.label:''
    const PopularTag = siteConfig.customFields.blogTags
    return <>
    <h3 className={clsx('blog-page-list-title',styles.blogPageListTitle)}>Popular topics</h3>
    <div className={clsx('blog-page-populartag',styles.popularTag)}>
    {PopularTag.map((item,index)=>{
      return <a key={index} href={`/blog/tags/${item}`} className={clsx(styles.item, { [styles.activeItem]: item === activeItem })}>
      # {item}
    </a>
    })}
   </div>
   </>
}

export default PopularTags