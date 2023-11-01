import React from 'react';
import Translate, {translate} from '@docusaurus/Translate';
import PaginatorNavLink from '@theme/PaginatorNavLink';
import JoinCommunity from '@site/src/components/JoinCommunity';
import styles from './styles.module.scss';
import Link from '@docusaurus/Link';
import { ArrowLeft, ArrowRight } from '@site/src/components/Icons';
export default function DocPaginator(props) {
  const {previous, next} = props;
  return (
    <div>
       <nav
        className="pagination-nav docusaurus-mt-lg"
        aria-label={translate({
          id: 'theme.docs.paginator.navAriaLabel',
          message: 'Docs pages navigation',
          description: 'The ARIA label for the docs pagination',
        })}>
        {
          previous 
          ?  <Link className={styles.page} to={previous?.permalink}>
              <ArrowLeft />
              <span>{previous?.title}</span>
            </Link>
          : <i></i>
        }
        {next && (
          <Link className={styles.page} to={next?.permalink}>
            <span>{next?.title}</span>
            <ArrowRight />
          </Link>
        )}
      </nav>
      <div className={styles.community}>
        <JoinCommunity maxWidth={720} justifyContent='flex-start' titleAlign='left'/>
      </div>
    </div>
  );
}
