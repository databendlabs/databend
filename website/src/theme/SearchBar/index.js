import React, {useCallback, useEffect, useMemo, useRef, useState} from 'react';
import {DocSearchButton, useDocSearchKeyboardEvents} from '@docsearch/react';
import Head from '@docusaurus/Head';
import Link from '@docusaurus/Link';
import {useHistory} from '@docusaurus/router';
import {isRegexpStringMatch} from '@docusaurus/theme-common';
import {useSearchPage} from '@docusaurus/theme-common/internal';
import {
  useAlgoliaContextualFacetFilters,
  useSearchResultUrlProcessor,
} from '@docusaurus/theme-search-algolia/client';
import Translate from '@docusaurus/Translate';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {createPortal} from 'react-dom';
import translations from '@theme/SearchTranslations';
import SearchInitModal from './init-modal';
import AISearch from './ai-search';
import CommonModal from '@site/src/components/BaseComponents/CommonModal';
let DocSearchModal = null;
function Hit({hit, children}) {
  return <Link to={hit.url}>{children}</Link>;
}
function ResultsFooter({state, onClose}) {
  const {generateSearchPageLink} = useSearchPage();
  return (
    <Link to={generateSearchPageLink(state.query)} onClick={onClose}>
      <Translate
        id="theme.SearchBar.seeAll"
        values={{count: state.context.nbHits}}>
        {'See all {count} results'}
      </Translate>
    </Link>
  );
}
function mergeFacetFilters(f1, f2) {
  const normalize = (f) => (typeof f === 'string' ? [f] : f);
  return [...normalize(f1), ...normalize(f2)];
}
function DocSearch({contextualSearch, externalUrlRegex, ...props}) {
  const [selected, setSelected] = useState(undefined);
  const {siteMetadata} = useDocusaurusContext();
  const processSearchResultUrl = useSearchResultUrlProcessor();
  const contextualSearchFacetFilters = useAlgoliaContextualFacetFilters();
  const configFacetFilters = props.searchParameters?.facetFilters ?? [];
  const facetFilters = contextualSearch
    ? // Merge contextual search filters with config filters
      mergeFacetFilters(contextualSearchFacetFilters, configFacetFilters)
    : // ... or use config facetFilters
      configFacetFilters;
  // We let user override default searchParameters if she wants to
  const searchParameters = {
    ...props.searchParameters,
    facetFilters,
  };
  const history = useHistory();
  const searchContainer = useRef(null);
  const searchButtonRef = useRef(null);
  const [isOpen, setIsOpen] = useState(false);
  const [initialQuery, setInitialQuery] = useState(undefined);
  useEffect(()=> {
    if (selected === 0) {
      insertCustomDom()
    } else {
      resetOverflow()
    }
  }, [selected]);
  const importDocSearchModalIfNeeded = useCallback(() => {
    if (DocSearchModal) {
      return Promise.resolve();
    }
    return Promise.all([
      import('@docsearch/react/modal'),
      import('@docsearch/react/style'),
      import('./styles.css'),
    ]).then(([{DocSearchModal: Modal}]) => {
      DocSearchModal = Modal;
    });
  }, []);
  const onOpen = useCallback(() => {
    importDocSearchModalIfNeeded().then(() => {
      searchContainer.current = document.createElement('div');
      document.body.insertBefore(
        searchContainer.current,
        document.body.firstChild,
      );
      setIsOpen(true);
      setSelected(undefined);
    });
  }, [importDocSearchModalIfNeeded, setIsOpen]);
  const onClose = useCallback(() => {
    setIsOpen(false);
    resetOverflow()
    searchContainer.current?.remove();
  }, [setIsOpen]);
  const onInput = useCallback(
    (event) => {
      importDocSearchModalIfNeeded().then(() => {
        setIsOpen(true);
        setSelected(undefined);
        setInitialQuery(event.key);
      });
    },
    [importDocSearchModalIfNeeded, setIsOpen, setInitialQuery],
  );
  const navigator = useRef({
    navigate({itemUrl}) {
      // Algolia results could contain URL's from other domains which cannot
      // be served through history and should navigate with window.location
      if (isRegexpStringMatch(externalUrlRegex, itemUrl)) {
        window.location.href = itemUrl;
      } else {
        history.push(itemUrl);
      }
    },
  }).current;
  const transformItems = useRef((items) =>
    props.transformItems
      ? // Custom transformItems
        props.transformItems(items)
      : // Default transformItems
        items.map((item) => ({
          ...item,
          url: processSearchResultUrl(item.url),
        })),
  ).current;
  const resultsFooterComponent = useMemo(
    () =>
      // eslint-disable-next-line react/no-unstable-nested-components
      (footerProps) =>
        <ResultsFooter {...footerProps} onClose={onClose} />,
    [onClose],
  );
  const transformSearchClient = useCallback(
    (searchClient) => {
      searchClient.addAlgoliaAgent(
        'docusaurus',
        siteMetadata.docusaurusVersion,
      );
      return searchClient;
    },
    [siteMetadata.docusaurusVersion],
  );
  useDocSearchKeyboardEvents({
    isOpen,
    onOpen,
    onClose,
    onInput,
    searchButtonRef,
  });
  function insertCustomDom() {
    const docSearchModal = document.querySelector('.DocSearch-Modal');
    if (docSearchModal) {
      const divElement = document.createElement('div');
      divElement.className = 'searchTopActionWrap'
      divElement.innerHTML = '<div class="searchTopAction"><svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 20 20" preserveAspectRatio="xMidYMid meet" fill="none" stroke="currentColor" stroke-width="1" stroke-linecap="round" stroke-linejoin="round"><path d="M5.722 4L3 6.333l2.722 2.723"></path><path d="M3 6.333h8.942c2.677 0 4.95 2.186 5.054 4.861.11 2.827-2.225 5.25-5.054 5.25h-6.61"></path></svg>Search docs</div>';
      divElement.addEventListener('click', () => {
        setSelected(undefined);
        setInitialQuery('');
      });
      if (docSearchModal.firstChild) {
        docSearchModal.insertBefore(divElement, docSearchModal.firstChild);
      } else {
        docSearchModal.appendChild(divElement);
      }
    }
  }
  function resetStatus() {
    setSelected(-1);
    setIsOpen(false);
    resetOverflow()
  }
  function resetOverflow() {
    const bodyClassList = document.body.classList;
    if (bodyClassList.contains('DocSearch--active')) {
      bodyClassList.remove('DocSearch--active');
    }
  }
  return (
    <>
      <Head>
        {/* This hints the browser that the website will load data from Algolia,
        and allows it to preconnect to the DocSearch cluster. It makes the first
        query faster, especially on mobile. */}
        <link
          rel="preconnect"
          href={`https://${props.appId}-dsn.algolia.net`}
          crossOrigin="anonymous"
        />
      </Head>

      <DocSearchButton
        onTouchStart={importDocSearchModalIfNeeded}
        onFocus={importDocSearchModalIfNeeded}
        onMouseOver={importDocSearchModalIfNeeded}
        onClick={onOpen}
        ref={searchButtonRef}
        translations={translations.button}
      />
      {/* isOpen && */}
      <>
      {
        selected == 0 &&
        DocSearchModal &&
        searchContainer.current &&
        createPortal(
          <DocSearchModal
            onClose={onClose}
            initialScrollY={window.scrollY}
            initialQuery={initialQuery}
            navigator={navigator}
            transformItems={transformItems}
            hitComponent={Hit}
            transformSearchClient={transformSearchClient}
            {...(props.searchPagePath && {
              resultsFooterComponent,
            })}
            {...props}
            searchParameters={searchParameters}
            placeholder={translations.placeholder}
            translations={translations.modal}
          />,
          searchContainer.current,
        )}
      </>
      {
        selected === undefined && 
        <SearchInitModal 
          getInputValue={(value)=> setInitialQuery(value)}
          onClose={()=> {
            resetStatus()
          }}
          onSelect={(selected)=> setSelected(selected)} 
          visible={isOpen}/>
      }
      <AIModal 
        onClose={()=> {
          resetStatus();
        }}
        initialQuery={initialQuery}
        visible={selected === 1} 
        onCloseAiSearch={()=> {
          setSelected(undefined);
          setInitialQuery('');
        }} />
    </>
  );
}

function AIModal({ visible, onCloseAiSearch, onClose, initialQuery }) {
  return (
    <CommonModal
      onClose={onClose}
      visible={visible}
      width={766}>
      <AISearch 
        initialQuery={initialQuery}
        onReturn={()=> onCloseAiSearch()}/>
    </CommonModal>
  )
}

export default function SearchBar() {
  const {siteConfig} = useDocusaurusContext();
  return <DocSearch {...siteConfig.themeConfig.algolia} />;
}
