// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: 'Databend',
    tagline: 'The modern data warehouse, have the Elasticity and Performance both',
    url: 'https://databend.rs',
    baseUrl: '/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',
    favicon: 'img/favicon.svg',
    organizationName: 'datafuselabs',
    projectName: 'databend',

    presets: [
        [
            '@docusaurus/preset-classic',
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    path: '../docs/doc',
                    routeBasePath: 'doc',
                    sidebarPath: require.resolve('../docs/doc/sidebars.js'),
                    editUrl: 'https://github.com/datafuselabs/databend/edit/main/databend',
                },
                blog: {
                    showReadingTime: true,
                    editUrl:
                        'https://github.com/datafuselabs/databend/edit/main/databend/blog',
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.scss'),
                },
                sitemap: {
                    changefreq: 'daily',
                    priority: 0.5,
                },
            }),
        ],
    ],
    plugins: [
        'docusaurus-plugin-sass',
        './src/plugins/pxToVw',
        './src/plugins/globalSassVarInject',
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'learn',
                path: '../docs/learn',
                routeBasePath: 'learn',
                sidebarPath: require.resolve('../docs/learn/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/databend',
            },
        ],
        [
            '@docusaurus/plugin-client-redirects',
            {
                // this will be removed later, make a mark~.
                redirects: [
                    {
                        to: '/', // string
                        from: '/doc/index.md', // string | string[]
                    },
                ],
            },
        ]
    ],
    themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
        ({
            navbar: {
                title: 'Databend',
                logo: {
                    alt: 'Databend Logo',
                    src: 'img/favicon.svg',
                },
                items: [
                    {
                        to: '/learn/category/lessons', label: 'Learn', position: 'left', items:[
                            {label: 'Lessons', to: '/learn/category/lessons'},
                        ]
                    },
                    {
                        href: 'https://github.com/datafuselabs/databend',
                        label: 'GitHub',
                        position: 'right',
                    },
                ],
            },
            footer: {
                style: 'dark',
                links: [
                    {
                        title: 'Resources',
                        items: [
                            {
                                label: 'SQL Reference',
                                to: 'doc/category/sql'
                            },
                            {
                                label: 'Performance',
                                to: '/doc/category/performance'
                            },
                        ]
                    },
                    {
                        title: 'Community',
                        items: [
                            {
                                label: 'Slack',
                                href: 'https://join.slack.com/t/datafusecloud/shared_invite/zt-nojrc9up-50IRla1Y1h56rqwCTkkDJA',
                            },
                            {
                                label: 'Twitter',
                                href: 'https://twitter.com/Datafuse_Labs',
                            },
                        ],
                    },
                    {
                        title: 'More',
                        items: [
                            {
                                label: 'Weekly',
                                href: 'https://weekly.databend.rs/'
                            },
                            {
                                label: 'GitHub',
                                href: 'https://github.com/datafuselabs/databend',
                            },
                        ],
                    },
                ],
                copyright: `Copyright © ${new Date().getFullYear()} Datafuse Labs, Inc. Built with Docusaurus. <br> <a style="display:block;margin-top: 10px" href="https://vercel.com/?utm_source=databend&utm_campaign=oss"><img src="https://www.datocms-assets.com/31049/1618983297-powered-by-vercel.svg"></a>`,
            },
            prism: {
                theme: lightCodeTheme,
                darkTheme: darkCodeTheme,
            },
            algolia: {
                appId: 'RL7MS9PKE8',
                apiKey: '78bb6be96bb0361a4be9dab6bd83936c',
                indexName: 'databend-rs',
                contextualSearch: true,
            }
        }),
};

module.exports = config;