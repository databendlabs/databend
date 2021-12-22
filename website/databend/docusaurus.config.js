// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: 'Databend',
    tagline: 'The Open Source Serverless Data Warehouse for Everyone.',
    url: 'https://databend.rs',
    baseUrl: '/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'warn',
    favicon: 'img/favicon.png',
    organizationName: 'datafuselabs',
    projectName: 'databend',

    presets: [
        [
            '@docusaurus/preset-classic',
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    path: 'docs/overview',
                    routeBasePath: 'overview',
                    sidebarPath: require.resolve('./docs/overview/sidebars.js'),
                    editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
                },
                blog: {
                    showReadingTime: true,
                    editUrl:
                        'https://github.com/datafuselabs/databend/edit/main/website/databend/blog',
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.scss'),
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
                id: 'user',
                path: 'docs/user',
                routeBasePath: 'user',
                sidebarPath: require.resolve('./docs/user/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
            },
        ],
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'dev',
                path: 'docs/dev',
                routeBasePath: 'dev',
                sidebarPath: require.resolve('./docs/dev/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
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
                    src: 'img/favicon.png',
                },
                items: [
                    {
                        position: 'left', label: 'Overview', items: [
                            {label: 'Architecture', to: '/overview/architecture'},
                            {label: 'Performance', to: '/overview/performance'}
                        ]
                    },
                    {
                        to: '/user', label: 'User Guide', position: 'left', items: [
                            {label: 'Get Started', to: '/user'},
                            {label: 'SQL', to: '/user/category/sql-statement'},
                            {label: 'CLI', to: '/user/category/cli'},
                            {label: 'API', to: '/user/category/api'}
                        ]
                    },
                    {
                        label: 'Dev Guide', position: 'left', items: [
                            {label: 'Contributing', to: '/dev/category/contributing'},
                            {label: 'Development', to: '/dev/category/development'},
                            {label: 'Roadmap', to: '/dev/roadmap'},
                            {label: 'Policies', to: '/dev/category/policies'},
                            {label: 'RFCs', to: '/dev/category/rfcs'}
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
                                label: 'CLI Reference',
                                to: '/user/category/cli'
                            },
                            {
                                label: 'Performance',
                                to: '/overview/performance'
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
                copyright: `Copyright © ${new Date().getFullYear()} Datafuselabs, Inc. Built with Docusaurus. <br> <a style="display:block;margin-top: 10px" href="https://vercel.com/?utm_source=databend&utm_campaign=oss"><img src="https://www.datocms-assets.com/31049/1618983297-powered-by-vercel.svg"></a>`,
            },
            prism: {
                theme: lightCodeTheme,
                darkTheme: darkCodeTheme,
            },
            algolia: {
                apiKey: 'TBD',
                indexName: 'TBD',
                contextualSearch: true,
                searchParameters: {},
            },
            gtag: {
                trackingID: 'TBD',
            },
        }),
};

module.exports = config;
