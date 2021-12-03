// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
    title: 'Databend',
    tagline: 'Built to make the Data Cloud easy.',
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
                    customCss: require.resolve('./src/css/custom.css'),
                },
            }),
        ],
    ],
    plugins: [
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'api',
                path: 'docs/api',
                routeBasePath: 'api',
                sidebarPath: require.resolve('./docs/api/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
            },
        ],
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'cli',
                path: 'docs/cli',
                routeBasePath: 'cli',
                sidebarPath: require.resolve('./docs/cli/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
            },
        ],
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'development',
                path: 'docs/development',
                routeBasePath: 'development',
                sidebarPath: require.resolve('./docs/development/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
            },
        ],
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'policies',
                path: 'docs/policies',
                routeBasePath: 'policies',
                sidebarPath: require.resolve('./docs/policies/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
            },
        ],
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'rfcs',
                path: 'docs/rfcs',
                routeBasePath: 'rfcs',
                sidebarPath: require.resolve('./docs/rfcs/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
            },
        ],
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'sqlstatement',
                path: 'docs/sqlstatement',
                routeBasePath: 'sqlstatement',
                sidebarPath: require.resolve('./docs/sqlstatement/sidebars.js'),
                editUrl: 'https://github.com/datafuselabs/databend/edit/main/website/databend',
            },
        ],
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'system',
                path: 'docs/system',
                routeBasePath: 'system',
                sidebarPath: require.resolve('./docs/system/sidebars.js'),
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
                        type: 'doc',
                        position: 'left',
                        docId: 'architecture',
                        label: 'Overview',
                    },
                    {to: '/cli/cli', label: 'CLI', position: 'left'},
                    {to: '/development/contributing', label: 'Development', position: 'left'},
                    {to: '/blog', label: 'Blog', position: 'left'},
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
                                label: 'Performance',
                                to: '/overview/performance'
                            },
                            {
                                label: 'Whitepapers',
                                to: '/overview/architecture'
                            }
                        ]
                    },
                    {
                        title: 'Community',
                        items: [
                            {
                                label: 'Stack Overflow',
                                href: 'https://stackoverflow.com/questions/tagged/docusaurus',
                            },
                            {
                                label: 'Discord',
                                href: 'https://discordapp.com/invite/docusaurus',
                            },
                            {
                                label: 'Twitter',
                                href: 'https://twitter.com/docusaurus',
                            },
                        ],
                    },
                    {
                        title: 'More',
                        items: [
                            {
                                label: 'Blog',
                                to: '/blog',
                            },
                            {
                                label: 'Weekly',
                                href: 'https://weekly.databend.rs/'
                            },
                            {
                                label: 'GitHub',
                                href: 'https://github.com/facebook/docusaurus',
                            },
                        ],
                    },
                ],
                copyright: `Copyright © ${new Date().getFullYear()} Datafuselabs, Inc. Built with Docusaurus.`,
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
