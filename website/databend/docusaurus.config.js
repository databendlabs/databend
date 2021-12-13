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
                        to: '/user/index', label: 'User Guide', position: 'left', items: [
                            {label: 'Get Started', to: '/user/index'},
                            {label: 'API', to: '/user/api/index'},
                            {label: 'CLI', to: '/user/cli/index'},
                            {label: 'SQL Statement', to: '/user/sqlstatement/index'},
                            {label: 'System', to: '/user/system/index'}
                        ]
                    },
                    {
                        to: '/dev/index', label: 'Dev Guide', position: 'left', items: [
                            {label: 'Get Started', to: '/dev/index'},
                            {label: 'Roadmap', to: '/dev/roadmap'},
                            {label: 'Building', to: '/dev/building/index'},
                            {label: 'Contributing', to: '/dev/contributing/index'},
                            {label: 'RFCs', to: '/dev/rfcs/index'},
                            {label: 'Policies', to: '/dev/policies/index'}
                        ]
                    },
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
                                label: 'Blog',
                                to: '/blog',
                            },
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
                copyright: `Copyright © ${new Date().getFullYear()} Datafuselabs, Inc. Built with Docusaurus. <br> <a href="https://vercel.com/?utm_source=databend&utm_campaign=oss"><img src="https://www.datocms-assets.com/31049/1618983297-powered-by-vercel.svg"></a>`,
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
