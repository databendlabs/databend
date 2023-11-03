// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const TwitterSvg =
    '<svg width="20" style="top: 5px; position: relative" height="20" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true"><g><path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"></path></g></svg>';

const lightCodeTheme = require('prism-react-renderer/themes/oceanicNext');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');
const { site_env } = process.env;
const isProduction = site_env === 'production';
const ASKBEND_URL = 'https://ask.databend.rs';
/** @type {import('@docusaurus/types').Config} */
const config = {
    title: 'Databend',
    staticDirectories: ['static', '../docs/public'],
    tagline: 'Databend - Your best alternative to Snowflake. Cost-effective and simple for massive-scale analytics.',
    url: 'https://databend.rs',
    baseUrl: '/',
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'throw',
    favicon: 'img/logo/logo-no-text.svg',
    organizationName: 'datafuselabs',
    projectName: 'databend',

    i18n: {
        defaultLocale: 'en-US',
        locales: ['en-US'],
        localeConfigs: {
            'en-US': {
                label: 'English',
            },
        },
    },
    headTags: [
        {
            tagName: 'link',
            attributes: {
                rel: 'mask-icon',
                sizes: 'any',
                color: '#0175f6',
                href: '/img/logo/logo-no-text.svg',
            },
        },
    ],
    customFields: {
        blogTags: ['weekly', 'databend'],
        askBendUrl: isProduction ? ASKBEND_URL : ''
    },
    presets: [
        [
            '@docusaurus/preset-classic',
            /** @type {import('@docusaurus/preset-classic').Options} */
            ({
                docs: {
                    path: '../docs/doc',
                    routeBasePath: 'doc',
                    sidebarPath: require.resolve('../docs/doc/sidebars.js'),
                    editUrl: ({ locale, docPath }) => {
                        if (locale !== config.i18n.defaultLocale) {
                            return `https://databend.crowdin.com/databend/${locale}`;
                        }
                        return `https://github.com/datafuselabs/databend/edit/main/docs/doc/${docPath}`;
                    },
                },
                blog: {
                    showReadingTime: true,
                    editUrl: ({ locale, blogPath }) => {
                        if (locale !== config.i18n.defaultLocale) {
                            return `https://databend.crowdin.com/databend/${locale}`;
                        }
                        return `https://github.com/datafuselabs/databend/edit/main/website/blog/${blogPath}`;
                    },
                    blogSidebarCount: 5,
                    postsPerPage: 'ALL',
                    blogListComponent: '@site/src/components/CustomBlog/CustomBlogListPage.js',
                    blogPostComponent: '@site/src/components/CustomBlog/BlogPostDetails.js',
                    blogTagsPostsComponent: '@site/src/components/CustomBlog/CustomBlogTagsPostsPage.js',
                },
                theme: {
                    customCss: require.resolve('./src/css/custom.scss'),
                },
                sitemap: {
                    changefreq: 'daily',
                    priority: 0.5,
                },
                gtag: {
                    trackingID: 'G-WBQPTTG4ZG',
                    anonymizeIP: true,
                },
            }),
        ],
    ],
    plugins: [
        'docusaurus-plugin-sass',
        './src/plugins/global-sass-var-inject',
        './src/plugins/fetch-databend-releases',
        [
            '@docusaurus/plugin-content-docs',
            /** @type {import('@docusaurus/plugin-content-docs').Options} */
            {
                id: 'dev',
                path: '../docs/dev',
                routeBasePath: 'dev',
                sidebarPath: require.resolve('../docs/dev/sidebars.js'),
                editUrl: ({ locale, devPath }) => {
                    if (locale !== config.i18n.defaultLocale) {
                        return `https://databend.crowdin.com/databend/${locale}`;
                    }
                    return `https://github.com/datafuselabs/databend/edit/main/docs/dev/${devPath}`;
                },
            },
        ],
        'plugin-image-zoom',
        [
            "docusaurus-plugin-devserver",
            {
                devServer: {
                    proxy: {
                        "/query": {
                            target: ASKBEND_URL,
                            // pathRewrite: { "^/query": "" },
                            changeOrigin: true,
                            headers: {
                                Origin: ASKBEND_URL
                            }
                        },
                    },
                },
            },
        ]
    ],
    themeConfig:
        /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
        ({
            imageZoom: {
                selector: 'article :not(a) > img'
            },
            announcementBar: {
                id: 'announcementBar-2', // Increment on change
                content: `⭐️ If you like Databend, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/datafuselabs/databend">GitHub</a> and follow us on <a target="_blank" rel="noopener noreferrer" href="https://twitter.com/DatabendLabs" >Twitter</a> ${TwitterSvg}`,
            },
            navbar: {
                title: 'Databend',
                logo: {
                    alt: 'Databend Logo',
                    src: 'img/logo/logo-no-text.svg',
                },
                items: [
                    {
                        to: '/doc',
                        label: 'Documentation',
                        position: 'right',
                    },
                    { to: '/blog', label: 'Blog', position: 'right' }, // or position: 'right'
                    {
                        to: '/download',
                        label: 'Download',
                        position: 'right',
                    },
                ],
            },
            footer: {
                links: [
                    {
                        title: 'RESOURCES',
                        items: [
                            {
                                label: 'Performance',
                                to: 'https://databend.rs/blog/clickbench-databend-top'
                            },
                            {
                                label: 'Deployment',
                                to: '/doc/deploy'
                            },
                            {
                                label: 'Releases',
                                to: '/doc/releases'
                            },
                        ]
                    },
                    {
                        title: 'COMMUNITY',
                        items: [
                            {
                                label: 'Slack',
                                href: 'https://link.databend.rs/join-slack',
                            },
                            {
                                label: 'Twitter',
                                href: 'https://twitter.com/DatabendLabs',
                            },
                        ],
                    },
                ],
                copyright: `Copyright © 2023 Datafuse Labs, Inc. Built with Docusaurus. <br><br> <img src="https://www.datocms-assets.com/31049/1618983297-powered-by-vercel.svg">`,
            },
            prism: {
                theme: lightCodeTheme,
                darkTheme: darkCodeTheme,
                additionalLanguages: ['toml', 'rust'],
            },
            algolia: {
                appId: 'RL7MS9PKE8',
                apiKey: 'cb5d6af612410c0fced698ff39ccd47a',
                indexName: 'databend-rs-docs',
                contextualSearch: true,
            },
            image: 'img/logo/logo-no-text.png',
            metadata: [
                { name: 'twitter:card', content: 'summary_large_image' },
                { name: 'twitter:site', content: '@databend.rs' }
            ],
        }),
};

module.exports = config;
