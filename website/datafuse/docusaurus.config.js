/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const path = require('path');

module.exports = {
  title: 'Distributed ClickHouse with Cloud-Native Architecture in Rust\nBlazing Fast ~50 billion rows/s',
  tagline: ' ',
  organizationName: 'datafuse',
  projectName: 'datafuse',
  url: 'https://datafuse.dev',
  baseUrl: '/',
  favicon: 'img/favicon.ico',
  themeConfig: {
    navbar: {
      title: '',
      logo: {
        alt: 'DataFuse Logo',
        src: 'img/logo.png',
      },
      items: [
        { to: 'docs/overview/building-and-running', label: 'Documentation', position: 'left' },
        // Please keep GitHub link to the right for consistency.
        {
          href: 'https://github.com/datafuselabs/datafuse',
          label: 'GitHub',
          position: 'left',
        },
      ],
    },
    footer: {
      style: 'dark',
      copyright: `Copyright © ${new Date().getFullYear()} DatafuseLabs. Built with Docusaurus.`,
    },
    prism: {
      defaultLanguage: 'shell',
      theme: require('./src/prismTheme'),
    }
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          showLastUpdateAuthor: false,
          showLastUpdateTime: true,
          showLastUpdateTime: true,
          editUrl:
            'https://github.com/datafuselabs/datafuse/blob/master/website/datafuse/',
          path: 'docs',
          sidebarPath: require.resolve('./sidebars.json'),
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
  plugins: [
    path.join(__dirname, '/plugins/monaco-editor'),
    path.join(__dirname, '/plugins/case-sensitive-paths')
  ],
};
