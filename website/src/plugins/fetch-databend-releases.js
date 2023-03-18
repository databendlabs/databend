const axios = require('axios');
const bytes = require('bytes');
// Define constant
const LINUX_GENERIC_X86 = 'Linux Generic(x86, 64-bit)';
const LINUX_GENERIC_ARM = 'Linux Generic(ARM, 64-bit)';
const MAC_X86 = 'macOS (x86, 64-bit)';
const MAC_ARM = 'macOS (ARM, 64-bit)';
const GITHUB_DOWNLOAD = 'https://github.com/datafuselabs/databend/releases/download';
const GITHUB_REPO = 'https://api.github.com/repos/datafuselabs/databend';
const DATABEND_RELEASES = 'https://repo.databend.rs/databend/releases.json';
const DATABEND_DOWNLOAD =  'https://repo.databend.rs/databend';

const IGNORE_TEXT = '<!-- Release notes generated using configuration in .github/release.yml at main -->';
const REG = /https:\/\/github\.com\/datafuselabs\/databend\/pull\/(\d+)/g;
const REPLACE_TEXT = '[#$1](https://github.com/datafuselabs/databend/pull/$1)';

module.exports = function fetchDatabendReleasesPlugin() {
  return {
    name: 'fetch-databend-releases',
    async contentLoaded({_, actions}) {
      const { setGlobalData } = actions;
      const { data: releasesList } = await axios.get(DATABEND_RELEASES);
      const { data: repoResource } = await axios.get(GITHUB_REPO);
      // Preprocessing data, Just part of it
      let releases = releasesList?.filter(release=> release.assets?.length).slice(0, 21);
      const processedData = releases.map(release=> {
        let afterFilterAssets = 
          release.assets
            .filter(asset => !/linux-gnu|testsuites|sha256sums|\.deb/.test(asset.name))
            .map(asset => {
              let isApple = asset.name.includes('apple');
              let isAarch64 = asset.name.includes('aarch64');
              let osTypeDesc = isApple 
                ? (isAarch64 ? MAC_ARM : MAC_X86) 
                : (isAarch64 ? LINUX_GENERIC_ARM : LINUX_GENERIC_X86);
              return {
                ...asset,
                isApple,
                osTypeDesc
              }
            })
            .sort((systemLinux, systemMac) => systemMac.isApple - systemLinux.isApple)
            .map(asset=> {
              return {
                ...asset,
                formatSize: bytes.format(asset?.size, { thousandsSeparator: ',', decimalPlaces: 1 }),
                osType: asset?.osTypeDesc.match(/\(([^)]+)\)/)[1].split(',')[0],
                browser_download_url: asset?.browser_download_url?.replace(
                  GITHUB_DOWNLOAD,
                  DATABEND_DOWNLOAD
                )
              }
            });
        return {
          ...release,
          originAssets: release.assets,
          assets: afterFilterAssets,
          filterBody: release.body
                  .replace(IGNORE_TEXT, '')
                  .replace(REG, REPLACE_TEXT)
                  .replace(/@(\w+)/g, '**@$1**')
        }
      });
      // Set global data
      setGlobalData({releasesList: processedData, repoResource, stargazersCount: repoResource.stargazers_count});
    },
  };
}