const axios = require('axios');
const bytes = require('bytes');
// Define constant
const LINUX_GENERIC_X86 = 'Linux Generic (x86, 64-bit)';
const LINUX_GENERIC_ARM = 'Linux Generic (ARM, 64-bit)';
const MAC_X86 = 'Mac Intel Chip (x86, 64-bit)';
const MAC_ARM = 'Mac Apple Chip (ARM, 64-bit)';
const GITHUB_DOWNLOAD = 'https://github.com/datafuselabs/databend/releases/download';
const GITHUB_REPO = 'https://api.github.com/repos/datafuselabs/databend';
const DATABEND_RELEASES = 'https://api.github.com/repos/datafuselabs/databend/releases';
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
      const releases = releasesList?.filter(release=> release.assets?.length).slice(0, 21);
      const processedData = releases.map(release=> {
        const filterAssets = namesToMatch(release);
        const afterProcessedAssets = 
          filterAssets
            .map(asset => {
              const isApple = asset.name.includes('apple');
              const isAarch64 = asset.name.includes('aarch64');
              const osTypeDesc = isApple 
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
          assets: afterProcessedAssets,
          filterBody: release.body
                  .replace(IGNORE_TEXT, '')
                  .replace(REG, REPLACE_TEXT)
                  .replace(/\@[\w\-]+/g, '**$&**')
        }
      });
      // name match list
      function namesToMatch(release) {
        const { assets, tag_name } = release;
        const namesDisplayList = [
          `databend-${tag_name}-aarch64-apple-darwin.tar.gz`,
          `databend-${tag_name}-x86_64-apple-darwin.tar.gz`,
          `databend-${tag_name}-aarch64-unknown-linux-musl.tar.gz`,
          `databend-${tag_name}-x86_64-unknown-linux-gnu.tar.gz`
        ];
        const filteredAssets = assets?.filter(item => {
          return namesDisplayList?.includes(item?.name);
        });
        return filteredAssets;
      }
      // Set global data
      setGlobalData({releasesList: processedData, repoResource, stargazersCount: repoResource.stargazers_count});
    },
  };
}