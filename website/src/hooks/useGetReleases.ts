import { usePluginData } from "@docusaurus/useGlobalData";
interface IGlobalData {
  releasesList: any[], 
  repoResource: any, 
  stargazersCount: number,
  [prop: string]: any
}
function numberFormat(num: number): string | number {
  return num < 1000 ? num : (num / 1000).toFixed(1) + 'K';
}
export const useGetReleases = () => {
  const { releasesList, repoResource, stargazersCount } = usePluginData('fetch-databend-releases') as IGlobalData;
  let tagName = releasesList[0]?.tag_name??'v1.0.22-nightly';
  const formatStargazersCount = numberFormat(stargazersCount);
  return {
    releasesList,
    tagName,
    repoResource,
    stargazersCount,
    formatStargazersCount
  };
};

