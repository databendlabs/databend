export interface IAssets {
  size: number;
  formatSize?: string;
  browser_download_url: string;
  name: string;
  osTypeDesc?: string;
  osType: string;
  isApple?: boolean;
  isUbuntu?: boolean;
}

export interface IRepoResource {
  stargazers_count: number;
  formatStargazersCount?: number;
  [prop: string]: any
}
export interface IRelease {
  tag_name: string;
  published_at: string;
  prerelease: boolean;
  name: string;
  body: string;
  filterBody: string;
  originAssets: IAssets[];
  assets: IAssets[]
}
export interface IGlobalData {
  releasesList: IRelease[], 
  repoResource: IRepoResource, 
  stargazersCount: number,
  [prop: string]: any
}