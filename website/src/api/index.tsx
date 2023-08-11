import axios from 'axios';
import docusaurusConfig from '@generated/docusaurus.config';
const { customFields: { askBendUrl } = {} } = docusaurusConfig;
export function getAnswers(question: string) {
  return axios.post(`${askBendUrl}/query`, {
    query: question
  });
}

interface ICountParames {
  name: string;
  osType: string;
  osTypeDesc: string;
  id: number;
  downloadUrl: string;
  isApple: boolean;
  version: string;
  size: number
  createdAt: string;
  isUbuntu: boolean;
}
export function countDownloads(data: ICountParames) {
  console.log(data, 'data')
  return axios.post(`${askBendUrl}/query`, data);
}