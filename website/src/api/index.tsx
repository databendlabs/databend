import axios from 'axios';
import docusaurusConfig from '@generated/docusaurus.config';
const { customFields: { askBendUrl } = {} } = docusaurusConfig;
export function getAnswers(question: string) {
  return axios.post(`${askBendUrl}/query`, {
    query: question
  });
}