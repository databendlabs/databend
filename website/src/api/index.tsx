import axios from 'axios';
import docusaurusConfig from '@generated/docusaurus.config';
export function getAnswers(question: string) {
  const { customFields: { askBendUrl } = {} } = docusaurusConfig;
  return axios.post(`${askBendUrl}/query`, {
    query: question
  });
}