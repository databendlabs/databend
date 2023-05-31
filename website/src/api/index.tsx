import axios from 'axios';
export function getAnswers(question: string) {
  return axios.post('/query', {
    query: question
  });
}