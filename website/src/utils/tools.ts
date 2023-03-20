import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import isYesterday from 'dayjs/plugin/isYesterday';
dayjs.extend(relativeTime);
dayjs.extend(isYesterday);

// Calculate the time between now and before
export function timeFormatAgo(time: Date | string) {
  if (!time) return '-';
  const t = dayjs(time);
  return t.isYesterday() ? 'yesterday' : t.fromNow();
}