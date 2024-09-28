export * from "./graph";

/**
 * Formats a given percentage value.
 * @param {number} numerator - The numerator of the percentage.
 * @param {number} denominator - The denominator of the percentage.
 * @returns {string} - The formatted percentage string.
 */
export function getPercent(numerator: number, denominator: number): string {
  if (denominator === 0) {
    return "0%";
  }
  const percent = (numerator / denominator) * 100;
  return `${percent.toFixed(1)}%`;
}

/**
 * Formats a given millisecond value into a human-readable time format.
 * @param {number} milliseconds - The value in milliseconds to be formatted.
 * @returns {string} - The formatted time string.
 */
export function filterMillisecond(milliseconds: number): string {
  if (typeof milliseconds !== 'number' || isNaN(milliseconds) || milliseconds < 0) {
    return "Invalid Input"; // Return error message for invalid input
  }

  if (milliseconds < 1000) {
    // Less than 1 second, show in milliseconds
    return `${milliseconds} ms`;
  } else if (milliseconds < 60000) {
    // Less than 1 minute, show in seconds (up to two decimal places)
    const seconds = (milliseconds / 1000).toFixed(2);
    return `${seconds} s`;
  } else if (milliseconds < 3600000) {
    // Less than 1 hour, show in minutes and seconds
    const minutes = Math.floor(milliseconds / 60000);
    const seconds = ((milliseconds % 60000) / 1000).toFixed(2);
    return `${minutes} min ${seconds} s`;
  } else {
    // More than 1 hour, show in hours, minutes, and seconds
    const hours = Math.floor(milliseconds / 3600000);
    const minutes = Math.floor((milliseconds % 3600000) / 60000);
    const seconds = ((milliseconds % 60000) / 1000).toFixed(2);
    return `${hours} h ${minutes} min ${seconds} s`;
  }
}

/**
 * Transforms the errors array by extracting the error type and merging it with the error details.
 * @param {any[]} errors - The array of errors to be transformed.
 * @returns {any[]} - The transformed array of errors.
 */
export function transformErrors(errors) {
  return errors.map(error => {
    const type = Object.keys(error)[0];
    return { _errorType: type, ...error[type] };
  });
}