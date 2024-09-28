import { useEffect, useRef } from 'react';

/**
 * Custom hook to handle DOM reshaping events and register callbacks.
 *
 * @returns {Object} - An object containing the reshapeDOM function.
 */
export const useReshape = () => {
  const callbackRef = useRef<(() => void) | null>(null);

  /**
   * Registers a callback to be called when a DOM reshape event occurs.
   * @param {Function} callback - The function to be called on DOM reshape.
   */
  const reshapeDOM = (callback: () => void) => {
    // Store the callback in a ref to keep a stable reference
    callbackRef.current = callback;
  };

  useEffect(() => {
    // Handler to call the stored callback on window resize
    const handleResize = () => {
      if (callbackRef.current) {
        callbackRef.current();
      }
    };

    // Attach resize listener
    window.addEventListener('resize', handleResize);

    // Cleanup listener on unmount
    return () => {
      window.removeEventListener('resize', handleResize);
    };
  }, []);

  return {
    reshapeDOM,
  };
};
