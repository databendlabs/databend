// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Headers } from 'headers.js';

/**
 * Implementation of the Fetch API that wraps the Rust fetch implementation
 */

/**
 * @typedef {Object} RequestInit
 * @property {string} [method='GET'] - HTTP method
 * @property {Object} [headers] - Request headers
 * @property {string} [body] - Request body
 * @property {number} [timeout_ms] - Request timeout in milliseconds
 */

/**
 * Implements the Fetch API fetch() function
 * @param {string|Request} input - URL or Request object
 * @param {RequestInit} [init] - Request configuration
 * @returns {Promise<Response>} Response promise
 */
async function fetch(input, init = {}) {
    // Handle input parameter
    let url;
    if (typeof input === 'string') {
        url = input;
    } else if (input instanceof Request) {
        url = input.url;
        // Merge Request object properties with init
        init = {
            method: input.method,
            headers: input.headers,
            body: input.body,
            ...init
        };
    } else {
        throw new TypeError('First argument must be a URL string or Request object');
    }

    // Process init options
    const method = (init.method || 'GET').toUpperCase();
    const headers = init.headers ? Object.fromEntries(
        Object.entries(init.headers).map(([k, v]) => [k.toLowerCase(), String(v)])
    ) : null;
    const body = init.body ? String(init.body) : null;
    const timeout_ms = init.timeout_ms ? Math.floor(init.timeout_ms) : null;

    try {
        // Call Rust implementation
        return await sendHttpRequest(method, url, headers, body, timeout_ms);
    } catch (error) {
        throw new Error('fetch error: ' + error.message);
    }
}

/**
 * Request class implementing the Web Fetch API Request interface
 */
class Request {
    #method;
    #url;
    #headers;
    #body;

    /**
     * @param {string|Request} input - URL or Request object 
     * @param {RequestInit} [init] - Request configuration
     */
    constructor(input, init = {}) {
        if (typeof input === 'string') {
            this.#url = input;
        } else if (input instanceof Request) {
            this.#url = input.url;
            this.#method = input.method;
            this.#headers = {...input.headers};
            this.#body = input.body;
        } else {
            throw new TypeError('First argument must be a URL string or Request object');
        }

        // Override with init properties
        this.#method = (init.method || this.#method || 'GET').toUpperCase();
        this.#headers = init.headers || this.#headers || {};
        this.#body = init.body !== undefined ? init.body : this.#body;
    }

    get method() { return this.#method; }
    get url() { return this.#url; }
    get headers() { return this.#headers; }
    get body() { return this.#body; }

    /**
     * Creates a copy of the request
     * @returns {Request}
     */
    clone() {
        return new Request(this.#url, {
            method: this.#method,
            headers: {...this.#headers},
            body: this.#body
        });
    }
}

export { fetch, Request, Headers };
