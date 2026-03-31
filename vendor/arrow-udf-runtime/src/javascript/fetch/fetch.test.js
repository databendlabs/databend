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

// Test utilities
function assert(condition, message) {
    if (!condition) {
        throw new Error(message || 'Assertion failed');
    }
}

function assertEquals(actual, expected, message) {
    if (actual !== expected) {
        throw new Error(message || `Expected ${expected} but got ${actual}`);
    }
}

function assertObjectEquals(actual, expected, message) {
    const actualStr = JSON.stringify(actual);
    const expectedStr = JSON.stringify(expected);
    if (actualStr !== expectedStr) {
        throw new Error(message || `Expected ${expectedStr} but got ${actualStr}`);
    }
}

// Test Request class
{
    // Test Request constructor with URL string
    const req1 = new Request('https://example.com/api');
    assertEquals(req1.method, 'GET');
    assertEquals(req1.url, 'https://example.com/api');
    assertObjectEquals(req1.headers, {});
    // assert(req1.body === null);

    // Test Request constructor with init options
    const req2 = new Request('https://example.com/api', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ key: 'value' })
    });
    assertEquals(req2.method, 'POST');
    assertEquals(req2.url, 'https://example.com/api');
    // assertObjectEquals(req2.headers, { 'content-type': 'application/json' });
    assertEquals(req2.body, '{"key":"value"}');

    // Test Request constructor with Request object
    const req3 = new Request(req2);
    assertEquals(req3.method, 'POST');
    assertEquals(req3.url, 'https://example.com/api');
    // assertObjectEquals(req3.headers, { 'content-type': 'application/json' });
    assertEquals(req3.body, '{"key":"value"}');

    // Test Request constructor with Request object and init
    const req4 = new Request(req2, {
        method: 'PUT',
        body: JSON.stringify({ other: 'value' })
    });
    assertEquals(req4.method, 'PUT');
    assertEquals(req4.url, 'https://example.com/api');
    // assertObjectEquals(req4.headers, { 'content-type': 'application/json' });
    assertEquals(req4.body, '{"other":"value"}');

    // Test Request clone
    const req5 = req4.clone();
    assertEquals(req5.method, 'PUT');
    assertEquals(req5.url, 'https://example.com/api');
    // assertObjectEquals(req5.headers, { 'content-type': 'application/json' });
    assertEquals(req5.body, '{"other":"value"}');

    // Test invalid input
    try {
        new Request({});
        assert(false, 'Should throw on invalid input');
    } catch (e) {
        assert(e instanceof TypeError);
    }
}

// Test fetch function
{
    // Mock `sendHttpRequest` for testing
    let lastFetchCall = null;
    globalThis.sendHttpRequest = async (method, url, headers, body, timeout_ms) => {
        lastFetchCall = { method, url, headers, body, timeout_ms };
        return {
            status: 200,
            headers: { 'content-type': 'application/json' }
        };
    };

    // Test fetch with string URL
    await fetch('https://example.com/api');
    assertObjectEquals(lastFetchCall, {
        method: 'GET',
        url: 'https://example.com/api',
        headers: null,
        body: null,
        timeout_ms: null
    });

    // Test fetch with Request object
    const req = new Request('https://example.com/api', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ test: true })
    });
    await fetch(req);
    assertObjectEquals(lastFetchCall, {
        method: 'POST',
        url: 'https://example.com/api',
        headers: { 'content-type': 'application/json' },
        body: '{"test":true}',
        timeout_ms: null
    });

    // Test fetch with init options
    await fetch('https://example.com/api', {
        method: 'PUT',
        headers: { 'Authorization': 'Bearer token' },
        body: 'test data',
        timeout_ms: 5000
    });
    assertObjectEquals(lastFetchCall, {
        method: 'PUT',
        url: 'https://example.com/api',
        headers: { 'authorization': 'Bearer token' },
        body: 'test data',
        timeout_ms: 5000
    });

    // Test fetch error handling
    globalThis.sendHttpRequest = async () => {
        throw new Error('timeout');
    };
    try {
        await fetch('https://example.com/api');
        assert(false, 'Should throw on timeout');
    } catch (e) {
        assert(e instanceof Error);
        assert(e.message.includes('timeout'));
    }

    // Test invalid input
    try {
        await fetch({});
        assert(false, 'Should throw on invalid input');
    } catch (e) {
        assert(e instanceof TypeError);
    }
}
