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

function assertArrayEquals(actual, expected, message) {
    if (actual.length !== expected.length) {
        throw new Error(message || `Arrays have different lengths. Expected ${expected.length} but got ${actual.length}`);
    }
    for (let i = 0; i < actual.length; i++) {
        if (actual[i] !== expected[i]) {
            throw new Error(message || `Arrays differ at index ${i}. Expected ${expected[i]} but got ${actual[i]}`);
        }
    }
}

// Constructor tests
{
    const emptyHeaders = new Headers();
    assert(emptyHeaders.get('content-type') === null, "Empty headers should return null");

    const objHeaders = new Headers({
        'Content-Type': 'text/plain',
        'X-Custom': 'test'
    });
    assertEquals(objHeaders.get('content-type'), 'text/plain');
    assertEquals(objHeaders.get('x-custom'), 'test');

    const arrayHeaders = new Headers([
        ['Content-Type', 'text/plain'],
        ['X-Custom', 'test']
    ]);
    assertEquals(arrayHeaders.get('content-type'), 'text/plain');
    assertEquals(arrayHeaders.get('x-custom'), 'test');
}

// Method tests
{
    const headers = new Headers();

    // append
    headers.append('X-Test', 'value1');
    assertEquals(headers.get('x-test'), 'value1');
    
    headers.append('X-Test', 'value2');
    assertEquals(headers.get('x-test'), 'value1, value2');

    // set
    headers.set('X-Test', 'value3');
    assertEquals(headers.get('x-test'), 'value3');

    // has
    assert(headers.has('x-test'), "has() should return true for existing header");
    assert(!headers.has('nonexistent'), "has() should return false for non-existing header");

    // delete
    headers.delete('X-Test');
    assert(!headers.has('x-test'), "delete() should remove header");

    // getSetCookie
    headers.append('Set-Cookie', 'cookie1=value1');
    headers.append('Set-Cookie', 'cookie2=value2');
    assertArrayEquals(headers.getSetCookie(), ['cookie1=value1', 'cookie2=value2']);
}

// Iteration tests
{
    const headers = new Headers({
        'Content-Type': 'text/plain',
        'X-Custom': 'test',
        'Accept': 'application/json'
    });

    // entries
    const entries = [...headers.entries()];
    assertArrayEquals(entries.flat(), [
        'accept', 'application/json',
        'content-type', 'text/plain',
        'x-custom', 'test'
    ]);

    // keys
    const keys = [...headers.keys()];
    assertArrayEquals(keys, ['accept', 'content-type', 'x-custom']);

    // values
    const values = [...headers.values()];
    assertArrayEquals(values, ['application/json', 'text/plain', 'test']);

    // forEach
    const forEachEntries = [];
    headers.forEach((value, key) => {
        forEachEntries.push(key, value);
    });
    assertArrayEquals(forEachEntries, [
        'accept', 'application/json',
        'content-type', 'text/plain',
        'x-custom', 'test'
    ]);
}

// Error cases
{
    const headers = new Headers();
    
    try {
        headers.set('Invalid:Name', 'value');
        assert(false, "Should throw on invalid header name");
    } catch (e) {
        assert(e instanceof TypeError);
    }

    try {
        headers.set('X-Test', 'value\n with\n newlines');
        assert(false, "Should throw on invalid header value");
    } catch (e) {
        assert(e instanceof TypeError);
    }
}

