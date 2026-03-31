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

// A simple Headers implementation

const _headerList = Symbol("header list");
const _iterableHeaders = Symbol("iterable headers");
const _iterableHeadersCache = Symbol("iterable headers cache");
const _guard = Symbol("guard");

// Helper functions
function byteLowerCase(s) {
  return String(s).toLowerCase();
}

function httpTrim(s) {
  return String(s).trim();
}

const HTTP_TOKEN_CODE_POINT_RE = /^[!#$%&'*+\-.^_`|~\w]+$/;

function checkHeaderNameForHttpTokenCodePoint(name) {
  return HTTP_TOKEN_CODE_POINT_RE.test(name);
}

function checkForInvalidValueChars(value) {
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c === 0x0a || c === 0x0d || c === 0x00) {
      return false;
    }
  }
  return true;
}

function normalizeHeaderValue(potentialValue) {
  return httpTrim(potentialValue);
}

function appendHeader(headers, name, value) {
  value = normalizeHeaderValue(value);

  if (!checkHeaderNameForHttpTokenCodePoint(name)) {
    throw new TypeError(`Invalid header name: "${name}"`);
  }
  if (!checkForInvalidValueChars(value)) {
    throw new TypeError(`Invalid header value: "${value}"`);
  }

  if (headers[_guard] == "immutable") {
    throw new TypeError("Cannot change header: headers are immutable");
  }

  const list = headers[_headerList];
  const lowercaseName = byteLowerCase(name);
  for (let i = 0; i < list.length; i++) {
    if (byteLowerCase(list[i][0]) === lowercaseName) {
      name = list[i][0];
      break;
    }
  }
  list.push([name, value]);
}

function getHeader(list, name) {
  const lowercaseName = byteLowerCase(name);
  const entries = [];
  for (let i = 0; i < list.length; i++) {
    if (byteLowerCase(list[i][0]) === lowercaseName) {
      entries.push(list[i][1]);
    }
  }

  return entries.length === 0 ? null : entries.join(", ");
}

function fillHeaders(headers, object) {
  if (Array.isArray(object)) {
    for (let i = 0; i < object.length; ++i) {
      const header = object[i];
      if (header.length !== 2) {
        throw new TypeError(
          `Invalid header: length must be 2, but is ${header.length}`,
        );
      }
      appendHeader(headers, header[0], header[1]);
    }
  } else {
    for (const key in object) {
      if (Object.hasOwn(object, key)) {
        appendHeader(headers, key, object[key]);
      }
    }
  }
}

class Headers {
  [_headerList] = [];
  [_guard];

  get [_iterableHeaders]() {
    const list = this[_headerList];

    if (this[_guard] === "immutable" && this[_iterableHeadersCache] !== undefined) {
      return this[_iterableHeadersCache];
    }

    const seenHeaders = Object.create(null);
    const entries = [];
    
    for (let i = 0; i < list.length; ++i) {
      const entry = list[i];
      const name = byteLowerCase(entry[0]);
      const value = entry[1];
      
      if (name === "set-cookie") {
        entries.push([name, value]);
      } else {
        const seenHeaderIndex = seenHeaders[name];
        if (seenHeaderIndex !== undefined) {
          const entryValue = entries[seenHeaderIndex][1];
          entries[seenHeaderIndex][1] = entryValue.length > 0
            ? entryValue + ", " + value
            : value;
        } else {
          seenHeaders[name] = entries.length;
          entries.push([name, value]);
        }
      }
    }

    entries.sort((a, b) => {
      const akey = a[0];
      const bkey = b[0];
      if (akey > bkey) return 1;
      if (akey < bkey) return -1;
      return 0;
    });

    this[_iterableHeadersCache] = entries;
    return entries;
  }

  constructor(init = undefined) {
    this[_guard] = "none";
    if (init !== undefined) {
      fillHeaders(this, init);
    }
  }

  append(name, value) {
    appendHeader(this, String(name), String(value));
  }

  delete(name) {
    name = String(name);
    if (!checkHeaderNameForHttpTokenCodePoint(name)) {
      throw new TypeError(`Invalid header name: "${name}"`);
    }
    if (this[_guard] == "immutable") {
      throw new TypeError("Cannot change headers: headers are immutable");
    }

    const list = this[_headerList];
    const lowercaseName = byteLowerCase(name);
    for (let i = 0; i < list.length; i++) {
      if (byteLowerCase(list[i][0]) === lowercaseName) {
        list.splice(i, 1);
        i--;
      }
    }
  }

  get(name) {
    name = String(name);
    if (!checkHeaderNameForHttpTokenCodePoint(name)) {
      throw new TypeError(`Invalid header name: "${name}"`);
    }
    return getHeader(this[_headerList], name);
  }

  getSetCookie() {
    const list = this[_headerList];
    const entries = [];
    for (let i = 0; i < list.length; i++) {
      if (byteLowerCase(list[i][0]) === "set-cookie") {
        entries.push(list[i][1]);
      }
    }
    return entries;
  }

  has(name) {
    name = String(name);
    if (!checkHeaderNameForHttpTokenCodePoint(name)) {
      throw new TypeError(`Invalid header name: "${name}"`);
    }

    const list = this[_headerList];
    const lowercaseName = byteLowerCase(name);
    for (let i = 0; i < list.length; i++) {
      if (byteLowerCase(list[i][0]) === lowercaseName) {
        return true;
      }
    }
    return false;
  }

  set(name, value) {
    name = String(name);
    value = String(value);
    value = normalizeHeaderValue(value);

    if (!checkHeaderNameForHttpTokenCodePoint(name)) {
      throw new TypeError(`Invalid header name: "${name}"`);
    }
    if (!checkForInvalidValueChars(value)) {
      throw new TypeError(`Invalid header value: "${value}"`);
    }

    if (this[_guard] == "immutable") {
      throw new TypeError("Cannot change headers: headers are immutable");
    }

    const list = this[_headerList];
    const lowercaseName = byteLowerCase(name);
    let added = false;
    for (let i = 0; i < list.length; i++) {
      if (byteLowerCase(list[i][0]) === lowercaseName) {
        if (!added) {
          list[i][1] = value;
          added = true;
        } else {
          list.splice(i, 1);
          i--;
        }
      }
    }
    if (!added) {
      list.push([name, value]);
    }
  }

  // Iterator methods
  entries() {
    return this[_iterableHeaders].values();
  }

  keys() {
    return this[_iterableHeaders].map(entry => entry[0]).values();
  }

  values() {
    return this[_iterableHeaders].map(entry => entry[1]).values();
  }

  forEach(callback, thisArg) {
    for (const [name, value] of this[_iterableHeaders]) {
      callback.call(thisArg, value, name, this);
    }
  }

  [Symbol.iterator]() {
    return this.entries();
  }
}

export { Headers };
