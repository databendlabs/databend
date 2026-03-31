# Fetch API for Arrow UDF JS

A lightweight implementation of the [Web Fetch API](https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API) for Arrow UDF JavaScript functions. This implementation allows making HTTP requests from JavaScript UDFs while maintaining basic compatibility with the standard Web Fetch API.

## Usage

Before using this module, you need to enable `fetch` in the `Runtime`.

```rust,ignore
let runtime = Runtime::new().await.unwrap();
runtime.enable_fetch();
```

Then you can use the `fetch` API in your JavaScript UDF.

```js
export async function my_fetch_udf(input) {
    const response = await fetch("https://api.example.com/data");
    return await response.json();
}
```

More examples:

```js
// Basic GET request
const response = await fetch('https://api.example.com/data');
// get json from response
const data = await response.json();
// or get text from response
const data = await response.text();

// POST request with headers and body
const response = await fetch('https://api.example.com/data', {
    method: 'POST',
    headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer token'
    },
    body: JSON.stringify({ key: 'value' }),
    timeout_ms: 5000 // Optional timeout
});

// Using Request object
const request = new Request('https://api.example.com/data', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ key: 'value' })
});
const response = await fetch(request);
```


## Internal Design

```text
  ┌─────────────┐                      
  │ JS UDF Code │                      
  └──────┬──────┘                      
         │                             
    ┌────▼─────┐                       
    │  fetch() │  Source: fetch.js     
    └────┬─────┘                       
         │                             
┌────────▼──────────┐                  
│ sendHttpRequest() │  Source: mod.rs  
└───────────────────┘                  
```

1. A JavaScript `fetch()` wrapper function to normalize parameters
2. Calls native `sendHttpRequest`
3. Rust makes HTTP request via Rust library `reqwest`
4. Response wrapped in `Response` class
5. Returned to JavaScript
