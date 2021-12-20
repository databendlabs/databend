use std::pin::Pin;
use std::task::Poll;

use futures;
use futures::prelude::*;
use futures::AsyncRead;

pub type Reader = Box<dyn futures::io::AsyncRead + Unpin + Send>;

pub struct CallbackReader {
    inner: Reader,
    f: Box<dyn Fn(usize) -> ()>,
}

impl futures::AsyncRead for CallbackReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.inner).poll_read(cx, buf);

        if let Poll::Ready(Ok(len)) = r {
            (self.f)(len);
        };

        r
    }
}
