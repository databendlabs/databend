use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::FutureExt;

pub fn select3<A, B, C>(fut1: A, fut2: B, fut3: C) -> Select3<A, B, C>
where
    A: Future + Unpin,
    B: Future + Unpin,
    C: Future + Unpin,
{
    Select3 {
        inner: Some((fut1, fut2, fut3)),
    }
}

pub struct Select3<A, B, C> {
    inner: Option<(A, B, C)>,
}

impl<A: Unpin, B: Unpin, C: Unpin> Unpin for Select3<A, B, C> {}

impl<A, B, C> Future for Select3<A, B, C>
where
    A: Future + Unpin,
    B: Future + Unpin,
    C: Future + Unpin,
{
    type Output = Select3Output<(A::Output, B, C), (B::Output, A, C), (C::Output, A, B)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (mut a, mut b, mut c) = self.inner.take().expect("cannot poll Select twice");

        if let Poll::Ready(val) = a.poll_unpin(cx) {
            return Poll::Ready(Select3Output::Left((val, b, c)));
        }

        if let Poll::Ready(val) = b.poll_unpin(cx) {
            return Poll::Ready(Select3Output::Middle((val, a, c)));
        }

        if let Poll::Ready(val) = c.poll_unpin(cx) {
            return Poll::Ready(Select3Output::Right((val, a, b)));
        }

        self.inner = Some((a, b, c));
        Poll::Pending
    }
}

pub enum Select3Output<A, B, C> {
    Left(A),
    Middle(B),
    Right(C),
}
