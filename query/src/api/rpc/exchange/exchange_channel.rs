use common_base::tokio::sync::Notify;
use common_base::tokio::sync::Semaphore;

use std::collections::VecDeque;
use std::sync::{Arc};
use common_infallible::Mutex;

struct Queue<T> {
    finished: bool,
    max_queue_size: usize,
    inner_queue: VecDeque<T>,
}

impl<T> Queue<T> {
    pub fn create(capacity: usize) -> Queue<T> {
        Queue::<T> {
            finished: false,
            max_queue_size: capacity,
            inner_queue: VecDeque::with_capacity(capacity),
        }
    }

    fn pop_back(&mut self) -> Result<Option<T>, RecvError> {
        Ok(self.inner_queue.pop_back())
    }

    fn push_back(&mut self, value: T) -> Result<Option<T>, SendError<T>> {
        match self.inner_queue.len() >= self.max_queue_size {
            true => Ok(Some(value)),
            false => {
                self.inner_queue.push_back(value);
                Ok(None)
            }
        }
    }
}

pub struct Channel<T> {
    pop_notify: Notify,
    push_notify: Notify,
    queue: Mutex<Queue<T>>,
}

impl<T> Channel<T> {
    pub fn create(capacity: usize) -> Channel<T> {
        Channel::<T> {
            pop_notify: Notify::new(),
            push_notify: Notify::new(),
            queue: Mutex::new(Queue::create(capacity)),
        }
    }

    pub async fn send(&self, mut value: T) -> Result<(), SendError<T>> {
        while let Some(new_value) = self.push_back(value)? {
            value = new_value;
            self.push_notify.notified().await;
        }

        self.pop_notify.notify_one();
        Ok(())
    }

    pub fn try_send(&self, mut value: T) -> Result<(), SendError<T>> {
        match self.push_back(value) {
            Ok(None) => {
                self.pop_notify.notify_one();
                Ok(())
            }
            Ok(Some(value)) => Err(SendError::QueueIsFull(value)),
            Err(failure) => Err(failure),
        }
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        loop {
            if let Some(value) = self.pop_back()? {
                self.push_notify.notify_one();
                return Ok(value);
            }

            // Wait for values to be available
            self.pop_notify.notified().await;
        }
    }

    pub fn try_recv(&self) -> Result<T, RecvError> {
        match self.pop_back() {
            Ok(Some(value)) => {
                self.push_notify.notify_one();
                Ok(value)
            }
            Ok(None) => Err(RecvError::QueueIsEmpty),
            Err(cause) => Err(cause),
        }
    }

    fn push_back(&self, value: T) -> Result<Option<T>, SendError<T>> {
        let mut queue = self.queue.lock();
        match queue.finished {
            true => Err(SendError::Finished),
            false => queue.push_back(value)
        }
    }

    fn pop_back(&self) -> Result<Option<T>, RecvError> {
        let mut queue = self.queue.lock();
        match queue.finished {
            true => Err(RecvError::Finished),
            false => queue.pop_back()
        }
    }
}

pub enum SendError<T> {
    Finished,
    QueueIsFull(T),
}

#[derive(Clone)]
pub struct Sender<T> {
    channel: Arc<Channel<T>>,
}

impl<T> Sender<T> {
    pub fn create(channel: Arc<Channel<T>>) -> Sender<T> {
        Sender { channel }
    }

    pub async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.channel.send(value).await
    }

    pub fn try_send(&self, value: T) -> Result<(), SendError<T>> {
        self.channel.try_send(value)
    }
}

pub enum RecvError {
    Finished,
    QueueIsEmpty,
}

#[derive(Clone)]
pub struct Receiver<T> {
    channel: Arc<Channel<T>>,
}

impl<T> Receiver<T> {
    pub fn create(channel: Arc<Channel<T>>) -> Receiver<T> {
        Receiver { channel }
    }

    pub async fn recv(&self) -> Result<T, RecvError> {
        self.channel.recv().await
    }

    // pub fn finished(&self) -> Result<()> {
    //
    // }

    pub fn try_recv(&self) -> Result<T, RecvError> {
        self.channel.try_recv()
    }
}

pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let channel = Arc::new(Channel::create(capacity));
    (Sender::create(channel.clone()), Receiver::create(channel))
}



