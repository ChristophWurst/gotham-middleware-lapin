extern crate futures;
extern crate gotham;
#[macro_use]
extern crate gotham_derive;
extern crate lapin_futures as lapin;
#[macro_use]
extern crate log;
extern crate tokio_core;

use std::io;

use gotham::middleware::{Middleware, NewMiddleware};
use gotham::state::{request_id, State};
use gotham::handler::HandlerFuture;
use lapin::channel::Channel;
use tokio_core::net::TcpStream;

#[derive(StateData)]
pub struct LapinChannel {
    channel: Channel<TcpStream>,
}

impl LapinChannel {
    pub fn channel(&self) -> Channel<TcpStream> {
        self.channel.clone()
    }
}

/// A Gotham compatible Middleware that lets you dispatch messages to an AMQP queue.
pub struct LapinMiddleware {
    channel: Channel<TcpStream>,
}

impl LapinMiddleware {
    pub fn new(channel: Channel<TcpStream>) -> Self {
        LapinMiddleware { channel: channel }
    }
}

impl NewMiddleware for LapinMiddleware {
    type Instance = LapinMiddleware;

    fn new_middleware(&self) -> io::Result<Self::Instance> {
        Ok(LapinMiddleware::new(self.channel.clone()))
    }
}

impl Middleware for LapinMiddleware {
    fn call<Chain>(self, mut state: State, chain: Chain) -> Box<HandlerFuture>
    where
        Chain: FnOnce(State) -> Box<HandlerFuture>,
    {
        debug!("[{}] pre chain", request_id(&state));
        state.put(LapinChannel {
            channel: self.channel.clone(),
        });

        Box::new(chain(state))
    }
}
