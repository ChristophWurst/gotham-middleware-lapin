extern crate amq_protocol;
extern crate futures;
extern crate gotham;
#[macro_use]
extern crate gotham_derive;
extern crate lapin_futures as lapin;
#[macro_use]
extern crate log;
extern crate tokio_core;

use std::io;
use std::net::SocketAddr;

use amq_protocol::types::FieldTable;
use futures::Future;
use gotham::middleware::{Middleware, NewMiddleware};
use gotham::state::{request_id, FromState, State};
use gotham::handler::HandlerFuture;
use lapin::client::ConnectionOptions;
use lapin::channel::{Channel, ConfirmSelectOptions, ExchangeDeclareOptions, QueueBindOptions,
                     QueueDeclareOptions};
use tokio_core::{net::TcpStream, reactor::Handle};

#[derive(StateData)]
pub struct LapinChannel {
    handle: Handle,
    addr: SocketAddr,
}

impl LapinChannel {
    pub fn queue<CB>(
        &self,
        exchange: &'static str,
        queue: &'static str,
        cb: CB,
    ) -> Box<Future<Item = (), Error = io::Error>>
    where
        CB: FnOnce(Channel<TcpStream>) -> Box<Future<Item = Channel<TcpStream>, Error = io::Error>>
            + 'static,
    {
        let handle = self.handle.clone();

        let f = TcpStream::connect(&self.addr, &handle)
            .and_then(|stream| {
                lapin::client::Client::connect(
                    stream,
                    &ConnectionOptions {
                        frame_max: 65535,
                        ..Default::default()
                    },
                )
            })
            .and_then(move |(client, heartbeat_future_fn)| {
                let heartbeat_client = client.clone();
                handle.spawn(heartbeat_future_fn(&heartbeat_client).map_err(|_| ()));

                client.create_confirm_channel(ConfirmSelectOptions::default())
            })
            .and_then(move |channel| {
                let id = channel.id;
                println!("created channel with id: {}", id);

                channel
                    .queue_declare(queue, &QueueDeclareOptions::default(), &FieldTable::new())
                    .map(|_| channel)
            })
            .and_then(move |channel| {
                println!("channel {} declared queue {}", channel.id, queue);
                channel
                    .exchange_declare(
                        exchange,
                        "direct",
                        &ExchangeDeclareOptions::default(),
                        &FieldTable::new(),
                    )
                    .map(|_| channel)
            })
            .and_then(move |channel| {
                println!("channel {} declared exchange {}", channel.id, exchange);
                channel
                    .queue_bind(
                        queue,
                        exchange,
                        queue,
                        &QueueBindOptions::default(),
                        &FieldTable::new(),
                    )
                    .map(|_| channel)
            })
            .and_then(move |channel| {
                println!("channel {} bound queue {}", channel.id, queue);

                cb(channel)
            })
            .and_then(|channel| {
                println!("closing amqp connection ...");
                channel.close(200, "Bye")
            })
            .and_then(|_| Ok(()));

        Box::new(f)
    }
}

/// A Gotham compatible Middleware that lets you dispatch messages to an AMQP queue.
pub struct LapinMiddleware {
    addr: SocketAddr,
}

impl LapinMiddleware {
    pub fn new(addr: SocketAddr) -> Self {
        LapinMiddleware { addr: addr }
    }
}

impl NewMiddleware for LapinMiddleware {
    type Instance = LapinMiddleware;

    fn new_middleware(&self) -> io::Result<Self::Instance> {
        Ok(LapinMiddleware {
            addr: self.addr.clone(),
        })
    }
}

impl Middleware for LapinMiddleware {
    fn call<Chain>(self, mut state: State, chain: Chain) -> Box<HandlerFuture>
    where
        Chain: FnOnce(State) -> Box<HandlerFuture>,
    {
        debug!("[{}] pre chain", request_id(&state));
        let handle = Handle::take_from(&mut state).clone();
        state.put(LapinChannel {
            handle: handle,
            addr: self.addr.clone(),
        });

        Box::new(chain(state))
    }
}
