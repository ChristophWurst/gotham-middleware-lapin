extern crate futures;
extern crate gotham;
extern crate gotham_middleware_lapin;
extern crate hyper;
extern crate lapin_futures as lapin;
#[macro_use]
extern crate log;
extern crate mime;
extern crate tokio_core;

use futures::future::ok;
use futures::Stream;
use futures::future::Future;
use hyper::StatusCode;
use gotham::http::response::create_response;
use gotham::handler::HandlerFuture;
use gotham::state::State;
use gotham::router::Router;
use gotham::router::builder::*;
use gotham::pipeline::new_pipeline;
use gotham::pipeline::single::single_pipeline;
use gotham_middleware_lapin::{LapinChannel, LapinMiddleware};
use lapin::client::ConnectionOptions;
use lapin::channel::{BasicProperties, BasicPublishOptions, Channel, ConfirmSelectOptions,
                     ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use tokio_core::reactor::Core;
use tokio_core::net::TcpStream;

pub fn say_hello(state: State) -> Box<HandlerFuture> {
    let res = create_response(
        &state,
        StatusCode::Ok,
        Some((String::from("Hello, Lapin!").into_bytes(), mime::TEXT_PLAIN)),
    );

    Box::new(ok((state, res)))
}

fn router(channel: Channel<TcpStream>) -> Router {
    let lapin_mw = LapinMiddleware::new(channel);

    let (chain, pipelines) = single_pipeline(new_pipeline().add(lapin_mw).build());

    build_router(chain, pipelines, |route| {
        route.get("/").to(say_hello);
    })
}

pub fn main() {
    // create the reactor
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let addr = "127.0.0.1:5672".parse().unwrap();
    core.run(
        TcpStream::connect(&addr, &handle)
            .and_then(|stream| {
                // connect() returns a future of an AMQP Client
                // that resolves once the handshake is done
                lapin::client::Client::connect(
                    stream,
                    &ConnectionOptions {
                        frame_max: 65535,
                        ..Default::default()
                    },
                )
            })
            .and_then(|(client, heartbeat_future_fn)| {
                let heartbeat_client = client.clone();
                handle.spawn(heartbeat_future_fn(&heartbeat_client).map_err(|_| ()));

                client.create_confirm_channel(ConfirmSelectOptions::default())
            })
            .and_then(|channel| {
                let id = channel.id;
                println!("created channel with id: {}", id);

                channel
                    .queue_declare("raw", &QueueDeclareOptions::default(), &FieldTable::new())
                    .map(|_| channel)
            })
            .and_then(|channel| {
                println!("channel {} declared queue {}", channel.id, "raw");
                channel
                    .exchange_declare(
                        "raw_ex",
                        "direct",
                        &ExchangeDeclareOptions::default(),
                        &FieldTable::new(),
                    )
                    .map(|_| channel)
            })
            .and_then(|channel| {
                println!("channel {} declared exchange {}", channel.id, "raw_ex");
                channel
                    .queue_bind(
                        "raw",
                        "raw_ex",
                        "raw_2",
                        &QueueBindOptions::default(),
                        &FieldTable::new(),
                    )
                    .map(|_| channel)
            })
            .and_then(|channel| {
                info!("channel {} declared queue {}", channel.id, "hello");

                let addr = "127.0.0.1:7878";
                println!("Listening for requests at http://{}", addr);
                gotham::start(addr, router(channel.clone()));

                channel
                    .basic_publish(
                        "raw_ex",
                        "raw_2",
                        b"hello from Gotham",
                        &BasicPublishOptions::default(),
                        BasicProperties::default(),
                    )
                    .join(ok(channel))
            })
            .map(|(confirmation, channel)| {
                println!("publish got confirmation: {:?}", confirmation);
                channel
            })
            .and_then(|channel| {
                println!("closing amqp connection …");
                channel.close(200, "Bye")
            }),
    ).expect("fail");
}
