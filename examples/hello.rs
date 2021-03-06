extern crate futures;
extern crate gotham;
extern crate gotham_middleware_lapin;
extern crate hyper;
extern crate lapin_futures as lapin;
extern crate mime;
extern crate tokio_core;

use futures::{future, Future};
use hyper::StatusCode;
use gotham::http::response::create_response;
use gotham::handler::HandlerFuture;
use gotham::state::State;
use gotham::router::Router;
use gotham::router::builder::*;
use gotham::state::FromState;
use gotham::pipeline::new_pipeline;
use gotham::pipeline::single::single_pipeline;
use gotham_middleware_lapin::{LapinChannel, LapinMiddleware};
use lapin::channel::{BasicProperties, BasicPublishOptions};

pub fn say_hello(state: State) -> Box<HandlerFuture> {
    Box::new(
        LapinChannel::borrow_from(&state)
            .queue("hello_ex", "hello_queue", |channel| {
                Box::new(
                    channel
                        .basic_publish(
                            "hello_ex",
                            "hello_queue",
                            format!("hello").as_bytes(),
                            &BasicPublishOptions::default(),
                            BasicProperties::default(),
                        )
                        .and_then(|_| future::ok(channel)),
                )
            })
            .then(|res| {
                if let Err(err) = res {
                    println!("error sending message: {}", err);
                }

                let res = create_response(
                    &state,
                    StatusCode::Ok,
                    Some((String::from("Hello, Lapin!").into_bytes(), mime::TEXT_PLAIN)),
                );
                Ok((state, res))
            }),
    )
}

fn router() -> Router {
    let lapin_mw = LapinMiddleware::new("127.0.0.1:5672".parse().unwrap());

    let (chain, pipelines) = single_pipeline(new_pipeline().add(lapin_mw).build());

    build_router(chain, pipelines, |route| {
        route.get("/").to(say_hello);
    })
}

pub fn main() {
    let addr = "127.0.0.1:7878";
    println!("Listening for requests at http://{}", addr);
    gotham::start(addr, router())
}
