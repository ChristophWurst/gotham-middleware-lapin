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
use tokio_core::reactor::Handle;

pub fn say_hello(mut state: State) -> Box<HandlerFuture> {
    let sending = {
        let handle = Handle::take_from(&mut state);
        LapinChannel::borrow_from(&state).channel(&handle, |channel| {
            let f = channel
                .basic_publish(
                    "raw_ex",
                    "raw_2",
                    format!("hello").as_bytes(),
                    &BasicPublishOptions::default(),
                    BasicProperties::default(),
                )
                .and_then(|_| future::ok(channel));

            Box::new(f)
        })
    };

    let f = sending.then(|res| {
        if let Err(err) = res {
            println!("error sending message: {}", err);
        }

        let res = create_response(
            &state,
            StatusCode::Ok,
            Some((String::from("Hello, Lapin!").into_bytes(), mime::TEXT_PLAIN)),
        );
        Ok((state, res))
    });

    Box::new(f)
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
