// (Lines like the one below ignore selected Clippy rules
//  - it's useful when you want to check your code with `cargo make verify`
// but some rules are too "annoying" or are not applicable for your case.)
#![allow(clippy::wildcard_imports)]

use seed::{prelude::*, *};

// ------ ------
//     Init
// ------ ------

// `init` describes what should happen when your app started.
fn init(_: Url, _orders: &mut impl Orders<Msg>) -> Model {
    Model { 
        counter: 0,
        wsurl: "ws://127.0.0.1:1234".to_owned(),
        ws: None,
        errormsg: "".to_owned(),
    }
}

// ------ ------
//     Model
// ------ ------

// `Model` describes our app state.
struct Model {
    counter: i32,
    wsurl: String,
    ws: Option<WebSocket>,
    errormsg: String,
}

// ------ ------
//    Update
// ------ ------

// (Remove the line below once any of your `Msg` variants doesn't implement `Copy`.)
#[derive()]
// `Msg` describes the different events you can modify state with.
enum Msg {
    Increment,
    StartCounter,
    UpdateWsUrl(String),
    ToggleConnectWs,
    WsClosed,
    WsError,
    WsConnected,
    WsMessage(WebSocketMessage),
}

// `update` describes how to handle each `Msg`.
fn update(msg: Msg, model: &mut Model, orders: &mut impl Orders<Msg>) {
    match msg {
        Msg::Increment => model.counter += 1,
        Msg::StartCounter => {
            orders.stream(seed::app::streams::interval(1000, ||Msg::Increment));
        }
        Msg::UpdateWsUrl(x) => model.wsurl = x,
        Msg::ToggleConnectWs => {
            if let Some(ws) = model.ws.as_mut() {
                let _ = ws.close(None, None);
                model.ws = None;
            } else {
                let ws = seed::browser::web_socket::WebSocket::builder(&model.wsurl, orders)
                .on_open(||Msg::WsConnected)
                .on_close(|_|Msg::WsClosed)
                .on_error(||Msg::WsError)
                .on_message(|x| Msg::WsMessage(x))
                .build_and_open();
                match ws {
                    Ok(x) => {
                        model.ws = Some(x);
                    }
                    Err(e) => {
                        log(e);
                    }
                }
            }
            //orders.stream()
        }
        Msg::WsClosed => {
            model.errormsg = "WebSocket closed".to_owned();
            model.ws = None;
        }
        Msg::WsError =>  {
            model.errormsg = "WebSocket error".to_owned();
            model.ws = None;
        }
        Msg::WsConnected =>  {
            model.errormsg.clear();
        }
        Msg::WsMessage(x) =>  {
            if let Ok(t) = x.json() {
                let t: serde_json::Value = t;
                let _ = model.ws.as_mut().unwrap().send_json(&t);
            }
        }
    }
}

// ------ ------
//     View
// ------ ------

// `view` describes what to display.
fn view(model: &Model) -> Node<Msg> {
    div![
        div![
            "This is a counter: ",
            C!["counter"],
            button![model.counter, ev(Ev::Click, |_| Msg::StartCounter),],
        ],
        div![
            C!["errormsg"],
            &model.errormsg,
        ],
        div![
            "WebSocket URL:",
            input![
                C!["websocket-uri"],
                attrs!{ At::Value => model.wsurl },
                input_ev(Ev::Input, Msg::UpdateWsUrl),
            ],
            button![
                if model.ws.is_none() { "Connect" } else { "Disconnect"} ,
                ev(Ev::Click, |_|Msg::ToggleConnectWs),
            ],
        ],
    ]
}

// ------ ------
//     Start
// ------ ------

// (This function is invoked by `init` function in `index.html`.)
#[wasm_bindgen(start)]
pub fn start() {
    // Mount the `app` to the element with the `id` "app".
    App::start("app", init, update, view);
}
