// (Lines like the one below ignore selected Clippy rules
//  - it's useful when you want to check your code with `cargo make verify`
// but some rules are too "annoying" or are not applicable for your case.)
#![allow(clippy::wildcard_imports)]

use seed::{*, prelude::{*}};

// ------ ------
//     Init
// ------ ------

// `init` describes what should happen when your app started.
fn init(_: Url, orders: &mut impl Orders<Msg>) -> Model {
    
    let mut wsurl = "ws://127.0.0.1:1234".to_owned();
    if let Ok(mut durl) =  html_document().url() {
        let splits : Vec<&str> = durl.split('#').collect();
        if splits.len() >= 2 {
            wsurl = splits[1].to_owned();
            // assuming it would be rendered by that time:
            orders.perform_cmd(cmds::timeout(20, || Msg::AutoConnectAndFocusPassword));
        } else {
            if durl.starts_with("http") {
                durl = format!("ws{}",&durl[4..]);
            }
            if durl.ends_with(".html") {
                durl = format!("{}/ws", &durl[..(durl.len()-5)]);
                wsurl = durl;
            }
        }
    };

    let mut password = "".to_owned();

    if let Ok(x) = LocalStorage::get("password") {
        password = x;
    }

    orders.stream(seed::app::streams::interval(1000, ||Msg::SecondlyUpdate));
    Model { 
        wsurl,
        ws: None,
        errormsg: "".to_owned(),
        status: None,
        password,
        visible_password: false,
        conn_status: ConnStatus::Disconnected,
    }
}

// ------ ------
//     Model
// ------ ------

// `Model` describes our app state.
struct Model {
    wsurl: String,
    ws: Option<WebSocket>,
    errormsg: String,
    status: Option<SystemStatus>,
    password: String,
    visible_password: bool,
    conn_status: ConnStatus,
}


#[derive(Debug)]
enum ConnStatus {
    Disconnected,
    Unauthenticated,
    Connected,
}

#[derive(serde_derive::Deserialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamStatus {
    Disabled,
    Paused,
    Connecting,
    Connected,
    Mirroring,
}

#[derive(serde_derive::Deserialize, Debug)]
pub struct SystemStatus {
    upstream_status: UpstreamStatus,

    #[serde(flatten)]
    #[allow(dead_code)]
    rest: serde_json::Value,
}

#[derive(serde_derive::Serialize, Debug)]
#[serde(tag = "action", content = "data")]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)]
enum ControlMessage {
    Preroll(u64),
    Monitor,
    Filter(Vec<String>),
    RemoveRetainingLastN(u64),
    DatabaseSize,
    Status,
    Shutdown,
    PauseUpstream,
    ResumeUpstream,
    CursorToSpecificId(u64),
    Password(String),
    WriteConfig(serde_json::Value),
    ReadConfig,
}

#[derive(serde_derive::Deserialize, Debug)]
#[serde(tag = "T")]
#[serde(rename_all = "snake_case")]
enum ReplyMessage {
    Stats(SystemStatus),
    Error{msg:String},
    Hello{status:UpstreamStatus},
    Config(serde_json::Value),
}

// ------ ------
//    Update
// ------ ------

// (Remove the line below once any of your `Msg` variants doesn't implement `Copy`.)
#[derive()]
// `Msg` describes the different events you can modify state with.
enum Msg {
    SecondlyUpdate,
    UpdateWsUrl(String),
    UpdatePassword(String),
    ToggleConnectWs,
    WsClosed,
    WsError,
    WsConnected,
    WsMessage(WebSocketMessage),
    ToggleVisiblePassword,
    AutoConnectAndFocusPassword,
    SendPassword,
}

fn handle_ws_message(msg: ReplyMessage, model: &mut Model, _orders: &mut impl Orders<Msg>) {
    if ! matches! (msg, ReplyMessage::Stats{..}) {
        log(&msg);
    }
    model.conn_status = ConnStatus::Connected;
    match msg {
        ReplyMessage::Stats(x) => {
            model.status = Some(x);
        }
        ReplyMessage::Error{msg:x} => {
            if x.contains("Supply a password first") || x.contains("Invalid password") {
                model.conn_status = ConnStatus::Unauthenticated;
            } else {
                model.errormsg = format!("Error from server: {}", x);
            }
        }
        ReplyMessage::Hello{status:upstream_status} => {
            model.status = Some(SystemStatus{
                upstream_status,
                rest: serde_json::Value::Null,
            });
            if ! model.password.is_empty() {
                let _ = model.ws.as_ref().unwrap().send_json(&ControlMessage::Password(model.password.clone()));
            }
            let _ = model.ws.as_ref().unwrap().send_json(&ControlMessage::Status);
        }
        ReplyMessage::Config{0:x} => {
            drop(x);
        }
    }
}   

fn send_ws(cmsg: ControlMessage, model: &mut Model, _orders: &mut impl Orders<Msg>) {
    if let Some(ws) = model.ws.as_ref() {
        log(&cmsg);
        if let Err(e) = ws.send_json(&cmsg) {
            model.errormsg = format!("WebSocket error: {:?}", e);
        }
    } else {
        model.errormsg = "Error: WebSocket must be connected for this".to_owned();
    }
}

fn closews(model: &mut Model, _orders:  &mut impl Orders<Msg>) {
    if let Some(ws) = model.ws.as_mut() {
        let _ = ws.close(None, None);
        model.ws = None;
    }
    model.conn_status = ConnStatus::Disconnected;
}

// `update` describes how to handle each `Msg`.
fn update(msg: Msg, model: &mut Model, orders: &mut impl Orders<Msg>) {
    match msg {
        Msg::SecondlyUpdate => {
            if let Some(ws) = model.ws.as_ref() {
                if ! matches!(model.conn_status, ConnStatus::Unauthenticated) {
                    drop(ws);
                } 
            }
        }
        Msg::UpdateWsUrl(x) => model.wsurl = x,
        Msg::ToggleConnectWs => {
            if model.ws.is_some() {
                closews(model, orders);
            } else {
                let ws = seed::browser::web_socket::WebSocket::builder(&model.wsurl, orders)
                .on_open(||Msg::WsConnected)
                .on_close(|_|Msg::WsClosed)
                .on_error(||Msg::WsError)
                .on_message(|x| Msg::WsMessage(x))
                .build_and_open();
                match ws {
                    Ok(x) => {
                        // for in-browser debugging using web console:
                        let _ = js_sys::Reflect::set(&window(), &JsValue::from_str("ws"), x.raw_web_socket());

                        model.ws = Some(x);
                    }
                    Err(e) => {
                        model.errormsg = format!("{:?}", e);
                        log(e);
                    }
                }
            }
            //orders.stream()
        }
        Msg::WsClosed => {
            model.errormsg = "WebSocket closed".to_owned();
            closews(model, orders);
        }
        Msg::WsError =>  {
            model.errormsg = "WebSocket error".to_owned();
            closews(model, orders);
        }
        Msg::WsConnected =>  {
            model.errormsg.clear();
        }
        Msg::WsMessage(x) =>  {
            if let Ok(msgs) = x.json().map(|x:Vec<ReplyMessage>|x) {
                for t in msgs {
                    handle_ws_message(t, model, orders);
                }
            } else {
                log!("Invalid WebSocket message: {}", x.text());
            }
        }
        Msg::UpdatePassword(x) => model.password = x,
        Msg::SendPassword => {
            let _ = LocalStorage::insert("password", &model.password);
            send_ws(ControlMessage::Password(model.password.clone()), model, orders);
            model.conn_status=ConnStatus::Connected;
        },
        Msg::ToggleVisiblePassword => model.visible_password ^= true,
        Msg::AutoConnectAndFocusPassword => {
            //log("1");
            if let Some(pe) = html_document().get_element_by_id("passwordentry") {
                //log("2");
                if let Ok(fm) = js_sys::Reflect::get(pe.as_ref(), &JsValue::from_str("focus")) {
                    //log("3");
                    if let Some(fm) = wasm_bindgen::JsCast::dyn_ref(&fm) {
                        let _ = js_sys::Function::call0(fm, pe.as_ref());
                    }
                }
            }
            return update(Msg::ToggleConnectWs, model, orders);
        }
    }
    html_document().set_title(match model.conn_status {
        ConnStatus::Disconnected => "WsVw: diconnected",
        ConnStatus::Unauthenticated => "WsVw: passwd",
        ConnStatus::Connected => match &model.status {
            Some(us) => match us.upstream_status {
                UpstreamStatus::Connected => "WsVw",
                UpstreamStatus::Disabled => "WsVw: off",
                UpstreamStatus::Paused => "WsVw: paused",
                UpstreamStatus::Connecting => "WsVw: connecing",
                UpstreamStatus::Mirroring => "WsVw: mirroring",
            }
            None => "WsPrx: ?",
        }
    });
}

// ------ ------
//     View
// ------ ------

// `view` describes what to display.
fn view(model: &Model) -> Node<Msg> {
    div![
        div![
            C!["title"],
            "AlpacaProxy viewer"
        ],
        div![
            C!["websocketcontrol"],
            label![
                span!["WebSocket URL:"],
                input![
                    C!["websocket-uri"],
                    attrs!{ At::Value => model.wsurl, At::Type => "text" },
                    input_ev(Ev::Input, Msg::UpdateWsUrl),
                    keyboard_ev(Ev::KeyUp, |e| { if e.key() == "Enter" { Some(Msg::ToggleConnectWs) } else { None } }),
                ],
            ],
            button![
                if model.ws.is_none() { "Connect" } else { "Disconnect"} ,
                ev(Ev::Click, |_|Msg::ToggleConnectWs),
            ],
        ],
        div![
            C!["passwordcontrol"],
            label![
                span!["Password:"],
                input![
                    C!["password"],
                    id!["passwordentry"],
                    attrs! {
                        At::Type => if model.visible_password { "text" } else { "password" },
                        At::Value => model.password,
                    },
                    input_ev(Ev::Input, Msg::UpdatePassword),
                    keyboard_ev(Ev::KeyUp, |e| { if e.key() == "Enter" { Some(Msg::SendPassword) } else { None } }),
                ],
            ],
            label![
                C!["visiblepwd"],
                "Visible password",
                input![
                    attrs!{At::Type => "checkbox", At::Checked => model.visible_password.as_at_value()},
                    ev(Ev::Change, |_| Msg::ToggleVisiblePassword),
                ],
            ],
            button![
                "Send",
                ev(Ev::Click, |_|Msg::SendPassword),
            ],
        ],
        div![
            C!["errormsg"],
            &model.errormsg,
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
