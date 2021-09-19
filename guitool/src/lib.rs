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
                durl = format!("{}/ws", &durl[(durl.len()-5)..]);
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
        allow_shutdown: false,
        show_config_editor: false,
        config: "".to_owned(),
        allow_sending_status_inquiries: true,
        conn_status: ConnStatus::Disconnected,
        show_raw_status: false,
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
    allow_shutdown: bool,
    show_config_editor: bool,
    visible_password: bool,
    config: String,
    allow_sending_status_inquiries: bool,
    conn_status: ConnStatus,
    show_raw_status: bool,
}


#[derive(Debug)]
enum ConnStatus {
    Disconnected,
    Unauthenticated,
    Connected,
    Lagging,
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
    database_size: u64,
    first_datum_id: Option<u64>,
    last_datum_id: Option<u64>,
    clients_connected: usize,
    last_ticker_update_ms: Option<u64>,
    upstream_status: UpstreamStatus,
    new_tickers_this_session: usize,
    server_version: String,

    #[serde(flatten)]
    rest: serde_json::Value,
}

#[derive(serde_derive::Serialize, Debug)]
#[serde(tag = "stream", content = "data")]
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
#[serde(tag = "stream", content = "data")]
#[serde(rename_all = "snake_case")]
enum ReplyMessage {
    Stats(SystemStatus),
    Error(String),
    Hello(UpstreamStatus),
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
    UpdateConfig(String),
    ToggleConnectWs,
    WsClosed,
    WsError,
    WsConnected,
    WsMessage(WebSocketMessage),
    PauseUpstream,
    ResumeUpstream,
    SendPassword,
    ToggleAllowServerShutdown,
    SendServerShutdown,
    ToggleConfigEditorDisplay,
    ReadConfig,
    WriteConfig,
    ToggleVisiblePassword,
    AutoConnectAndFocusPassword,
    ToggleShowRawStatus,
}

fn handle_ws_message(msg: ReplyMessage, model: &mut Model, _orders: &mut impl Orders<Msg>) {
    if ! matches! (msg, ReplyMessage::Stats(..)) {
        log(&msg);
    }
    model.conn_status = ConnStatus::Connected;
    match msg {
        ReplyMessage::Stats(x) => {
            model.allow_sending_status_inquiries = true;
            model.status = Some(x);
        }
        ReplyMessage::Error(x) => {
            model.allow_sending_status_inquiries = true;
            if x.contains("Supply a password first") || x.contains("Invalid password") {
                model.conn_status = ConnStatus::Unauthenticated;
            } else {
                model.errormsg = format!("Error from server: {}", x);
            }
        }
        ReplyMessage::Hello(upstream_status) => {
            model.status = Some(SystemStatus{
                database_size: 0,
                clients_connected: 0,
                first_datum_id: None,
                last_datum_id: None,
                last_ticker_update_ms: None,
                new_tickers_this_session: 0,
                server_version: "".to_owned(),
                upstream_status,
                rest: serde_json::Value::Null,
            });
            if ! model.password.is_empty() {
                let _ = model.ws.as_ref().unwrap().send_json(&ControlMessage::Password(model.password.clone()));
            }
            let _ = model.ws.as_ref().unwrap().send_json(&ControlMessage::Status);
        }
        ReplyMessage::Config(x) => {
            if let Ok(y) = serde_json::to_string_pretty(&x) {
                model.config = y;
            } else {
                model.errormsg = "Strange thing instead of config".to_owned();
            }
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
                    if model.allow_sending_status_inquiries {
                        let _ = ws.send_json(&ControlMessage::Status);
                        model.allow_sending_status_inquiries = false;
                    } else {
                        model.conn_status = ConnStatus::Lagging;
                    }
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
            if let Ok(t) = x.json() {
                handle_ws_message(t, model, orders);
            } else {
                log!("Invalid WebSocket message: {}", x.text());
            }
        }
        Msg::UpdatePassword(x) => model.password = x,
        Msg::SendPassword => {
            let _ = LocalStorage::insert("password", &model.password);
            send_ws(ControlMessage::Password(model.password.clone()), model, orders);
            model.conn_status=ConnStatus::Connected;
            model.allow_sending_status_inquiries = true;
        },
        Msg::ToggleAllowServerShutdown => {
            model.allow_shutdown = !model.allow_shutdown;
        }
        Msg::PauseUpstream => send_ws(ControlMessage::PauseUpstream,model,orders),
        Msg::ResumeUpstream => send_ws(ControlMessage::ResumeUpstream,model,orders),
        Msg::SendServerShutdown => {
            if model.allow_shutdown {
                send_ws(ControlMessage::Shutdown, model, orders);
            } else {
                model.errormsg = "Check the checkbox to enable this function".to_owned();
            }
        }
        Msg::ToggleConfigEditorDisplay => {
            model.show_config_editor = !model.show_config_editor;
        }
        Msg::ReadConfig => {
            send_ws(ControlMessage::ReadConfig, model, orders);
        }
        Msg::WriteConfig => {
            match serde_json::from_str(&model.config) {
                Err(e) => {
                    model.errormsg = format!("Input is not valid JSON: {}", e);
                }
                Ok(x) => {
                    let x : serde_json::Value = x;
                    send_ws(ControlMessage::WriteConfig(x), model, orders);
                }
            }
        }
        Msg::ToggleVisiblePassword => model.visible_password ^= true,
        Msg::UpdateConfig(x) => model.config = x,
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
        Msg::ToggleShowRawStatus => model.show_raw_status ^= true,
    }
    html_document().set_title(match model.conn_status {
        ConnStatus::Disconnected => "WsPrx: diconnected",
        ConnStatus::Lagging => "WsPrx: lagging",
        ConnStatus::Unauthenticated => "WsPrx: passwd",
        ConnStatus::Connected => match &model.status {
            Some(us) => match us.upstream_status {
                UpstreamStatus::Connected => "WsPrx",
                UpstreamStatus::Disabled => "WsPrx: off",
                UpstreamStatus::Paused => "WsPrx: paused",
                UpstreamStatus::Connecting => "WsPrx: connecing",
                UpstreamStatus::Mirroring => "WsPrx: mirroring",
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
            "AlpacaProxy control panel"
        ],
        div![
            C!["mainstatus"],
            div![
                C!["connstatus"],
                C![format!("connstatus_{:?}", model.conn_status)],
            ],
            div![
                C!["serverstatus"],
                if let Some(ss) = &model.status {
                    match ss.upstream_status {
                        UpstreamStatus::Connected => {
                            if let Some(ltu) = ss.last_ticker_update_ms {
                                if ltu > 120 {
                                    C!["serverstatus_ConnectedNoRecv"]
                                } else {
                                    C!["serverstatus_Connected"]
                                }
                            } else {
                                C!["serverstatus_ConnectedNoTU"]
                            }
                        },
                        UpstreamStatus::Mirroring => {
                            if let Some(ltu) = ss.last_ticker_update_ms {
                                if ltu > 120 {
                                    C!["serverstatus_MirroringNoRecv"]
                                } else {
                                    C!["serverstatus_Mirroring"]
                                }
                            } else {
                                C!["serverstatus_MirroringNoTU"]
                            }
                        }
                        x => C![format!("serverstatus_{:?}", x )],
                    }
                } else {
                    C!["serverstatus_None"]
                },
            ],
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
            C!["mainactions"],
            label![
                "Allow server shutdown",
                input![
                    C!["allowshutdown"],
                    attrs!{At::Type => "checkbox", At::Checked => model.allow_shutdown.as_at_value()},
                    ev(Ev::Change, |_| Msg::ToggleAllowServerShutdown),
                ],
            ],
            button![ "⏸️ Pause upstream", C!["pause"], ev(Ev::Click, |_|Msg::PauseUpstream)],
            button![ "▶️ Resume upstream", C!["resume"], ev(Ev::Click, |_|Msg::ResumeUpstream)],
            button![ "🛑 Server shutdown", C!["shutdown"], ev(Ev::Click, |_|Msg::SendServerShutdown)],
        ],
        div![
            C!["configeditor"],
            div![
                label![
                    C!["showconfig"],
                    "Show config editor",
                    input![
                        attrs!{At::Type => "checkbox", At::Checked => model.show_config_editor.as_at_value()},
                        ev(Ev::Change, |_| Msg::ToggleConfigEditorDisplay),
                    ],
                ],
                if model.show_config_editor {
                    div![
                        C!["configbuttons"],
                        button![ "Read config", C!["readconfig"], ev(Ev::Click, |_|Msg::ReadConfig)],
                        button![ "Write config", C!["writeconfig"], ev(Ev::Click, |_|Msg::WriteConfig)],
                    ]
                } else {
                    div![]
                },
            ],
            if model.show_config_editor {
                textarea![
                    C!["configtext"],
                    attrs!{ At::Value => model.config },
                    input_ev(Ev::Input, Msg::UpdateConfig),
                ]
            } else {
                div![]
            },
        ],
        div![
            C!["errormsg"],
            &model.errormsg,
        ],
        div![
            C!["systemstatus"],
            if let Some(st) = &model.status {
                div![
                    C!["nicestatus"],
                    div![
                        span!["Database size:"],
                        span![C!["val"], format!("{}B", size_format::SizeFormatterBinary::new(st.database_size))],
                        if let (Some(start), Some(end)) = (st.first_datum_id, st.last_datum_id) {
                            span![format!(" ({} samples, {} new)",  end+1-start, st.new_tickers_this_session)]
                        } else {
                            span![]
                        }
                    ],
                    div![
                        span!["Server version:"],
                        span![C!["val"], &st.server_version],
                        span!["Clients currently connected:"],
                        span![C!["val"], st.clients_connected],
                    ],
                    if let Some(ltr) = st.last_ticker_update_ms {
                        div![
                            span!["Last sample received "],
                            span![C!["val"], timeago::Formatter::new().convert(std::time::Duration::from_millis(ltr))],
                        ]
                    } else {
                        div!["Waiting for the first sample to arrive"]
                    },
                ]
            } else {
                div![C!["nonicestatus"]]
            },
           
            label![
                C!["showrawstatus"],
                "Show raw status",
                input![
                    attrs!{At::Type => "checkbox", At::Checked => model.show_raw_status.as_at_value()},
                    ev(Ev::Change, |_| Msg::ToggleShowRawStatus),
                ],
            ],
            if model.show_raw_status {
                pre![
                    C!["rawstatus"],
                    if let Some(ss) = &model.status {
                        format!("{:#?}", ss)
                    } else {
                        "Not connected".to_owned()
                    }
                ]
            } else {
                pre![]
            },
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
