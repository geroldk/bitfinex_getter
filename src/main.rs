use chrono::prelude::*;
//use env_logger;
//use log::{debug, error, info};
use log;
use slog::{slog_o};
use slog_async;
use slog_stdlog;
use slog_scope::{info, error, warn, debug};
use slog::Drain;
use slog_journald;

use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use ws::{CloseCode, Error, Handler, Handshake, Message, Result, Sender, WebSocket};
use std::cell::RefCell;

#[derive(Serialize, Deserialize, Debug)]
struct TraidingPairs {
    url_symbol: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Symbol(String);

struct FileWrite {
    fs_ts: RefCell<Option<String>>,
    file: RefCell<Option<File>>,
}

struct Client<'a, 'b> {
    out: Sender,
    symbols: &'a [Symbol],
    file_write: &'b FileWrite,

}

impl FileWrite {
    fn new() -> FileWrite {
        FileWrite {
            fs_ts: RefCell::new(None),
            file: RefCell::new(None),
        }
    }
    fn build_file_name(ts: &str) -> String {
        let n = format!("data/bitfinex-ws-{}.log", ts);
        info!("{}", n);
        n
    }
    fn create_file(name: &str) -> File {
        let path = Path::new(name);
        OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap()
    }
    fn write(&self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let utc: DateTime<Utc> = Utc::now();
        let fs_ts = utc.format("%Y-%m-%d_%HZ").to_string();
        let gleich = self.fs_ts.borrow().as_ref().filter(|x| x == &&fs_ts).is_some();
        if !gleich {
            debug!("timestamp: {}", fs_ts);
            self.file.borrow_mut().as_mut().map(|x| {
                x.flush().unwrap();
                x
            }).map(|x| {
                x.sync_all().unwrap();
                x
            });

            self.fs_ts.replace(Some(fs_ts));
            self.file.replace(Some(FileWrite::create_file(&FileWrite::build_file_name(
                &self.fs_ts.borrow().as_ref().unwrap()),
            )));
        };

        self.file.borrow_mut().as_mut().unwrap().write(buf)
    }
}

impl<'a, 'b> Client<'a, 'b> {
    fn new(out: Sender, symbols: &'a [Symbol], fw: &'b FileWrite) -> Client<'a, 'b> {
        Client {
            out,
            symbols,
            file_write: fw,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribeMessageData {
    channel: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SubscribeMessage {
    event: String,
    data: SubscribeMessageData,
}


const CONF: & str = "{\"event\": \"conf\", \"flags\": 196608}";

static URL: & str = "wss://api-pub.bitfinex.com/ws/2";

impl<'a, 'b> Handler for Client<'a, 'b> {
    fn on_open(&mut self, shake: Handshake) -> Result<()> {
        if let Some(addr) = shake.remote_addr()? {
            info!("Connection with {} now open", addr);
        }
        self.out.send(CONF).unwrap();
        for symbol in self.symbols.iter() {
            let trade = format!("{{\"event\": \"subscribe\", \"channel\": \"trades\", \"symbol\": \"{}\"}}", symbol.0);
            let book = format!("{{\"event\": \"subscribe\", \"channel\": \"book\", \"prec\": \"R0\", \"symbol\": \"{}\", \"len\": 100 }}", symbol.0);
            //info!("send trade {} {}", count, trade);
            self.out.send(trade)?;
            //info!("send trade book {}", book);
            self.out.send(book)?;
            //info!("gesendet {}", count);
        }

        info!("subscribed");
        Ok(())
    }
    fn on_message(&mut self, msg: Message) -> Result<()> {
        //println!("{:?}", msg);
        let utc: DateTime<Utc> = Utc::now();
        if let Message::Text(s) = msg {
            let ts = format!("{:?}", utc);
            let mut mm = String::with_capacity(s.len() + ts.len() + 3);
            mm.push_str(&ts);
            mm.push_str(", ");
            mm.push_str(&s);
            mm.push_str("\n");
            self.file_write.write(mm.as_bytes()).unwrap();
        } else {
            error!("{:?}", msg);
        }
        Ok(())
    }
    fn on_close(&mut self, code: CloseCode, reason: &str) {
        info!("Connection closing due to ({:?}) {}", code, reason);
        self.out.connect( url::Url::parse(URL).unwrap()).unwrap();
    }
    fn on_error(&mut self, err: Error) {
        error!("{:?}", err);
    }
}

fn setup_logging() -> slog_scope::GlobalLoggerGuard {
    //let decorator = slog_term::TermDecorator::new().build();
    //let drain_term = slog_async::Async::new(slog_term::FullFormat::new(decorator).build().fuse()).build().fuse();
    let drain = slog_journald::JournaldDrain.fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    //let drain = Duplicate::new(drain_term, drain).fuse();
    // let drain = slog_envlogger::new( drain).fuse();
    //let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, slog_o!("version" => env!("CARGO_PKG_VERSION")));
    let _scope_guard = slog_scope::set_global_logger(logger);
    let _log_guard = slog_stdlog::init_with_level(log::Level::Info).unwrap();
    _scope_guard
}

fn main() {
    let _logging_guard = setup_logging();
    let resp = ureq::get("https://api.bitfinex.com/v1/symbols").call();

    // .ok() tells if response is 200-299.
    if resp.ok() {
        let j: Vec<Symbol> = serde_json::from_reader(resp.into_reader()).unwrap();
        let mut c = j.chunks(15);
        let count = c.len();
        //let cc = c.next();
        //let jj: Vec<Symbol> = j.into_iter().filter(|x| x.0.contains("btc")).collect();
        //println!("{:?}",j);
        //let jj: Vec<Symbol> = j.into_iter().map(|x| Symbol(x.url_symbol)).collect();
        info!("{}", "INFO"; "APP" => "BITFINEX");
        //debug!("{:?}", jj);
        let f = FileWrite::new();
        let mut ws = WebSocket::new(|out| Client::new(out, c.next().unwrap(), &f)).unwrap();
        for _i in  0..count {
            ws.connect( url::Url::parse(URL).unwrap()).unwrap();
        };
        ws.run().unwrap();
        //connect("wss://api-pub.bitfinex.com/ws/2", ).unwrap();
        std::process::exit(1);
    }
}
