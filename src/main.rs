
use std::{collections::HashMap, any};
use serde::{ser::Serializer, Serialize, Deserialize};
use reqwest::IntoUrl;
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, RANGE};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};

use std::sync::Arc;
use tokio::sync::Mutex;
use futures_util::StreamExt;

#[macro_use] extern crate scan_fmt;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] std::io::Error),
  #[error(transparent)]
  Request(#[from] reqwest::Error),
  #[error(transparent)]
  UrlParse(#[from] url::ParseError),
  #[error("request failed, err:`{0}`")]
  RequestFail(String),
}

impl Serialize for Error {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
      S: Serializer,
    {
      serializer.serialize_str(self.to_string().as_ref())
    }
  }


  #[tokio::main]
  async fn main() {
    println!("Hello, world!");

    let scroll_id = postCreate().await.unwrap();

    println!("scroll_id: {}\n", scroll_id);

    let path = "/Users/joe/code/rust-projects/http_download/disk.dmg";
    let file = File::create(&path).await.unwrap();

    let muxFile = Arc::new(Mutex::new(file));

    let mut total_len = 0;
    let mut last_timesec_data = LastTimesecData(0,0);
    let response = loop {
        let local_file = Arc::clone(&muxFile);
        let result = getData(scroll_id.as_str(), local_file).await;
        match result {
            Ok((_data_len, _has_more, _last_timesec_data)) => { 
                total_len += _data_len; 
                if !_has_more {
                    break Ok(())
                }
                if _last_timesec_data.0 != last_timesec_data.0 {
                    last_timesec_data.1 += _last_timesec_data.1
                } else {
                    last_timesec_data = _last_timesec_data
                }
            }
            Err(e) => {
                break Err(e);
            }
        };
    }.unwrap();

    //progress
    

    println!("file download ok!");

}

#[derive(Serialize, Debug)]
struct PostCreateReq {
    netlink_id: u32,
    start_time: u64,
    end_time: u64,
    filter: Option<String>,
}
#[derive(Deserialize, Debug)]
struct PostCreateRes {
    scroll_id: String,
}

#[derive(Deserialize, Debug)]
struct Response {
    code: String,
    msg: String,
    data: Option<PostCreateRes>,
}

struct LastTimesecData(u64, u64);

struct Progress {
    total_len: u64,
    tail_time_sec: u64,
    tail_data_len: u64,
}

async fn postCreate() -> Result<String> {
    let endpoint = String::from("http://localhost:8080/");
    let mut url = url::Url::parse(endpoint.as_str())?;
    url.set_path("/v1/download/_scroll");

    print!("url:{}\n", url.to_string());

    let reqBody = PostCreateReq{
        netlink_id: 1,
        start_time: 123,
        end_time: 456,
        filter: None,
    };

     let req = reqwest::Client::new()
        .post(url)
        .bearer_auth("token123456")
        .json(&reqBody);

     let resp = req.send().await?;

     if !resp.status().is_success() {
        return Err(Error::RequestFail(format!("status is {}", resp.status())))
     }

     let resObj = resp.json::<Response>().await?;
     if let Some(data) = resObj.data {
        return Ok(data.scroll_id)
     }
     Err(Error::RequestFail(format!("resp data is null")))
}


async fn getData(scroll_id: &str, file: Arc<Mutex<File>>) -> Result<(u64, bool, LastTimesecData)> {

    let endpoint = String::from("http://localhost:8080/");
    let mut url = url::Url::parse(endpoint.as_str())?;
    url.set_path("/v1/download/_scroll");

    let req = reqwest::Client::new()
        .get(url)
        .bearer_auth("token123456")
        .query(&[("scroll_id", scroll_id)]);

    let resp = req.send().await?;
    if !resp.status().is_success() {
        return Err(Error::RequestFail(format!("status is {}", resp.status())))
     }

     let headers = resp.headers();
     
     let hasMore = headers.get("continue").map(|val| val.to_str().ok())
     .flatten()
     .map(|val| val.parse::<bool>().ok())
     .flatten()
     .ok_or(Error::RequestFail(String::from("get continue failed")))?;


     let dataRange = headers.get("data-range").map(|val| val.to_str().ok())
     .flatten()
     .map(|val| scan_fmt!(val, "{}:{}-{}:{}", u64, u32, u64, u32).ok())
     .flatten()
     .ok_or(Error::RequestFail(String::from("get data-range failed")))?;

     let mut stream = resp.bytes_stream();

     let mut writed_len = 0;

     while let Some(chunk) = stream.next().await {
        let mut chunk = chunk?;
        let mut file = file.lock().await;
        writed_len += chunk.len() as u64;
        file.write_all_buf(&mut chunk).await?;
    }

    Ok((writed_len, hasMore, LastTimesecData(dataRange.2, dataRange.3 as u64)))
}