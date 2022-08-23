
use futures::TryStreamExt;
use serde::{ser::Serializer, Serialize};
use tauri::{command, plugin::Plugin, Invoke, Runtime, Window};
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

use read_progress_stream::ReadProgressStream;

use std::{collections::HashMap, sync::Mutex};

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
  #[error(transparent)]
  Io(#[from] std::io::Error),
  #[error(transparent)]
  Request(#[from] reqwest::Error),
}

impl Serialize for Error {
  fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
  where
    S: Serializer,
  {
    serializer.serialize_str(self.to_string().as_ref())
  }
}

#[derive(Clone, Serialize)]
struct ProgressPayload {
  id: u32,
  progress: u64,
  total: u64,
}

#[command]
async fn upload<R: Runtime>(
  window: Window<R>,
  id: u32,
  url: &str,
  file_path: &str,
  headers: HashMap<String, String>,
) -> Result<serde_json::Value> {
  // Read the file
  let file = File::open(file_path).await?;

  // Create the request and attach the file to the body
  let client = reqwest::Client::new();
  let mut request = client.post(url).body(file_to_body(id, window, file));

  // Loop trought the headers keys and values
  // and add them to the request object.
  for (key, value) in headers {
    request = request.header(&key, value);
  }

  let response = request.send().await?;

  response.json().await.map_err(Into::into)
}

fn file_to_body<R: Runtime>(id: u32, window: Window<R>, file: File) -> reqwest::Body {
  let stream = FramedRead::new(file, BytesCodec::new()).map_ok(|r| r.freeze());
  let window = Mutex::new(window);
  reqwest::Body::wrap_stream(ReadProgressStream::new(
    stream,
    Box::new(move |progress, total| {
      let _ = window.lock().unwrap().emit(
        "upload://progress",
        ProgressPayload {
          id,
          progress,
          total,
        },
      );
    }),
  ))
}