use std::io::{Read, Write};
use std::net::TcpStream;

use crate::proto::{host_from_url, learn_url, Client, Track};
use crate::{Error, Progress, ProgressUpdate};

/// Used to retrieve and update info on job execution progress.
pub struct Tracker<S: Read + Write> {
    c: Client<S>,
}

impl Tracker<TcpStream> {
    /// Create new tracker
    pub fn new(url: Option<&str>) -> Result<Tracker<TcpStream>, Error> {
        let url = learn_url(url)?;
        let addr = host_from_url(&url);
        let pwd = url.password().map(|p| p.to_string());
        let stream = TcpStream::connect(addr)?;
        Ok(Tracker {
            c: Client::new_tracker(stream, pwd)?,
        })
    }

    /// describe me
    pub fn set_progress(&mut self, upd: ProgressUpdate) -> Result<(), Error> {
        let cmd = Track::Set(upd);
        self.c.issue(&cmd)?.await_ok()
    }

    /// describe me
    pub fn get_progress(&mut self, jid: String) -> Result<Option<Progress>, Error> {
        let cmd = Track::Get(jid);
        self.c.issue(&cmd)?.read_json()
    }
}

#[cfg(test)]
mod test {
    use super::Tracker;
    use std::env;

    #[test]
    fn test_trackers_created_ok() {
        if env::var_os("FAKTORY_URL").is_none() || env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
        let _ = Tracker::new(None).expect("tracker successfully instantiated and connected");
    }
}
