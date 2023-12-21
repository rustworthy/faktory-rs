use std::io::{Read, Write};
use std::net::TcpStream;

use crate::proto::{host_from_url, learn_url, Client, Track};
use crate::{Error, Progress, ProgressUpdate};

/// Used to retrieve and update information on a job's execution progress.
pub struct Tracker<S: Read + Write> {
    c: Client<S>,
}

impl Tracker<TcpStream> {
    /// Create new tracker.
    pub fn connect(url: Option<&str>) -> Result<Tracker<TcpStream>, Error> {
        let url = learn_url(url)?;
        let addr = host_from_url(&url);
        let pwd = url.password().map(|p| p.to_string());
        let stream = TcpStream::connect(addr)?;
        Ok(Tracker {
            c: Client::new_tracker(stream, pwd)?,
        })
    }
}

impl<S: Read + Write> Tracker<S> {
    /// Connect to a Faktory server with a non-standard stream.
    pub fn connect_with(stream: S, pwd: Option<String>) -> Result<Tracker<S>, Error> {
        Ok(Tracker {
            c: Client::new_tracker(stream, pwd)?,
        })
    }

    /// Send information on a job's execution progress to Faktory .
    pub fn set_progress(&mut self, upd: ProgressUpdate) -> Result<(), Error> {
        let cmd = Track::Set(upd);
        self.c.issue(&cmd)?.await_ok()
    }

    /// Fetch information on a job's execution progress from Faktory.
    pub fn get_progress(&mut self, jid: String) -> Result<Option<Progress>, Error> {
        let cmd = Track::Get(jid);
        self.c.issue(&cmd)?.read_json()
    }
}

#[cfg(test)]
mod test {
    use crate::proto::{host_from_url, learn_url};

    use super::Tracker;
    use std::{env, net::TcpStream};

    #[test]
    fn test_trackers_created_ok() {
        if env::var_os("FAKTORY_URL").is_none() || env::var_os("FAKTORY_ENT").is_none() {
            return;
        }
        let _ = Tracker::connect(None).expect("tracker successfully instantiated and connected");

        let url = learn_url(None).expect("valid url");
        let host = host_from_url(&url);
        let stream = TcpStream::connect(host).expect("connected");
        let pwd = url.password().map(String::from);
        let _ = Tracker::connect_with(stream, pwd)
            .expect("tracker successfully instantiated and connected");
    }
}
