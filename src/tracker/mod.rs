use std::io::{Read, Write};
use std::marker::PhantomData;
use std::net::TcpStream;

use crate::proto::{host_from_url, learn_url, Client};
use crate::Error;

pub struct Reader;
pub struct _Updater;

/// Used to retrieve and update info on job execution progress.
pub struct Tracker<T, S: Read + Write> {
    _inner: Client<S>,
    side: PhantomData<T>,
}

impl<T> Tracker<T, TcpStream> {
    /// describe me
    pub fn reader(url: Option<&str>) -> Result<Tracker<Reader, TcpStream>, Error> {
        let url = learn_url(url)?;
        let addr = host_from_url(&url);
        let pwd = url.password().map(|p| p.to_string());
        let stream = TcpStream::connect(addr)?;
        Ok(Tracker {
            _inner: Client::new_producer(stream, pwd)?,
            side: PhantomData,
        })
    }
}
