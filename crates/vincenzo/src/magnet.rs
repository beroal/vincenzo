//! Handle magnet link
use std::ops::{Deref, DerefMut};
use std::net::{SocketAddr, AddrParseError};

use magnet_url::Magnet as Magnet_;

use crate::error::Error;

#[derive(Debug, Clone, Hash)]
pub struct Magnet {
    inner: Magnet_,
    x_pe: Vec<SocketAddr>,
}

impl Deref for Magnet {
    type Target = Magnet_;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Magnet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[derive(thiserror::Error, Debug)]
enum XPeError {
    #[error("when parsing a magnet URI")]
    Uri(#[from] uriparse::URIError),

    #[error("when parsing a peer address")]
    Address(#[from] AddrParseError),
}

fn parse_x_pe(magnet_url: &str) -> Result<Vec<SocketAddr>, XPeError> {
    let uri = uriparse::uri::URI::try_from(magnet_url)?;
    match uri.query() {
        None => Ok(Default::default()),
        Some(query) => {
            let iter = form_urlencoded::parse(query.as_ref())
                .filter_map(|(key, value)|
                    if key == "x.pe" { Some(value.parse()) } else { None }
                );
            let r: Result<Vec<std::net::SocketAddr>, _> = Result::from_iter(iter);
            Ok(r?)
        },
    }
}

impl Magnet {
    pub fn new(magnet_url: &str) -> Result<Self, Error> {
        Ok(Self {
            inner: Magnet_::new(magnet_url).map_err(|_| Error::MagnetLinkInvalid)?,
            x_pe: parse_x_pe(magnet_url).map_err(|_| Error::MagnetLinkInvalid)?,
        })
    }

    /// The name will come URL encoded, and it is also optional.
    pub fn parse_dn(&self) -> String {
        if let Some(dn) = self.dn.clone() {
            if let Ok(dn) = urlencoding::decode(&dn) {
                return dn.to_string();
            }
        }
        "Unknown".to_string()
    }

    /// Transform the "xt" field from hex, to a slice.
    pub fn parse_xt(&self) -> [u8; 20] {
        let info_hash = hex::decode(self.xt.clone().unwrap()).unwrap();
        let mut x = [0u8; 20];

        x[..20].copy_from_slice(&info_hash[..20]);
        x
    }

    /// Parse trackers so they can be used as socket addresses.
    pub fn parse_trackers(&self) -> Vec<String> {
        let tr: Vec<String> = self
            .tr
            .clone()
            .iter_mut()
            .filter(|x| x.starts_with("udp"))
            .map(|x| {
                *x = urlencoding::decode(x).unwrap().to_string();
                *x = x.replace("udp://", "");

                // remove any /announce
                if let Some(i) = x.find('/') {
                    *x = x[..i].to_string();
                };

                x.to_owned()
            })
            .collect();
        tr
    }

    /// Peer addresses. The values associated with `x.pe` entries
    /// in the query of the magnet URI.
    /// See the chapter “magnet URI format”
    /// in [BEP 9](https://bittorrent.org/beps/bep_0009.html)
    /// or [“Magnet URI scheme”](https://en.wikipedia.org/wiki/Magnet_URI_scheme#Format).
    pub fn parse_x_pe(&self) -> &Vec<SocketAddr> {
        &self.x_pe
    }
}
