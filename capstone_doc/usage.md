# Usage

The program isn't user friendly.
You need to compile it yourself and communicate with it
through command-line options and the log.

## Compilation

Compile it with Cargo as usual for Rust programs.
It requires the nightly compiler for the `impl_trait_in_assoc_type` feature.
For a starter, it's enough to compile the `vcz` crate.

## Running

The client offers several ways to run it, but the simplest is
to run the `vcz` crate, that is, the executable file `target/*/vcz`.
You may learn about the client command-line options in the client documentation.

It's best to observe the plugin
when the client only communicates with peers controlled by you.
That is, you run another mature client which already has the desired torrent
and forbid Vincenzo to get peer addresses from trackers.
This way, you can vary the download speed
by setting the upload speed of the controlled peer.
I added command-line options to Vincenzo to achieve this (see below).

The download behavior of the client in the wild is glitchy.
To see any downloading, choose torrents with at least 50 peers.

Use the command-line option `-d $X` where `$X` is the directory
where the client saves torrents it downloads.

Use the command-line option `-m $X`
where `$X` is the magnet URI of the desired torrent.
An example magnet URI
of a [short movie “Tears of Steel” (2012)](magnet:?xt=urn:btih:1EF2640A24086AE02E3F5BAB423580E4C1C27DE6&dn=Tears+of+Steel+%282012%29+Sci-Fi+BR2DVD+DD2.0+NL+Subs&tr=http%3A%2F%2Ffr33dom.h33t.com%3A3310%2Fannounce&tr=udp%3A%2F%2Ffr33domtracker.h33t.com%3A3310%2Fannounce&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A80&tr=udp%3A%2F%2Ftracker.1337x.org%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.istole.it%3A80&tr=udp%3A%2F%2Ftracker.ccc.de%3A80&tr=udp%3A%2F%2F9.rarbg.com%3A2710%2Fannounce&tr=udp%3A%2F%2F11.rarbg.com%3A80%2Fannounce&tr=udp%3A%2F%2F10.rarbg.com%2Fannounce&tr=udp%3A%2F%2Ftracker.publicbt.com%3A80%2Fannounce&tr=http%3A%2F%2Fcpleft.com%3A2710%2Fannounce&tr=http%3A%2F%2Ftracker.torrentbay.to%3A6969%2Fannounce&tr=http%3A%2F%2Fipv4.tracker.harry.lu%2Fannounce&tr=http%3A%2F%2Fretracker.kld.ru%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=http%3A%2F%2Ftracker.openbittorrent.com%3A80%2Fannounce&tr=udp%3A%2F%2Fopentracker.i2p.rocks%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.internetwarriors.net%3A1337%2Fannounce&tr=udp%3A%2F%2Ftracker.leechers-paradise.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fcoppersurfer.tk%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.zer0day.to%3A1337%2Fannounce)
(571 MB, 15 peers) published under a Creative Commons licence.

It's okay if the client fully loads the CPU. The client is inefficient.
Switching off logging increases its speed significantly.
You can switch off logging by editing the `vcz` crate.

### HTTP server (plugin)

In order to run the plugin, include the command-line option
`--http-server-addr $HTTP_ADDRESS` where `$HTTP_ADDRESS`
is the address you want the HTTP server to listen.
HTTP streaming is always on if the plugin is enabled
via the command line options.

The HTTP server only serves torrent files, no HTML pages.
It would be convenient for users to show file URLs via UI,
but I had no time to modify the UI of the client.
Thus file URLs are shown through the client log.
The log is in the files `/tmp/vcz-*.log` on Unix.
You will find URL paths on lines
containing a text of the form `ui_uri_path=$URL_PATH`.
Thus the URL of the file is
```
http://$HTTP_ADDRESS$URL_PATH
```

The latency of media playback through the HTTP server
greatly depends on the size of peer request queues.
This parameter is controlled by the constant `peer::MAX_REQ_QUEUE_LEN`
in the source code of the `vincenzo` crate.
The current value of `256` is good for streaming torrents
of bitrate approximately 8 Mb/s and torrent piece size less than 2 MB.
On real torrents expect latency of at least 10 seconds.
The current value may be too small if you care about download rate and not latency.

### Controlled peers

The command-line option `--no-tracker` prevents the client
from getting peer addresses from BitTorrent trackers.

In order to tell the client about your controlled peer,
add the HTTP query parameter `x.pe` with the value
which is the address of the controlled peer to the magnet URI.
In simple words, add `&x.pe=$X` to the end of the magnet URI
where `$X` is the address of the controlled peer.
You can read the TCP port of the controlled peer in its settings.
I used Transmission.
