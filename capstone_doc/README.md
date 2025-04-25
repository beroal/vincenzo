My capstone project “BitTorrent media streaming” for the Rust Bootcamp — Winter 2025.
The idea was chosen from a [list](ideas.md).

The following prerequisites would be helpful to understand this document:

- basic knowledge of the HTTP protocol
- the [unofficial BitTorrent protocol specification](https://wiki.theory.org/BitTorrentSpecification)
- the documentation for
  the [Vincenzo](https://github.com/gabrieldemian/vincenzo) BitTorrent client.

# Scope

A plugin for a BitTorrent client that streams media to a local media player.
The plugin sends media fragments as soon as they are downloaded via BitTorrent
so a user can start watching a movie immediately after starting downloading it.
The plugin will be embedded into a client written in Rust.
The plugin will support jumping to any point in media.
Media will be streamed via HTTP.
Actually, one of BitTorrent clients (rqbit) already
[does this](https://github.com/ikatson/rqbit#streaming-support),
but others
([synapse](https://github.com/Luminarys/synapse),
[Vincenzo](https://github.com/gabrieldemian/vincenzo),
[naryand/bittorrent](https://github.com/naryand/bittorrent),
[RustyBit](https://github.com/h33333333/rustybit),
[Rubit](https://github.com/spectre-xenon/rubit),
[Rusty Torrenter](https://github.com/ArloFilley/rusty_torrent#rusty-torrenter),
[xerus](https://gitlab.com/zenoxygen/xerus))
don't.

# Progress report

The BitTorrent client that the plugin would fit in was chosen to be Vincenzo.

My work consists of the following parts:

- fixing bugs in the client;
- adding the API for the plugin to the client;
- writing the plugin.

In the state I took the client it was mostly unusable.
It hanged, didn't support files larger than 4 GB,
saved corrupted torrent content to the file system, and fully loaded the CPU.
I fixed bugs and optimized it enough to demonstrate my plugin.
There are still bugs remaining.
This is despite the fact that the author of the client wrote tests.

The plugin allows multiple HTTP clients to access
files in multiple torrents simultaneously.
Multiple HTTP clients can access different parts of the same file simultaneously.
Users can seek in media files.
HTTP streaming is always on.
If there are no HTTP requests, the client downloads torrents as usual.

The UI was not extended to control the plugin.
A user communicates with the plugin through command-line options and the log.

# [Usage](usage.md)

# [Architecture](arch.md)

# To reviewers

While I modified the code of the client, maintaining the client is not my goal.
If you don't like its style, don't ask me to rewrite it.
I didn't want my modifications to be intrusive.
Focus on the independent code I wrote.
