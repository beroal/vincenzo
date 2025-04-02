# Architecture

## A crash course in BitTorrent

The *BitTorrent protocol* is a protocol for decentralized transfer of files.
Parts of a single file may and usually are downloaded
from different hosts simultaneously.
A program implementing the BitTorrent protocol
is called a *BitTorrent client*. (There are no servers.)
An internet host running a BitTorrent client is called a *peer*.
Anybody can become a peer.
Methods of finding peers aren't relevant to this project.

A *torrent* is reference to a piece of content.
A torrent contains a small file system (that is, directories and files).
The contents of a torrent are described by the *info entry*
of a *torrent file* (also called *metainfo file* or *metadata file*).
The info entry contains enough information to download the torrent content
from the BitTorrent network, but doesn't contains the content itself.
The info entry contains the directory structure and the list of files.
The index of a file in this list is convenient
to identify the file in the torrent.

Content downloaded via the BitTorrent protocol is validated
against cryptographically secure hashes. Since anybody can be a peer,
content that was not validated may contain garbage or viruses.
Playing such media content is a bad idea.
Hashes are stored in the info entry.

Hashes of torrent content are calculated in the following way.
Files of the torrent are concatenated in the order defined in the torrent,
and the result is split into fixed-sized *pieces*.
The piece size is defined in the info entry.
The info entry contains the list of the hashes of the pieces.

Thus a client should give piece contents to a media player
only after downloading and validating the **whole** piece.
This peculiarity influences latency of playback.
The hash of the info entry, called *info hash* (also *infohash*),
is commonly used to identify torrents.
Magnet links contain info hashes.

## A crash course in HTTP media streaming

The *bitrate* of a media file is the data rate at which a media player
consumes the content of the file while playing it at normal speed.
If the total download speed from all peers is less then the bitrate,
playback will stop periodically which makes watching movies very uncomfortable.
When `n` media players are playing simultaneously,
the total download speed should be greater than `n * bitrate`.

A *media player head* is the place in the media file
that the media player plays at this moment.
A user *seeks* in a media file when they move the media player head
to a new place in the media file.

The HTTP protocol supports downloading parts of HTTP resources
using the `GET` method with the *Range header*.
The Range header describes an interval (range) in an HTTP resource.
Both ends of the interval are written inclusive and in bytes.

When playing a media file through HTTP, a media player makes a `GET` request
to the media file with the Range header containing the interval
from the media player head to the end of the media file.
Then the media player reads the HTTP response.
If a user seeks, the media player closes the HTTP connection.
If a user pauses playback,
the media player stops reading the response from the socket.
The HTTP protocol implements backpressure,
so writing the response blocks in this case.

HTTP communication may be more complex.
Media players quite often request a piece at the end of the file
when starting playback
or make periodic requests during uninterrupted playback.
The reasons of this are unknown to me.

> [!NOTE]
> BitTorrent clients tout the **sequential download mode**
> as the support for media streaming.
> This mode is only useful in very restricted circumstances:
>
> - A user plays the first file in a torrent.
> - A user plays a file from the beginning.
> - A media player makes exactly one HTTP request to a file
>   (for example, this requires tracks to be interleaved
>   in the media file).
>
> This plugin doesn't use the sequential download mode.

## Code organization

This project is a fork of the client.
The repository of the client contains the master branch
and the version 0.0.3 release. Since the master branch
contains a work in progress, I based my work on the release.

The client consists of several crates (see the documentation for the client).
I put the plugin into a new crate `vcz_http_server`.
`vcz_http_server` uses the `vincenzo` crate.
The `vcz` crate is an executable of the client.
It was modified to show how to attach the plugin to the client.

The code I added to the client mostly went into the module `vincenzo::disk`
and a new module `vincenzo::disk::slice`.

# Content streaming

The plugin implements a specialized HTTP server answering `GET` requests
with the Range header. The HTTP server is built on top of Hyper.
The HTTP server is run by calling the `vcz_http_server::main` function.
The HTTP server uses Tokio for multi-threading.
For every HTTP request, it starts a separate Tokio task that serves the request.

To serve a request, the task parses, validates the request,
and calculates the response by calling
a component of the client called the *Disk*.
The client also uses Tokio, and it runs a single task called the Disk
that owns a value of type `vincenzo::disk::Disk`.
This value will also be called the Disk. The Disk:

- reads torrent content from the file system
  and sends it to peer tasks for upload to peers;
- receives torrent content downloaded from peers
  and writes it to the file system;
- remembers which pieces are downloaded;
- chooses which pieces to download when the request queue of a peer task
  becomes non-full.

The Disk handles all torrents.
The API of the Disk is described in `vincenzo::disk::DiskMsg`.

In order to choose pieces to download, the Disk need to take into account
all HTTP connections existing at the moment.
Thus I introduced the notion of a slice. A *slice* is the interval of a torrent
that an HTTP connection is interested in, namely, the value of the Range header.

> [!NOTE]
> The Range header may contain a sequence of ranges.
> However, such HTTP requests aren't sent by media players in practice.
> Thus I didn't implement handling such requests.

The Disk stores all slices in the `slice_db` field.
Every HTTP connection creates a slice from its HTTP request by calling the Disk.
Then the HTTP connection sends read requests to this slice.

To be specific, slice read requests are sent by `vcz_http_server::ContentBody`.
`ContentBody` represents the body of the HTTP response.
Thus it implements `http_body::Body`.

On receiving a *slice read request*, the Disk removes a prefix from the slice
and sends the prefix content as a response.
If the prefix is inside a piece that is not downloaded yet,
the Disk will hold the slice read request in the queue in `slice_db`.

When an HTTP connection is closed, `ContentBody` is dropped,
and the `std::ops::Drop::drop` method of one of its fields
removes the slice from the Disk.



# Playback latency

*Latency* is time from sending an HTTP request
to receiving the response to this request.
Since live streaming is impossible via torrents,
latency is mostly perceived by users when starting playback or seeking.

Latency is influenced by the piece size of a torrent:
you can't play a piece until you download it completely.
The larger is the piece size, the greater is the latency.

Latency is influenced by the size of peer request queues.
When downloading torrents, a BitTorrent client requests blocks from peers.
A *block* is an interval in a piece.
When the client sends a block request to a peer,
it adds this request to the *peer request queue*.
When the client receives a response,
it removes the corresponding request from the queue.

When a user seeks in a media file to a new place,
the new place will be downloaded
only after the peer answers all the requests standing in the peer request queue.

> [!NOTE]
> The same is true for starting playback because the client already filled
> peer request queues for usual downloading (not related to media streaming).

Thus the larger is the size of peer request queues, the greater is the latency.
However, reducing the size of peer request queues may reduce download rate.
To keep download rate close to the data rate of your network channel,
you need to make the size of peer request queues large enough.

The size of peer request queues should be adjusted automatically,
but, unfortunately, it is not in this client.
To make latency tolerable, I modified the client
so the size of every peer request queue never exceeds
the constant `vincenzo::peer::MAX_REQ_QUEUE_LEN`.
Sorry that I implemented it in the source code, but implementing it
as a command-line option would require changing a lot in the client.
It should be calculated automatically anyway.
