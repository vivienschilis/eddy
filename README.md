edvents
=======

Simple pub/sub with buffers.

Events are sent to a channel. A channel as a buffer size and a TTL.

Clients subscribe to N channel, get the buffered items and then all new messages
in order.

Events have a unique ID and a timestamp.

Redis is used as a data store and pub/sub channels but can be replaced by
something else.

TODO
----

Features:

* Channel settings, each event can optionally send a buffer size or TTL.
* Events ordering, protentially inject the event timestamp in the even body
  : right now channel buffers are not mixed on the server
* Unique event IDs
* Signed urls for opening channels
* BasicAuth on the publisher side
* explore if it's possible to use gzip Content-Encoding for lower bandwidth
* channel patterns ? See Psubscribe on redis
* LUA function to insert/retrieve the channels data
* Implement other channel types ?

Production:

* client compatiblity list, see polyfill: https://github.com/Yaffle/EventSource
* connection tracking
* sharding ?
* Redis cluster ? needs ZSET and PUB/SUB support
* monitoring
* GET /status => json
* Avoid creating 2 connections to redis per client
 => map connections to subscription
