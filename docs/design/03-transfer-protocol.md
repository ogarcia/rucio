# Transfer protocol

Protocol identifier: `/rucio/transfer/2.0.0`

Built on libp2p `request_response`. All messages are encoded with
[bincode](https://github.com/bincode-org/bincode).

## Chunk layout

Files are split into fixed-size chunks of **4 MiB**
(`rucio_core::protocol::chunk::CHUNK_SIZE`). The last chunk may be smaller.
Chunks are identified by their zero-based index.

```
file = [chunk_0 | chunk_1 | ... | chunk_n]
          4 MiB     4 MiB          ≤ 4 MiB
```

The total number of chunks for a file of size `S` is `ceil(S / 4_MiB)`.

The chunk size is **per-file** metadata: it is recorded in the manifest
(the `chunk_size` field) and the downloader uses that value rather than
assuming a constant. The producer side (`hashing::hash_file`, used when
indexing a shared file) always splits at `CHUNK_SIZE` (4 MiB), so every
manifest in practice carries 4 MiB — but a future change to the chunk size
stays backward-compatible because each file declares its own.

## Manifest request

Before downloading any data, the downloader fetches the **manifest** from a
provider. The manifest contains the authoritative metadata for the file.

**Request:**

```rust
TransferRequest::Manifest { root_hash: [u8; 32] }
```

**Response:**

```rust
TransferResponse::Manifest {
    name:        String,
    total_size:  u64,
    chunk_count: u32,
    peers:       Vec<String>,   // PEX: other known providers (multiaddrs)
}
```

The `peers` field implements Peer Exchange (PEX): the responding node shares
what other providers it knows about. The downloader may dial those peers for
parallel chunk downloads.

## Chunk request

```rust
TransferRequest::Chunk {
    root_hash:   [u8; 32],
    chunk_index: u32,
}
```

```rust
TransferResponse::Chunk {
    chunk_index: u32,
    data:        Vec<u8>,
}
```

Chunk data is served directly from disk — the daemon reads the corresponding
byte range of the shared file on demand.

## Download flow

```
downloader                          provider(s)
    |                                   |
    |-- Manifest request -------------> |
    |<- Manifest response (+ PEX) ----- |
    |                                   |
    |  (dial PEX peers if available)    |
    |                                   |
    |-- Chunk 0 request --------------> |
    |-- Chunk 1 request --------------> | (pipelined)
    |<- Chunk 0 data ------------------- |
    |<- Chunk 1 data ------------------- |
    |   ...                             |
    |-- Chunk N request --------------> |
    |<- Chunk N data ------------------- |
    |                                   |
    |  (verify root hash, move .part)   |
```

Chunk requests to different peers can be interleaved — the download engine
tracks which chunks have been requested and which have been received.

## In-progress storage

While downloading, chunks are written sequentially to a `.part` file in
`storage.temp_dir`. The file name is `<root_hash_hex>.part`.

On completion, the `.part` file is renamed (or copied if on a different
filesystem) to `storage.download_dir/<name>`.

## Partial sharing

A download is shared **while still in progress**, so a downloader contributes
to a file's availability from its first verified chunk — important for getting
a freshly introduced file to spread.

- On the **first chunk** that verifies and lands in the `.part`, the engine
  announces the file to the DHT (`StartProviding(root_hash)`), once per
  download.
- Incoming chunk requests for a hash that isn't a completed share fall back to
  the in-progress download: the chunk is served from the `.part` **only if it
  is marked `done`** in `download_chunks` (i.e. already hash-verified on
  receipt). A chunk we don't yet hold returns `NotFound` — we never serve bytes
  from a half-written chunk.
- On **completion**, the file moves to `download_dir`, is indexed as an ordinary
  share, and is served by the normal shared-files path; the provider
  announcement carries over.
- On **cancel**, the `.part` is deleted and we `StopProviding`. A **paused**
  download keeps sharing what it has (the `.part` and its `done` chunks remain).

## Resumption

On daemon startup, `DownloadEngine::resume_interrupted()` is called. It
queries the database for all downloads in states `finding_providers`, `queued`
or `downloading` and re-enqueues them.

For a download that was mid-transfer, the engine reads the database to
determine which chunks were already marked as received and only requests the
missing ones. Chunks already written to the `.part` file are not re-downloaded.

## FindingProviders state

When a download is started for a hash that has no known providers yet (e.g.
from a bare magnet link with no `peer=` parameters), the download is
registered in the database immediately with the state `finding_providers`.
This gives the user immediate feedback in `rucio download list`.

The engine queries the Kademlia DHT for providers. Once at least one provider
is found, the download transitions to `queued` and the manifest is fetched.

```
start()  →  create_pending(has_providers=false)  →  state: finding_providers
         →  kad.get_providers(hash)
         →  add_providers()                       →  state: queued
         →  fetch manifest (finalize_pending)
         →  begin chunk transfers                 →  state: downloading
         →  all chunks received                   →  state: completed
```

If providers are already known (from a search result or a magnet link with
`peer=` parameters), the download goes directly to `queued`.

## Error handling

- If a chunk response contains the wrong index or wrong data, it is discarded
  and re-requested from another provider.
- If a provider disconnects mid-transfer, the engine marks that provider as
  unavailable and redistributes its pending chunks to other providers.
- If no providers remain, the download transitions to `failed`.
