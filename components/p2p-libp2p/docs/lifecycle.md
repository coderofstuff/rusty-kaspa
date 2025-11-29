# Libp2p lifecycle notes

- Pending dials: each pending dial carries its own relay metadata (if any) and is tracked by request/connection id. Entries are cleared per-dial on StreamEvent::Outbound success, OutgoingConnectionError, ConnectionClosed, periodic expiry (~30s), and shutdown. No “one relay dial per peer” restriction remains.
- DCUtR/relay handoff: when a relayed dial succeeds via DCUtR, ConnectionEstablished moves the earliest pending relay dial for that peer to the actual connection_id so ConnectionClosed without a stream still drains the pending dial promptly (deliberately FIFO to avoid starving earlier attempts).
- Shutdown: the swarm driver, helper listener, reservation worker, and inbound bridge all listen for shutdown signals and exit; the driver drains pending dials and relay mappings, and reservations are released.
- Backpressure: incoming streams flow through a bounded channel (32). enqueue_incoming uses try_send; on Full/Closed it logs once and immediately drops the stream, so streams are not buffered unboundedly.
- Helper API: max line length is 8 KiB with a 5s read timeout. Each TCP connection handles a single command, responds with JSON (including for invalid UTF-8/JSON), then closes.
- Reservations: at most one active ReservationHandle per relay key; refresh releases the previous handle/listener before storing the new one, and shutdown releases all.
