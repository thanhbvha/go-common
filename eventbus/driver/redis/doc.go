// Package redis implements the EventBus transport using Redis Streams.
//
// Event consumers poll the main and private retry lanes fairly without blocking
// on an empty lane. When both are empty, XINFO STREAM snapshots plus a second
// empty sweep establish race-free cursors for a read-only blocking XREAD wake-up.
// That wake-up never acquires pending ownership; single-stream XREADGROUP calls
// remain bounded by the combined concurrency/prefetch budget, including budget 1.
// Steady idle timeouts reuse the cursors and issue one blocking command per
// configured interval. Task consumers retain their single-stream blocking path.
// Redis ACLs for event consumers must permit XREAD and XINFO STREAM in addition
// to the existing stream/group commands. Both lanes use the same cluster hash
// tag. Topology, retry isolation, settlement, lease and default options are unchanged.
package redis
