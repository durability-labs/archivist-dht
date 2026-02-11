# Copyright 2025 Archivist DHT authors
# Copyright (c) 2022 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

import std/options

import pkg/stew/endians2
import pkg/chronos
import pkg/libp2p
import pkg/kvstore
import pkg/chronicles
import pkg/questionable
import pkg/questionable/results

import ./common

const
  ExpiredCleanupBatch* = 1000
  CleanupInterval* = 24.hours

proc cleanupExpired*(
    store: KVStore, batchSize = ExpiredCleanupBatch
): Future[?!void] {.async: (raises: [CancelledError]).} =
  trace "Cleaning up expired records"

  let q = Query.init(CidKey, limit = batchSize)

  block:
    let iter = ?await query(store, q, ArchUnixTime)

    defer:
      if not isNil(iter):
        trace "Cleaning up query iterator"
        if err =? (await iter.dispose()).errorOption:
          warn "Error disposing query iterator", err = err.msg

    var records = newSeq[KVRecord[ArchUnixTime]]()
    let now = nowMs()
    while not iter.finished:
      let recordMaybe = ?await iter.next()
      if record =? recordMaybe:
        let
          expired = record.val.int64
          key = record.key

        if now >= expired:
          trace "Found expired record", key
          records.add(record)
          without pairs =? key.fromCidKey(), err:
            trace "Error extracting parts from cid key", key
            continue

        if records.len >= batchSize:
          break

    # Background cleanup - simple delete, will retry next cycle if conflict
    if err =? (await store.delete(records)).errorOption:
      trace "Error cleaning up batch, records left intact!",
        size = records.len, err = err.msg

    trace "Cleaned up expired records", size = records.len

  return success()

proc cleanupOrphaned*(
    store: KVStore, batchSize = ExpiredCleanupBatch
): Future[?!void] {.async: (raises: [CancelledError]).} =
  trace "Cleaning up orphaned records"

  let providersQuery = Query.init(ProvidersKey, limit = batchSize, value = false)
  block:
    let iter = ?await query(store, providersQuery)

    defer:
      if not isNil(iter):
        trace "Cleaning up orphaned query iterator"
        if err =? (await iter.dispose()).errorOption:
          warn "Unable to dispose query iter", err = err.msg

    var count = 0
    while not iter.finished:
      if count >= batchSize:
        trace "Batch cleaned up", size = batchSize
        break

      count.inc
      let maybeRecord = ?await iter.next()
      without record =? maybeRecord:
        continue

      let key = record.key
      without peerId =? key.fromProvKey(), err:
        trace "Error extracting parts from cid key", key
        continue

      without cidKey =? (CidKey / "*" / $peerId), err:
        trace "Error building cid key", err = err.msg
        continue

      without cidIter =?
        (await query(store, Query.init(cidKey, limit = 1, value = false))), err:
        trace "Error querying key", cidKey, err = err.msg
        continue

      let res = block:
        defer:
          if err =? (await cidIter.dispose()).errorOption:
            warn "Unable to dispose cid query iter", err = err.msg

        (?await cidIter.fetchAll()).len

      if res > 0:
        trace "Peer not orphaned, skipping", peerId
        continue

      # Background cleanup - simple delete, will retry next cycle if conflict
      if err =? (await store.delete(record)).errorOption:
        trace "Error deleting orphaned peer", err = err.msg
        continue

      trace "Cleaned up orphaned peer", peerId

  return success()
