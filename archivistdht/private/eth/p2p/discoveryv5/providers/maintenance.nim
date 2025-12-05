# Copyright 2025 Archivist DHT authors
# Copyright (c) 2022 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

import std/options
from std/times import now, utc, toTime, toUnix

import pkg/stew/endians2
import pkg/chronos
import pkg/libp2p
import pkg/datastore
import pkg/chronicles
import pkg/questionable
import pkg/questionable/results

import ./common

const
  ExpiredCleanupBatch* = 1000
  CleanupInterval* = 24.hours

proc cleanupExpired*(store: Datastore, batchSize = ExpiredCleanupBatch) {.async.} =
  trace "Cleaning up expired records"

  let q = Query.init(CidKey, limit = batchSize)

  block:
    without iter =? (await query[ArchUnixTime](store, q)), err:
      trace "Unable to obtain record for key", err = err.msg
      return

    defer:
      if not isNil(iter):
        trace "Cleaning up query iterator"
        iter.dispose()

    var records = newSeq[Record[ArchUnixTime]]()
    let now = nowMs()
    for item in iter:
      if recordMaybe =? (await item) and record =? recordMaybe:
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

proc cleanupOrphaned*(store: Datastore, batchSize = ExpiredCleanupBatch) {.async.} =
  trace "Cleaning up orphaned records"

  let providersQuery = Query.init(ProvidersKey, limit = batchSize, value = false)

  block:
    without iter =? (await query[void](store, providersQuery)), err:
      trace "Unable to obtain record for key"
      return

    defer:
      if not isNil(iter):
        trace "Cleaning up orphaned query iterator"
        iter.dispose()

    var count = 0
    for item in iter:
      if count >= batchSize:
        trace "Batch cleaned up", size = batchSize

      count.inc
      if maybeRecord =? (await item) and record =? maybeRecord:
        let key = record.key
        without peerId =? key.fromProvKey(), err:
          trace "Error extracting parts from cid key", key
          continue

        without cidKey =? (CidKey / "*" / $peerId), err:
          trace "Error building cid key", err = err.msg
          continue

        without cidIter =?
          (await query[void](store, Query.init(cidKey, limit = 1, value = false))), err:
          trace "Error querying key", cidKey, err = err.msg
          continue

        let res = block:
          var count = 0
          for item in cidIter:
            if maybeRecord =? (await item) and maybeRecord.isSome:
              count.inc
          count

        if not isNil(cidIter):
          trace "Disposing cid iter"
          cidIter.dispose()

        if res > 0:
          trace "Peer not orphaned, skipping", peerId
          continue

        # Background cleanup - simple delete, will retry next cycle if conflict
        if err =? (await store.delete(record)).errorOption:
          trace "Error deleting orphaned peer", err = err.msg
          continue

        trace "Cleaned up orphaned peer", peerId
