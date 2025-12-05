# Copyright 2025 Archivist DHT authors
# Copyright (c) 2022 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

import std/sequtils
import std/strutils
import std/times

import pkg/stew/endians2
import pkg/datastore
import pkg/chronos
import pkg/libp2p
import pkg/chronicles
import pkg/results as rs
import pkg/questionable
import pkg/questionable/results

{.push raises: [].}

import ./maintenance
import ./cache
import ./common
import ../spr

export cache, datastore

logScope:
  topics = "discv5 providers manager"

const DefaultProviderTTL* = initDuration(hours = 24).inMilliseconds()

type ProvidersManager* = ref object of RootObj
  store*: Datastore
  cache*: ProvidersCache
  maxItems*: uint
  maxProviders*: uint
  disableCache*: bool
  expiredLoop*: Future[void]
  orphanedLoop*: Future[void]
  started*: bool
  batchSize*: int
  cleanupInterval*: timer.Duration

proc add*(
    self: ProvidersManager,
    id: NodeId,
    provider: SignedPeerRecord,
    ttl = DefaultProviderTTL,
): Future[?!void] {.async.} =
  let peerId = provider.data.peerId

  trace "Adding provider to persistent store", id, peerId
  let
    provKey = ?makeProviderKey(peerId)
    cidKey = ?makeCidKey(id, peerId)
    expires = nowMs() + ttl

  # Middleware to handle conflicts - fetch current tokens and update values atomically
  proc updateMiddleware(
      failed: seq[RawRecord]
  ): Future[?!seq[RawRecord]] {.async: (raises: [CancelledError]).} =
    trace "Handling conflict for provider records", id, peerId
    let keys = failed.mapIt(it.key)
    # Use typed get with seq[byte] to get raw records
    let existing = ?(await get[seq[byte]](self.store, keys))
    var updated: seq[RawRecord]

    for record in existing:
      if record.key == provKey:
        # Check seqNo before updating provider
        let existingProv = ?SignedPeerRecord.decode(record.val)
        if existingProv.data.seqNo < provider.data.seqNo:
          trace "Provider key exists, updating with newer seqNo", id, peerId
          updated.add(RawRecord.init(record.key, encode(provider), record.token))
        else:
          trace "Existing provider has same or newer seqNo, keeping", id, peerId
          # Keep existing record unchanged
          updated.add(RawRecord.init(record.key, record.val, record.token))
      else:
        # TTL record - always update with new expiry
        trace "Updating TTL record", id, peerId
        updated.add(
          RawRecord.init(record.key, encode(expires.uint64.ArchUnixTime), record.token)
        )

    success updated

  # Batch both records into single atomic operation
  trace "Adding or updating provider and cid records atomically", id, peerId
  let failedRecords =
    ?(
      await self.store.tryPut(
        @[
          RawRecord.init(provKey, encode(provider), 0),
          RawRecord.init(cidKey, encode(expires.uint64.ArchUnixTime), 0),
        ],
        middleware = updateMiddleware,
      )
    )

  if failedRecords.len > 0:
    return
      failure newException(DatastoreError, "Unable to add provider due to conflict")

  self.cache.add(id, provider)
  trace "Provider for id added", cidKey, provKey
  return success()

proc get*(
    self: ProvidersManager, id: NodeId, start = 0, stop = MaxProvidersPerEntry.int
): Future[?!seq[SignedPeerRecord]] {.async.} =
  trace "Retrieving providers from persistent store", id

  let provs = self.cache.get(id, start = start, stop = stop)

  if provs.len > 0:
    return success provs

  let cidKey = ?(CidKey / id.toHex)

  trace "Querying providers from persistent store", id, key = cidKey
  var providers: seq[SignedPeerRecord]
  block:
    let
      q = Query.init(cidKey, offset = start, limit = stop)
      cidIter = ?(await query[void](self.store, q))

    defer:
      if not isNil(cidIter):
        trace "Cleaning up query iterator"
        cidIter.dispose()

    var orphanedRecords: seq[KeyRecord]
    for item in cidIter:
      if maybeRecord =? (await item) and record =? maybeRecord:
        let
          key = record.key
          pairs = ?key.fromCidKey()
          provKey = ?makeProviderKey(pairs.peerId)

        trace "Querying provider key", key = provKey
        without provRecord =? (await get[SignedPeerRecord](self.store, provKey)):
          # Provider not found - this is an orphaned cid entry
          orphanedRecords.add(record)
          continue

        trace "Retrieved provider with key", key = provKey
        providers.add(provRecord.val)
        self.cache.add(id, provRecord.val)

    # TODO: Should we really remove orphaned records here?
    trace "Deleting orphaned cid entries from store", len = orphanedRecords.len
    if orphanedRecords.len > 0 and
        err =? (await self.store.delete(orphanedRecords)).errorOption:
      trace "Error deleting orphaned records from persistent store", err = err.msg

    trace "Retrieved providers from persistent store", id = id, len = providers.len
  return success providers

proc contains*(
    self: ProvidersManager, id: NodeId, peerId: PeerId
): Future[bool] {.async.} =
  without key =? makeCidKey(id, peerId), err:
    return false

  return (await self.store.has(key)) |? false

proc contains*(self: ProvidersManager, peerId: PeerId): Future[bool] {.async.} =
  without provKey =? makeProviderKey(peerId), err:
    return false

  return (await self.store.has(provKey)) |? false

proc contains*(self: ProvidersManager, id: NodeId): Future[bool] {.async.} =
  without cidKey =? (CidKey / id.toHex), err:
    return false

  let q = Query.init(cidKey, limit = 1)

  block:
    without iter =? (await query[void](self.store, q)), err:
      trace "Unable to obtain record for key", key = cidKey
      return false

    defer:
      if not isNil(iter):
        trace "Cleaning up query iterator"
        iter.dispose()

    for item in iter:
      if maybeRecord =? (await item) and maybeRecord.isSome:
        return true

  return false

proc remove*(self: ProvidersManager, id: NodeId): Future[?!void] {.async.} =
  self.cache.drop(id)
  let
    cidKey = ?(CidKey / id.toHex)
    q = Query.init(cidKey)

  block:
    let iter = ?(await query[void](self.store, q))

    defer:
      if not isNil(iter):
        trace "Cleaning up query iterator"
        iter.dispose()

    var records: seq[KeyRecord]

    for item in iter:
      if maybeRecord =? (await item) and record =? maybeRecord:
        records.add(record)
        let pairs = ?record.key.fromCidKey
        self.cache.remove(id, pairs.peerId)
        trace "Deleted record from store", key = record.key

    if records.len > 0:
      # User-initiated remove - use tryDelete with refetch middleware
      proc refetchMiddleware(
          failed: seq[KeyRecord]
      ): Future[?!seq[KeyRecord]] {.async: (raises: [CancelledError]).} =
        var refreshed: seq[KeyRecord]
        for r in failed:
          let rec = (await get[void](self.store, r.key)).valueOr:
            continue
          refreshed.add(rec)
        success refreshed

      if err =?
          (await self.store.tryDelete(records, middleware = refetchMiddleware)).errorOption:
        trace "Error deleting record from persistent store", err = err.msg
        return failure err

  return success()

proc remove*(
    self: ProvidersManager, peerId: PeerId, entries = false
): Future[?!void] {.async.} =
  var allRecords: seq[KeyRecord]

  # Collect all cid entries for this peerId if requested
  if entries:
    without cidKey =? (CidKey / "*" / $peerId), err:
      return failure err

    let q = Query.init(cidKey)

    block:
      without iter =? (await query[void](self.store, q)), err:
        trace "Unable to obtain record for key", key = cidKey
        return failure err

      defer:
        if not isNil(iter):
          trace "Cleaning up query iterator"
          iter.dispose()

      for item in iter:
        if maybeRecord =? (await item) and record =? maybeRecord:
          allRecords.add(record)

      trace "Collected cid entries for deletion", count = allRecords.len

  # Add provider record to the batch
  without provKey =? peerId.makeProviderKey, err:
    return failure err

  trace "Removing provider from cache", peerId
  self.cache.remove(peerId)

  trace "Checking for provider record", key = provKey
  if provRecord =? (await get[SignedPeerRecord](self.store, provKey)):
    allRecords.add(provRecord.toKeyRecord)

  # Single atomic delete of all records (cid entries + provider)
  if allRecords.len > 0:
    let refetchMiddleware = proc(
        failed: seq[KeyRecord]
    ): Future[?!seq[KeyRecord]] {.async: (raises: [CancelledError]).} =
      var refreshed: seq[KeyRecord]
      for r in failed:
        let rec = (await get[void](self.store, r.key)).valueOr:
          continue
        refreshed.add(rec)
      success refreshed

    trace "Deleting all records atomically", count = allRecords.len
    if err =?
        (await self.store.tryDelete(allRecords, middleware = refetchMiddleware)).errorOption:
      trace "Error deleting records from persistent store", err = err.msg
      return failure err

    trace "Deleted records from store"

  return success()

proc remove*(
    self: ProvidersManager, id: NodeId, peerId: PeerId
): Future[?!void] {.async.} =
  self.cache.remove(id, peerId)
  without cidKey =? makeCidKey(id, peerId), err:
    trace "Error creating key from content id", err = err.msg
    return failure err.msg

  without record =? (await get[ArchUnixTime](self.store, cidKey)):
    return success() # already doesn't exist

  # User-initiated remove - use tryDelete with refetch middleware
  let refetchMiddleware = proc(
      failed: seq[Record[ArchUnixTime]]
  ): Future[?!seq[Record[ArchUnixTime]]] {.async: (raises: [CancelledError]).} =
    var refreshed: seq[Record[ArchUnixTime]]
    for r in failed:
      if rec =? (await get[ArchUnixTime](self.store, r.key)):
        refreshed.add(rec)
    success refreshed

  return (await self.store.tryDelete(record, middleware = refetchMiddleware))

proc cleanupExpiredLoop(self: ProvidersManager) {.async.} =
  try:
    while self.started:
      await self.store.cleanupExpired(self.batchSize)
      await sleepAsync(self.cleanupInterval)
  except CancelledError as exc:
    trace "Cancelled expired cleanup job", err = exc.msg
  except CatchableError as exc:
    trace "Exception in expired cleanup job", err = exc.msg
    raiseAssert "Exception in expired cleanup job"

proc cleanupOrphanedLoop(self: ProvidersManager) {.async.} =
  try:
    while self.started:
      await self.store.cleanupOrphaned(self.batchSize)
      await sleepAsync(self.cleanupInterval)
  except CancelledError as exc:
    trace "Cancelled orphaned cleanup job", err = exc.msg
  except CatchableError as exc:
    trace "Exception in orphaned cleanup job", err = exc.msg
    raiseAssert "Exception in orphaned cleanup job"

proc start*(self: ProvidersManager) {.async.} =
  self.started = true
  self.expiredLoop = self.cleanupExpiredLoop
  self.orphanedLoop = self.cleanupOrphanedLoop

proc stop*(self: ProvidersManager) {.async.} =
  await self.expiredLoop.cancelAndWait()
  await self.orphanedLoop.cancelAndWait()
  self.started = false

func new*(
    T: type ProvidersManager,
    store: Datastore,
    disableCache = false,
    maxItems = MaxProvidersEntries,
    maxProviders = MaxProvidersPerEntry,
    batchSize = ExpiredCleanupBatch,
    cleanupInterval = CleanupInterval,
): T =
  T(
    store: store,
    maxItems: maxItems,
    maxProviders: maxProviders,
    disableCache: disableCache,
    batchSize: batchSize,
    cleanupInterval: cleanupInterval,
    cache: ProvidersCache.init(
      size = maxItems, maxProviders = maxProviders, disable = disableCache
    ),
  )
