# Copyright 2025 Archivist DHT authors
# Copyright (c) 2022 Status Research & Development GmbH
# Licensed and distributed under either of
#   * MIT license (license terms in the root directory or at https://opensource.org/licenses/MIT).
#   * Apache v2 license (license terms in the root directory or at https://www.apache.org/licenses/LICENSE-2.0).
# at your option. This file may not be copied, modified, or distributed except according to those terms.

{.push raises: [].}

import std/strutils
import std/times

import pkg/chronos
import pkg/libp2p
import pkg/kvstore
import pkg/stew/endians2
import pkg/questionable
import pkg/questionable/results

import ../node

export node, results

type ArchUnixTime* = distinct uint64

proc nowMs*(): int64 =
  ## Returns current Unix time in milliseconds
  now().utc().toTime().toUnix() * 1000

const
  ProvidersKey* = Key.init("/providers").tryGet
    # keys is of the form /providers/peerid = provider
  CidKey* = Key.init("/cids").tryGet # keys is of the form /cids/cid/peerid/ttl = ttl

proc mapFailure*[T](err: T): ref CatchableError =
  newException(CatchableError, $err)

# ArchUnixTime encode/decode for typed datastore
proc encode*(time: ArchUnixTime): seq[byte] =
  @(endians2.toBytesBE(time.uint64))

proc decode*(_: type ArchUnixTime, bytes: seq[byte]): ?!ArchUnixTime =
  success endians2.fromBytesBE(uint64, bytes).ArchUnixTime

# SignedPeerRecord encode/decode for typed datastore
proc encode*(spr: SignedPeerRecord): seq[byte] =
  spr.envelope.encode().valueOr:
    raiseAssert "SignedPeerRecord encoding should not fail: " & $error

proc decode*(_: type SignedPeerRecord, bytes: seq[byte]): ?!SignedPeerRecord =
  # Use module-qualified call to avoid recursion with our own decode
  let res = libp2p.decode(SignedPeerRecord, bytes)
  if res.isOk:
    success res.get
  else:
    failure newException(CatchableError, "Failed to decode SignedPeerRecord")

proc makeProviderKey*(peerId: PeerId): ?!Key =
  (ProvidersKey / $peerId)

proc makeCidKey*(cid: NodeId, peerId: PeerId): ?!Key =
  (CidKey / cid.toHex / $peerId / "ttl")

proc fromCidKey*(key: Key): ?!tuple[id: NodeId, peerId: PeerId] =
  let parts = key.id.split(kvstore.Separator)

  if parts.len == 5:
    let
      peerId = ?PeerId.init(parts[3]).mapErr(mapFailure)
      id = ?NodeId.fromHex(parts[2]).catch

    return success (id, peerId)

  return failure("Unable to extract peer id from key")

proc fromProvKey*(key: Key): ?!PeerId =
  let parts = key.id.split(kvstore.Separator)

  if parts.len != 3:
    return failure("Can't find peer id in key")

  return success ?PeerId.init(parts[^1]).mapErr(mapFailure)
