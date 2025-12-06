version       = "0.7.1"
author        = "Archivist DHT Authors, Status Research & Development GmbH"
description   = "DHT based on Eth discv5 implementation"
license       = "MIT"
skipDirs      = @["tests"]

requires "secp256k1 >= 0.6.0"
requires "nimcrypto >= 0.6.2"
requires "bearssl >= 0.2.5 & < 0.3.0"
requires "chronicles >= 0.10.2"
requires "https://github.com/status-im/nim-chronos >= 4.0.4"
requires "https://github.com/durability-labs/nim-libp2p#multihash-poseidon2" # TODO: ensures compatiblity with archivist-node, replace once this branch is no longer needed
requires "metrics >= 0.1.0"
requires "stew >= 0.2.0"
requires "stint >= 0.8.1"
requires "https://github.com/durability-labs/nim-kvstore >= 0.1.0"
requires "questionable >= 0.10.15"

taskRequires "test", "asynctest >= 0.5.2"
taskRequires "test", "unittest2 >= 0.2.4"
