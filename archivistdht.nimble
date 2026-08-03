version       = "0.8.0"
author        = "Archivist DHT Authors, Status Research & Development GmbH"
description   = "DHT based on Eth discv5 implementation"
license       = "MIT"
skipDirs      = @["tests"]

requires "secp256k1 >= 0.6.0"
requires "nimcrypto >= 0.6.2"
requires "bearssl >= 0.2.7 & < 0.3.0"
requires "chronicles >= 0.10.2"
requires "chronos >= 4.2.2 & < 5.0.0"
requires "libp2p >= 2.2.0 & < 3.0.0"
requires "metrics >= 0.1.0"
requires "stew >= 0.2.0"
requires "stint >= 0.8.1"
requires "https://github.com/durability-labs/nim-kvstore#feat/atomic-async"
requires "taskpools >= 0.1.0"
requires "testutils >= 0.3.0 & < 0.7.0"
requires "questionable >= 0.10.15"
requires "serialization >= 0.4.9 & < 0.5.0"

taskRequires "test", "asynctest >= 0.5.2"
taskRequires "test", "unittest2 >= 0.2.4"

task format, "Format code using NPH":
  # exec "nimble install https://github.com/durability-labs/nph@#version-0-6-2-prerelease" # TODO: update to version 0.6.2 once it is released
  exec findExe("nph") & " archivistdht.nim"
  exec findExe("nph") & " archivistdht/"
  exec findExe("nph") & " tests/"
