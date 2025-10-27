{.used.}

import
  chronos,
  bearssl/rand,
  asynctest/chronos/unittest,
  archivistdht/discv5/protocol as discv5_protocol,
  archivistdht/discv5/routing_table,
  ../dht/test_helper

suite "Archivist system testing options Tests":
  var
    rng: ref HmacDrbgContext
    node1: discv5_protocol.Protocol
    node2: discv5_protocol.Protocol

  setup:
    rng = newRng()
    node1 = initDiscoveryNode(
      rng, PrivateKey.example(rng), localAddress(20301))
    node2 = initDiscoveryNode(
      rng, PrivateKey.example(rng), localAddress(20302))

  teardown:
    await node1.closeWait()
    await node2.closeWait()

  when defined(archivist_system_testing_options):
    proc nodesPingEachOther() {.async.} =
      # we ping each way twice to make sure if the nodes could discovery each other, they will.
      discard await node1.ping(node2.localNode)
      discard await node2.ping(node1.localNode)
      discard await node1.ping(node2.localNode)
      discard await node2.ping(node1.localNode)

    test "send failure is disabled":
      await nodesPingEachOther()
      
      # mutual discovery
      check:
        node1.routingTable.len == 1
        node2.routingTable.len == 1

    test "single node send failure":
      node1.transport.sendFailProb = 1

      await nodesPingEachOther()

      # no discovery
      check:
        node1.routingTable.len == 0
        node2.routingTable.len == 0

  else:
    test "Not compiled with option":
      debugEcho "These tests should be compiled with '-d:archivist_system_testing_options'"
      fail()
