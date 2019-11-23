package com.alagia.node

import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j

@CompileStatic
@EqualsAndHashCode
@Slf4j
class Node {

    private NodeId id
    private Map<String, Data> data = [:]
    private Cluster cluster

    Node(NodeId id, Cluster cluster) {
        this.id = id
        this.cluster = cluster
    }

    void save(Data data) {
        int dataPartition = calculatePartition(data.key)
        def node = cluster.partitionOwner(dataPartition)

        if (node == id) {
            log.info("Saving $data (partition $dataPartition) on node $id")
            this.data[data.key] = data
        } else {
            log.info("Sending $data (partition $dataPartition) to correct node $node")
            // fazer chamada rest para o nodo correto
        }
    }

    Optional<Data> get(String key) {
        int dataPartition = calculatePartition(key)
        def node = cluster.partitionOwner(dataPartition)

        if (node) {
            log.info("Getting data for key $key (partition $dataPartition) on node $id")
            return Optional.ofNullable(data[key])
        }
        log.info("Getting data for key $key (partition $dataPartition) on another node ($id)")
        // fazer chamada
        return null // fazer chamada para nodo correto
    }


    private static int calculatePartition(Object obj) {
        Math.abs(obj.hashCode()) % 360
    }
}

@Canonical
class NodeId {
    String name
    String address
    int port

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        NodeId nodeId = (NodeId) o

        if (port != nodeId.port) return false
        if (address != nodeId.address) return false
        if (name != nodeId.name) return false

        return true
    }

    int hashCode() {
        int result
        result = (name != null ? name.hashCode() : 0)
        result = 31 * result + (address != null ? address.hashCode() : 0)
        result = 31 * result + port
        return result
    }
}