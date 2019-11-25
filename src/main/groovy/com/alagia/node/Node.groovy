package com.alagia.node

import groovy.transform.Canonical
import groovy.transform.ToString

trait Node {

    NodeId id

    abstract void save(Data data)

    abstract Optional<Data> get(String key)

    abstract void saveLocalReplica(Data data)

    static int calculatePartition(String key) {
        Math.abs(key.hashCode()) % Cluster.NUNBER_OF_PARTITIONS
    }
}

@Canonical
@ToString(includePackage = false)
class NodeId {
    String name
    String address
    int port

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        NodeId nodeId = (NodeId) o

        if (name != nodeId.name) return false

        return true
    }

    int hashCode() {
        return (name != null ? name.hashCode() : 0)
    }
}