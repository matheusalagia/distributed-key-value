package com.alagia.node

import groovy.transform.Canonical
import groovy.transform.ToString

interface Node {

    NodeId getId()

    void save(Data data)

    Optional<Data> get(String key)

    void saveLocalReplica(Data data)
}

interface RemoteNode extends Node {
    boolean isHealthy()
}

@Canonical
@ToString(includePackage = false)
class NodeId {
    String name
    String address
    int port
    boolean leader

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