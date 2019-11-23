package com.alagia.node

import com.google.common.collect.Iterables

class Cluster {

    private ArrayList<NodeId> nodes
    private ArrayList<NodeId> partitions

    Cluster(ArrayList<NodeId> nodes) {
        this.nodes = nodes
        distributePartitions()
    }

    NodeId partitionOwner(int keyHash) {
        return partitions[keyHash]
    }

    void distributePartitions() {
        // TODO e os dados não devem ser movidos??
        def nodesIterator = Iterables.cycle(nodes).iterator()
        partitions = new ArrayList<>(360)
        (0..359).each {
            partitions.add(nodesIterator.next())
        }
    }
}
