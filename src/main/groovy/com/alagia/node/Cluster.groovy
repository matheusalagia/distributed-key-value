package com.alagia.node

import com.google.common.collect.Iterables
import groovy.transform.CompileStatic

@CompileStatic
class Cluster {

    public static final int NUNBER_OF_PARTITIONS = 360

    private List<NodeId> nodes
    private List<NodeId> partitions

    Cluster(List<NodeId> nodes) {
        this.nodes = nodes
        distributePartitions()
    }

    NodeId partitionOwner(int keyHash) {
        return partitions[keyHash]
    }

    void distributePartitions() {
        // TODO e os dados não devem ser movidos??
        def nodesIterator = Iterables.cycle(nodes).iterator()
        partitions = new ArrayList<>(NUNBER_OF_PARTITIONS)
        (0..NUNBER_OF_PARTITIONS - 1).each {
            partitions.add(nodesIterator.next())
        }
    }
}
