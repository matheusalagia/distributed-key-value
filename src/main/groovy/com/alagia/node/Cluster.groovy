package com.alagia.node

import com.google.common.collect.Iterables
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

@CompileStatic
@Slf4j
class Cluster {

    public static final int NUNBER_OF_PARTITIONS = 32
    public static final int NUMBER_OF_REPLICAS = 2

    List<RemoteNode> nodes
    List<Partition> partitions

    Cluster(List<RemoteNode> nodes) {
        this.nodes = nodes
        distributePartitions()
    }

    RemoteNode partitionLeader(int partition) {
        return partitions.find {it.id == partition}?.leader
    }

    void distributePartitions() {
        def nodesIterator = Iterables.cycle(nodes).iterator()
        partitions = new ArrayList<>(NUNBER_OF_PARTITIONS)
        NUNBER_OF_PARTITIONS.times {
            def partition = new Partition(id: it, leader: nodesIterator.next())
            partitions.add(partition)
        }

        log.info("Distributed partitions: $partitions")
    }
}

@Canonical
class Partition {
    int id
    RemoteNode leader
}
