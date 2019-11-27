package com.alagia.node

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Iterables
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.data.redis.core.StringRedisTemplate

@CompileStatic
@Slf4j
class Cluster {

    public static final int NUNBER_OF_PARTITIONS = 32
    public static final int NUMBER_OF_REPLICAS = 2

    List<RemoteNode> nodes
    StringRedisTemplate redisTemplate
    List<Partition> partitions

    Cluster(List<RemoteNode> nodes, StringRedisTemplate redisTemplate) {
        this.nodes = nodes
        this.redisTemplate = redisTemplate
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

    void saveReplicationInfo(ReplicationDefinitionEvent replicationDefinition) {
        def replicaNodes = nodes.findAll {it.id in replicationDefinition.replicas}

        partitions
                .find {it.id == replicationDefinition.partition}
                .replicas = replicaNodes
    }

    void broadcastReplicationDefinition(ReplicationDefinitionEvent replicationDefinitionEvent) {
        redisTemplate.convertAndSend('cluster-events', new ObjectMapper().writeValueAsString(replicationDefinitionEvent))
    }
}

@Canonical
class Partition {
    int id
    RemoteNode leader
    List<RemoteNode> replicas
}
