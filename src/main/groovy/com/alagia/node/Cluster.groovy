package com.alagia.node

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Iterables
import com.google.common.collect.Lists
import groovy.transform.Canonical
import groovy.transform.CompileStatic
import groovy.transform.ToString
import groovy.util.logging.Slf4j
import org.springframework.data.redis.core.StringRedisTemplate

@CompileStatic
@Slf4j
class Cluster {

    public static final int NUNBER_OF_PARTITIONS = 32
    public static final int NUMBER_OF_REPLICAS = 2

    List<RemoteNode> nodes = Lists.newCopyOnWriteArrayList()
    StringRedisTemplate redisTemplate
    List<Partition> partitions = new ArrayList<>(NUNBER_OF_PARTITIONS)
    ObjectMapper objectMapper

    Cluster(List<RemoteNode> nodes,
            StringRedisTemplate redisTemplate,
            ObjectMapper objectMapper) {
        this.nodes = nodes
        this.redisTemplate = redisTemplate
        this.objectMapper = objectMapper
    }

    RemoteNode partitionLeader(int partition) {
        def leaderId = partitions.find { it.id == partition }?.leader
        return nodes.find {it.id == leaderId}
    }

    List<Partition> getPartitiosOf(NodeId node) {
        return partitions.findAll {it.leader == node}
    }

    void distributePartitions() {
        def nodesIterator = Iterables.cycle(nodes.id).iterator()
        NUNBER_OF_PARTITIONS.times {
            def partition = new Partition(id: it, leader: nodesIterator.next())
            partitions.add(partition)
        }

        log.info("Distributed partitions: $partitions")
    }

    void broadcastPartitionigEvent(PartitioningUpdateEvent replicationDefinitionEvent) {
        def event = objectMapper.writeValueAsString(replicationDefinitionEvent)
        redisTemplate.convertAndSend('cluster-events', event)
    }

    void broadcastNodeDown(NodeId nodeId) {
        def event = objectMapper.writeValueAsString(new NodeDownEvent(nodeId: nodeId))
        redisTemplate.convertAndSend('cluster-events', event)
    }

    void redefinePartition(Partition partition) {
        log.info("Redefining partition $partition on $partitions")
        partitions.removeAll {it.id == partition.id}
        partitions.add(partition)
    }
}

@Canonical
@ToString(includePackage = false, includeFields = true)
class Partition {
    int id
    NodeId leader
    List<NodeId> replicas
}
