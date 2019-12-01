package com.alagia.node

import static com.alagia.node.ClusterEventTypes.PARTITION_NEW_LEADER
import static com.alagia.node.ClusterEventTypes.PARTITION_REPLICA_UPDATE
import static com.google.common.collect.Lists.newArrayList

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.Multimap
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j
import org.springframework.scheduling.annotation.Scheduled

@CompileStatic
@EqualsAndHashCode
@Slf4j
class LocalNode implements Node {

    private NodeId id
    private Cluster cluster
    private Map<String, Data> data = [:]
    private Multimap<Integer, Node> replicaMap = ArrayListMultimap.create()

    LocalNode(NodeId id,
              Cluster cluster) {
        this.id = id
        this.cluster = cluster
    }

    @Override
    NodeId getId() {
        return id
    }

    @Override
    void save(Data data) {
        int partition = calculatePartition(data.key)
        def node = cluster.partitionLeader(partition)

        if (node.id == id) {
            log.info("Saving $data (partition $partition) on local storage $id")
            this.data[data.key] = data
            replicate(partition, data)
        } else {
            log.info("Sending $data (partition $partition) to partition leader ${node.id}")
            node.save(data)
        }
    }

    @Override
    Optional<Data> get(String key) {
        int partition = calculatePartition(key)
        def node = cluster.partitionLeader(partition)

        if (node.id == id) {
            log.info("Getting data for key $key (partition $partition) on local storage $id")
            return Optional.ofNullable(data[key])
        }
        log.info("Getting data for key $key (partition $partition) on partition leader ($id)")
        return node.get(key)
    }

    @Scheduled(fixedRate = 2000L, initialDelay = 5000L)
    void distributePartititons() {
        if(id.leader) {
            defineClusterPartitioning()
        }
    }

    @Scheduled(fixedRate = 2000L, initialDelay = 10000L)
    void checkHealthOfClusterNodes() {
        if(id.leader) {
            log.info("Doing health check...")
            List<RemoteNode> otherNodes = cluster.nodes.findAll { it.id != id }
            def notHealthNodes = otherNodes.findAll { !it.isHealthy() }

            notHealthNodes.each { problematicNode ->
                log.info("Problematic node found ${problematicNode.id}")
                cluster.broadcastNodeDown(problematicNode.id)
                def partitionsOfNode = cluster.getPartitiosOf(problematicNode.id)
                partitionsOfNode.each { partition ->
                    Collections.shuffle(partition.replicas)
                    NodeId newLeader = partition.replicas.first()
                    Collection<NodeId> eligibleNodes = (cluster.nodes.id - partition.replicas - problematicNode.id)
                    Collections.shuffle(eligibleNodes)
                    List<NodeId> newReplicas = (partition.replicas - [newLeader])
                    newReplicas.add(eligibleNodes.first())

                    Partition updatedPartition = new Partition(partition.id, newLeader, newReplicas)
                    cluster.redefinePartition(updatedPartition)
                    PartitioningUpdateEvent event = new PartitioningUpdateEvent(
                            PARTITION_NEW_LEADER,
                            updatedPartition)
                    cluster.broadcastPartitionigEvent(event)
                }
            }
        }
    }

    @Override
    void saveLocalReplica(Data data) {
        this.data[data.key] = data
    }

    synchronized void newLeader(Partition partition) {
        log.info("Partition new leader event for $partition")
        if(partition.leader == id) {
            log.info("Im the partition leader for $partition")
            def localReplicas = replicaMap.get(partition.id)
            if(!localReplicas) {
                def otherNodes = cluster.nodes.findAll { it.id != id }
                Collections.shuffle(otherNodes)
                def replicaNodes = otherNodes.take(Cluster.NUMBER_OF_REPLICAS)
                replicaMap.putAll(partition.id, replicaNodes)
                def updatePartition = new Partition(id: partition.id, leader: id, replicas: replicaNodes.id)
                def leaderReplicationDefinition = new PartitioningUpdateEvent(PARTITION_REPLICA_UPDATE,
                        updatePartition)
                cluster.redefinePartition(updatePartition)
                cluster.broadcastPartitionigEvent(leaderReplicationDefinition)
            }
        } else {
            log.info("Im not the partition leader for $partition")
            cluster.redefinePartition(partition)
        }
    }

    synchronized void updateReplicas(Partition partition) {
        if(partition.leader != id) {
            //TODO se eu sou uma das réplicas devo pedir por dados para o master.
            cluster.redefinePartition(partition)
        }
    }

    void setNodeAsDown(NodeId nodeId) {
        def affectedPartitions = replicaMap
                .entries()
                .findAll {it.value.id == nodeId}
                .collect {it.key}

        affectedPartitions.each { index ->
            cluster.nodes.removeAll {it.id == nodeId }
            def eligibleNodes = newArrayList(cluster.nodes)
            Collections.shuffle(eligibleNodes)
            replicaMap.remove(index, nodeId)
            replicaMap.put(index, eligibleNodes.first())

            def redefinedReplicas = replicaMap.get(index).collect {it.id}
            def updatedPartition = new Partition(id: index, leader: id, replicas: redefinedReplicas)
            def leaderReplicationDefinition = new PartitioningUpdateEvent(
                    PARTITION_REPLICA_UPDATE,
                    updatedPartition)
            cluster.broadcastPartitionigEvent(leaderReplicationDefinition)
        }
    }

    def clusterState() {
        return ['my_partitions': replicaMap.entries(),
                'cluster_nodes': cluster.nodes.collect {it.id},
                'cluster_partitions': cluster.partitions]
    }

    private static int calculatePartition(String key) {
        Math.abs(key.hashCode()) % Cluster.NUNBER_OF_PARTITIONS
    }

    private void replicate(int partition, Data data) {
        log.info("Replicating $data")
        replicaMap
                .get(partition)
                .each { it.saveLocalReplica(data) }
        log.info("Data $data replicated.")
    }

    private void defineClusterPartitioning() {
        cluster.distributePartitions()
        cluster.partitions.each {
            def leaderReplicationDefinition = new PartitioningUpdateEvent(
                    PARTITION_NEW_LEADER,
                    it)
            cluster.broadcastPartitionigEvent(leaderReplicationDefinition)
        }
    }
}