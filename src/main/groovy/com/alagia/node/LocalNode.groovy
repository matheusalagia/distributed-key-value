package com.alagia.node


import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.Multimap
import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.util.logging.Slf4j

@CompileStatic
@EqualsAndHashCode
@Slf4j
class LocalNode implements Node {

    private Cluster cluster
    private Map<String, Data> data = [:]
    private Multimap<Integer, Node> replicaMap = ArrayListMultimap.create()

    LocalNode(NodeId id,
              Cluster cluster) {
        this.id = id
        this.cluster = cluster
        buildReplicaMap()
    }

    @Override
    void save(Data data) {
        int partition = calculatePartition(data.key)
        def node = cluster.partitionLeader(partition)

        if (node.id == id) {
            log.info("Saving $data (partition $partition) on node $id")
            this.data[data.key] = data
            replicate(partition, data)
        } else {
            log.info("Sending $data (partition $partition) to correct node ${node.id}")
            node.save(data)
        }
    }

    @Override
    Optional<Data> get(String key) {
        int dataPartition = calculatePartition(key)
        def node = cluster.partitionLeader(dataPartition)

        if (node.id == id) {
            log.info("Getting data for key $key (partition $dataPartition) on node $id")
            return Optional.ofNullable(data[key])
        }
        log.info("Getting data for key $key (partition $dataPartition) on another node ($id)")
        return node.get(key)
    }

    @Override
    void saveLocalReplica(Data data) {
        this.data[data.key] = data
    }

    private void replicate(int partition, Data data) {
        log.info("Replicating $data")
        replicaMap
                .get(partition)
                .each { it.saveLocalReplica(data) }
        log.info("Data $data replicated.")
    }

    private void buildReplicaMap() {
        def otherNodes = cluster.nodes.findAll { it.id != id }
        def nodePartitions = cluster.partitions.findAll { it.leader.id == id }

        nodePartitions.each {
            Collections.shuffle(otherNodes)
            replicaMap.putAll(it.id, otherNodes.take(Cluster.NUMBER_OF_REPLICAS))
        }

        log.info("Replication map: ${replicaMap}")
    }
}