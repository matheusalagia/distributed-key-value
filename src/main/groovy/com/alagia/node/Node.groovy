package com.alagia.node

import com.alagia.node.messaging.MessageSender
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
    private MessageSender messageSender

    Node(NodeId id,
         Cluster cluster,
         MessageSender messageSender) {
        this.id = id
        this.cluster = cluster
        this.messageSender = messageSender
    }

    void save(Data data) {
        int dataPartition = calculatePartition(data.key)
        def node = cluster.partitionOwner(dataPartition)

        if (node == id) {
            log.info("Saving $data (partition $dataPartition) on node $id")
            this.data[data.key] = data
        } else {
            log.info("Sending $data (partition $dataPartition) to correct node $node")
            messageSender.saveData(node, data)
        }
    }

    Optional<Data> get(String key) {
        int dataPartition = calculatePartition(key)
        def node = cluster.partitionOwner(dataPartition)

        if (node == id) {
            log.info("Getting data for key $key (partition $dataPartition) on node $id")
            return Optional.ofNullable(data[key])
        }
        log.info("Getting data for key $key (partition $dataPartition) on another node ($id)")
        return messageSender.getData(node, key)
    }


    private static int calculatePartition(Object obj) {
        Math.abs(obj.hashCode()) % Cluster.NUNBER_OF_PARTITIONS
    }
}

@Canonical
class NodeId {
    String name
    String address
    int port
}