package com.alagia.node


import static com.alagia.node.ClusterEventTypes.PARTITION_NEW_LEADER
import static com.alagia.node.ClusterEventTypes.PARTITION_REPLICA_UPDATE

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonSlurper
import groovy.transform.Canonical
import groovy.util.logging.Slf4j

@Slf4j
class ClusterEventsListener {

    private ObjectMapper objectMapper
    private LocalNode localNode

    ClusterEventsListener(ObjectMapper objectMapper,
                          LocalNode localNode) {
        this.objectMapper = objectMapper
        this.localNode = localNode
    }

    void listen(String event) {
        log.info("Cluster event received: $event")

        def slurper = new JsonSlurper()
        def evt = slurper.parseText(event)

        if(evt.type == PARTITION_NEW_LEADER || evt.type == PARTITION_REPLICA_UPDATE) {
            PartitioningUpdateEvent partitioningUpdateEvent  = objectMapper.readValue(event, PartitioningUpdateEvent)

            if(evt.type == PARTITION_NEW_LEADER) {
                localNode.newLeader(partitioningUpdateEvent.partition)
            } else {
                localNode.updateReplicas(partitioningUpdateEvent.partition)
            }
        }

        if(evt.type == ClusterEventTypes.NODE_DOWN) {
            NodeDownEvent nodeDownEvent  = objectMapper.readValue(event, NodeDownEvent)
            localNode.setNodeAsDown(nodeDownEvent.nodeId)
        }
    }
}

@Canonical
class PartitioningUpdateEvent {
    String type
    Partition partition

    PartitioningUpdateEvent() {
    }

    PartitioningUpdateEvent(String type, Partition partition) {
        this.type = type
        this.partition = partition
    }
}

@Canonical
class ClusterEventTypes {
    static final String PARTITION_REPLICA_UPDATE = 'replication-update'
    static final String PARTITION_NEW_LEADER = 'partitioning-new-leader'
    static final String NODE_DOWN = 'node_down'
}

@Canonical
class NodeDownEvent {
    String type = ClusterEventTypes.NODE_DOWN
    NodeId nodeId
}
