package com.alagia.node


import com.fasterxml.jackson.databind.ObjectMapper
import groovy.json.JsonSlurper
import groovy.transform.Canonical
import groovy.util.logging.Slf4j

@Slf4j
class ClusterEventsListener {

    private ObjectMapper objectMapper
    private Cluster cluster

    ClusterEventsListener(ObjectMapper objectMapper,
                          Cluster cluster) {
        this.objectMapper = objectMapper
        this.cluster = cluster
    }

    void listen(String event) {
        log.info("Cluster event received: $event")

        def slurper = new JsonSlurper()
        def evt = slurper.parseText(event)
        if(evt.type == 'partition-replication') {
            ReplicationDefinitionEvent replicationDefinition  = objectMapper.readValue(event, ReplicationDefinitionEvent)
            cluster.saveReplicationInfo(replicationDefinition)
        }
    }
}

@Canonical
class ReplicationDefinitionEvent {
    String id
    String type
    Integer partition
    NodeId leader
    List<NodeId> replicas

    ReplicationDefinitionEvent() {
    }

    ReplicationDefinitionEvent(Integer partition,
                               NodeId leader,
                               List<NodeId> replicas) {
        this.id = UUID.randomUUID().toString()
        this.type = 'partition-replication'
        this.partition = partition
        this.leader = leader
        this.replicas = replicas
    }
}
