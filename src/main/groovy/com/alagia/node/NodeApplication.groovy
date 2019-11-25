package com.alagia.node

import groovy.transform.CompileStatic
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.web.client.RestTemplate

@SpringBootApplication
@CompileStatic
class NodeApplication {

    static void main(String[] args) {
        SpringApplication.run(NodeApplication, args)
    }

    @Bean
    @ConfigurationProperties(prefix = 'nodes')
    ArrayList<NodeId> nodes() {
        return new ArrayList<NodeId>()
    }

    @Bean
    Cluster cluster(@Value('${name}') String name,
                    @Value('${address}') String address,
                    @Value('${port}') int port,
                    ArrayList<NodeId> nodes
    ) {
        def nodeId = new NodeId(name: name, address: address, port: port)
        nodes.add(nodeId)
        def restTemplate = new RestTemplate()
        List<RemoteNode> sort = nodes
                .collect { new RemoteNode(it, restTemplate) }
                .sort { it.id.hashCode() }
        return new Cluster(sort)
    }

    @Bean
    LocalNode node(@Value('${name}') String name,
                   @Value('${address}') String address,
                   @Value('${port}') int port,
                   Cluster cluster) {
        def nodeId = new NodeId(name: name, address: address, port: port)
        return new LocalNode(nodeId, cluster)
    }

}
