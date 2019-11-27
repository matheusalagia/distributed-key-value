package com.alagia.node

import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.PatternTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter
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
                    ArrayList<NodeId> nodes,
                    StringRedisTemplate redisTemplate
    ) {
        def nodeId = new NodeId(name: name, address: address, port: port)
        nodes.add(nodeId)
        def restTemplate = new RestTemplate()
        List<RemoteNode> sort = nodes
                .collect { new RemoteNode(it, restTemplate) }
                .sort { it.id.hashCode() }
        return new Cluster(sort, redisTemplate)
    }

    @Bean
    LocalNode node(@Value('${name}') String name,
                   @Value('${address}') String address,
                   @Value('${port}') int port,
                   Cluster cluster) {
        def nodeId = new NodeId(name: name, address: address, port: port)
        return new LocalNode(nodeId, cluster)
    }

    @Bean
    RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
                                            MessageListenerAdapter listenerAdapter) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer()
        container.setConnectionFactory(connectionFactory)
        container.addMessageListener(listenerAdapter, new PatternTopic("cluster-events"))

        return container
    }

    @Bean
    MessageListenerAdapter listenerAdapter(ClusterEventsListener eventsListener) {
        return new MessageListenerAdapter(eventsListener, "listen")
    }

    @Bean
    ClusterEventsListener receiver(Cluster cluster) {
        return new ClusterEventsListener(new ObjectMapper(), cluster)
    }

    @Bean
    StringRedisTemplate template(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory)
    }

}
