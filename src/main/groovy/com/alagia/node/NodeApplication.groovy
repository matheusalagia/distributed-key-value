package com.alagia.node

import com.alagia.node.messaging.MessageReceiver
import com.alagia.node.messaging.MessageSender
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
                    ArrayList<NodeId> nodes) {
        def nodeId = new NodeId(name: name, address: address, port: port)
        nodes.add(nodeId)
        ArrayList sort = nodes.sort { it.hashCode() }
        return new Cluster(sort)
    }

    @Bean
    Node node(@Value('${name}') String name,
              @Value('${address}') String address,
              @Value('${port}') int port,
              Cluster cluster) {
        def nodeId = new NodeId(name: name, address: address, port: port)
        return new Node(nodeId, cluster)
    }

    @Bean
    RedisMessageListenerContainer container(RedisConnectionFactory connectionFactory,
                                            MessageListenerAdapter listenerAdapter) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer()
        container.setConnectionFactory(connectionFactory)
        container.addMessageListener(listenerAdapter, new PatternTopic("cluster-commands"))

        return container
    }

    @Bean
    MessageListenerAdapter listenerAdapter(MessageReceiver receiver) {
        return new MessageListenerAdapter(receiver, "receiveMessage")
    }

    @Bean
    MessageReceiver receiver() {
        return new MessageReceiver()
    }

    @Bean
    StringRedisTemplate template(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory)
    }

    @Bean
    MessageSender messageSender() {
        return new MessageSender(new RestTemplate())
    }

}
