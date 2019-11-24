package com.alagia.node.messaging

import com.alagia.node.Data
import com.alagia.node.NodeId
import groovy.util.logging.Slf4j
import org.springframework.web.client.RestTemplate

@Slf4j
class MessageSender {

//    private StringRedisTemplate template
    private RestTemplate restTemplate

//    void send(Message message) {
//        template.convertAndSend('cluster-commands', message)
//    }

    MessageSender(RestTemplate restTemplate) {
        this.restTemplate = restTemplate
    }

    void saveData(NodeId nodeId, Data data) {
        try {
            String url = buildUrl(nodeId)
            restTemplate.postForLocation(url, data)
        } catch(Exception e) {
            log.error("Error saving Data $data to node $nodeId")
            throw new RuntimeException("Error saving remote data $data to node $nodeId and key <$key>.", e)
        }
    }

    Data getData(NodeId nodeId, String key) {
        try {
            String url = buildUrl(nodeId)
            return restTemplate.getForEntity("$url/$key", Data).getBody() as Data
        } catch(Exception e) {
            log.error("Error getting Data from node $nodeId")
            throw new RuntimeException("Error getting remote data from node $nodeId and key <$key>.", e)
        }
    }

    private static String buildUrl(NodeId nodeId) {
        "http://${nodeId.address}:${nodeId.port}"
    }
}
