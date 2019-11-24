package com.alagia.node.messaging

import com.alagia.node.Data
import com.alagia.node.NodeId
import groovy.util.logging.Slf4j
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
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
            restTemplate.postForLocation("$url/${data.key}", data.value)
        } catch(Exception e) {
            log.error("Error saving Data $data to node $nodeId")
            throw new RuntimeException("Error saving remote data $data to node $nodeId.", e)
        }
    }

    Optional<Data> getData(NodeId nodeId, String key) {
        try {
            String url = buildUrl(nodeId)
            def data = restTemplate.getForEntity("$url/$key", Data).getBody() as Data
            log.info("Data received $data")
            return Optional.of(data)
        } catch(HttpClientErrorException e) {
            if(e.getStatusCode() == HttpStatus.NOT_FOUND) {
                return Optional.empty()
            }
            throw new RuntimeException("Error getting remote data from node $nodeId and key <$key>.", e)
        } catch(Exception e) {
            log.error("Error getting Data from node $nodeId")
            throw new RuntimeException("Error getting remote data from node $nodeId and key <$key>.", e)
        }
    }

    private static String buildUrl(NodeId nodeId) {
        "http://${nodeId.address}:${nodeId.port}"
    }
}
