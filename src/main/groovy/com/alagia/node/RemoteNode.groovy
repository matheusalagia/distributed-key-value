package com.alagia.node

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.springframework.http.HttpStatus
import org.springframework.web.client.HttpClientErrorException
import org.springframework.web.client.RestTemplate

@Slf4j
@CompileStatic
class RemoteNode implements Node {

    NodeId id
    private String baseUrl
    private RestTemplate restTemplate

    RemoteNode(NodeId id, RestTemplate restTemplate) {
        this.id = id
        this.baseUrl = "http://${id.address}:${id.port}"
        this.restTemplate = restTemplate
    }

    void save(Data data) {
        try {
            String url = "$baseUrl/${data.key}"
            restTemplate.postForLocation(url, data.value)
        } catch(Exception e) {
            log.error("Error saving Data $data to node $id")
            throw new RuntimeException("Error saving remote data $data to node $id.", e)
        }
    }

    Optional<Data> get(String key) {
        try {
            String url = "$baseUrl/$key"
            def data = restTemplate.getForEntity(url, Data).getBody() as Data
            log.info("Data received $data")
            return Optional.of(data)
        } catch(HttpClientErrorException e) {
            if(e.getStatusCode() == HttpStatus.NOT_FOUND) {
                return Optional.empty()
            }
            throw new RuntimeException("Error getting remote data from node $id and key <$key>.", e)
        } catch(Exception e) {
            log.error("Error getting Data from node $id")
            throw new RuntimeException("Error getting remote data from node $id and key <$key>.", e)
        }
    }

    @Override
    void saveLocalReplica(Data data) {
        try {
            String url = "$baseUrl/replica/${data.key}"
            restTemplate.postForLocation(url, data.value)
        } catch(Exception e) {
            log.error("Error replicating Data $data on node $id")
            throw new RuntimeException("Error replicating data $data to node $id.", e)
        }
    }


    @Override
    String toString() {
        return this.id.toString()
    }
}
