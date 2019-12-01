package com.alagia.node

import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
@Slf4j
class RestApi {

    private LocalNode node

    @Autowired
    RestApi(LocalNode node) {
        this.node = node
    }

    @PostMapping(path = 'db/{key}')
    def save(@PathVariable String key,
             @RequestBody String value) {
        def data = new Data(key: key, value: value)
        log.info("Received save request: $data")
        node.save(data)
    }

    @GetMapping(path = 'db/{key}')
    ResponseEntity<Data> findKey(@PathVariable String key) {
        log.info("Received find by key request $key")
        def result = node.get(key)
        return ResponseEntity.of(result)
    }

    @PostMapping(path = 'replica/{key}')
    def saveReplica(@PathVariable String key,
             @RequestBody String value) {
        def data = new Data(key: key, value: value)
        log.info("Received replica save request: $data")
        node.saveLocalReplica(data)
    }

    @GetMapping(path = 'health')
    ResponseEntity health() {
        return ResponseEntity.ok().build()
    }

    @GetMapping(path = 'internal/cluster_state')
    ResponseEntity clusterState() {
        return ResponseEntity.ok(node.clusterState())
    }
}
