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

    private Node node

    @Autowired
    RestApi(Node node) {
        this.node = node
    }

    @PostMapping(path = '{key}')
    def save(@PathVariable String key,
             @RequestBody String value) {
        def data = new Data(key: key, value: value)
        log.info("Received save request: $data")
        node.save(data)
    }

    @GetMapping(path = '{key}')
    ResponseEntity<Data> findKey(@PathVariable String key) {
        log.info("Received find by key request $key")
        def result = node.get(key)
        return ResponseEntity.of(result)
    }
}
