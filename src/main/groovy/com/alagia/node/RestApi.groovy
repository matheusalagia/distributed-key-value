package com.alagia.node

import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
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
        log.info("Recebida requisição de save $data")
        node.save(data)
    }
}
