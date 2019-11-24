package com.alagia.node.messaging

import groovy.util.logging.Slf4j

@Slf4j
class MessageReceiver {

    void receiveMessage(String message) {
        log.info("Received <$message>")
    }
}
