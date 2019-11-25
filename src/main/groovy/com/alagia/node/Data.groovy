package com.alagia.node

import groovy.transform.Canonical
import groovy.transform.ToString

@Canonical
@ToString(includePackage = false)
class Data {
    String key
    String value
}
