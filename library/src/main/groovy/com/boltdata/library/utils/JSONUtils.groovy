package com.boltdata.library.utils

import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper

class JSONUtils {

    static jsonMapper(jsonMap) {
        return getJsonObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonMap)
    }

    static saveMapToFile(filename, jsonMap) {
        new File(filename).write(getJsonObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(jsonMap), 'UTF-8')
    }

    static getJsonObjectMapper() {
        return JsonMapper.builder().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true).build()
    }

    static parseJSONFile(filename) {
        def tempFile = new File(filename)

        if (tempFile.exists()) {
            return new ObjectMapper().readValue(tempFile, Map.class)
        }

        [:]
    }



    static getJSONAsMap(jsonString) {
        return new ObjectMapper().readValue(jsonString, Map.class)
    }
}
