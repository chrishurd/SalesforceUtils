package com.boltdata.library.utils

import java.text.SimpleDateFormat

class Utils {

    static getCurrentTimestamp() {
        return new SimpleDateFormat('yyyyMMddHHmmss').format(new Date())
    }

    static getSetFromString(csvString) {
        def fields = csvString.split("[,;\\s]")
        def newFields = [] as Set<String>

        fields.each { field ->
            if (field.trim()) {
                newFields.add(field)
            }
        }

        return newFields
    }

    static nullValue(value, defaultValue) {
        if (! value) {
            return defaultValue
        }

        return value
    }
}
