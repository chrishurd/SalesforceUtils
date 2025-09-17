package com.boltdata.library.utils

import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.xml.XmlMapper

class XMLUtils {

    static parseXMLFile(file) {
        XmlMapper xmlMapper = new XmlMapper()
        return xmlMapper.readValue(new File(file), Map)
    }

    static parseXML(xml) {
        XmlMapper xmlMapper = new XmlMapper()
        xmlMapper.registerModule(new SimpleModule().addDeserializer(Object.class, new ArrayInferringUntypedObjectDeserializer()))
        return (Map) xmlMapper.readValue(xml, Object.class)
    }
}
