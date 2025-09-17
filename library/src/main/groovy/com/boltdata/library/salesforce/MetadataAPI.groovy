package com.boltdata.library.salesforce

import com.sforce.soap.metadata.MetadataConnection
import com.sforce.ws.ConnectorConfig

class MetadataAPI {

    static readMetadata(username, metadataType, metadataItems) {
        def conn = getMetadataConnection(username)
        def readResult = conn.readMetadata(metadataType, metadataItems)
        return readResult
    }

    static upsertMetadata(username, metadata) {
        def conn = getMetadataConnection(username)
        def upsertResult = conn.upsertMetadata(metadata)
        return upsertResult
    }

    static getMetadataConnection(username) {
        def conn = Connection.getConnection(username)
        def sessionId = conn.getSessionHeader().getSessionId()
        def metadataUrl = Connection.getConnectionTokenMap(username).get('urls').get('metadata').replaceAll('\\{version\\}', '52.0')
        final ConnectorConfig config = new ConnectorConfig();
        config.setServiceEndpoint(metadataUrl);
        config.setSessionId(sessionId);
        return new MetadataConnection(config);
    }
}
