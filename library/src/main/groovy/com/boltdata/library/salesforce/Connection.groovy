package com.boltdata.library.salesforce

import com.fasterxml.jackson.databind.ObjectMapper
import com.sforce.soap.partner.PartnerConnection
import com.sforce.ws.ConnectorConfig
import org.apache.http.HttpEntity
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

class Connection {
    private static connectionMap = [:]
    private static connectionTokenMap = [:]
    private static userUrlMap = [:]
    private static sfConnectionInforMap = [:]


    static addConnection(username, password, url) {
        println url
        sfConnectionInforMap.put(username, [password: password, url: url])
        def cConfig = new ConnectorConfig()
        cConfig.setUsername(username)
        cConfig.setPassword(password)
        cConfig.setAuthEndpoint("https://${url}/services/Soap/u/50.0")
        def conn =  new PartnerConnection(cConfig)
        connectionMap.put(username, conn)
        userUrlMap.put(username, url)
    }

    static refreshConnection(username) {
        def connInfo = sfConnectionInforMap.get(username)
        if (connInfo) {
            addConnection(username, connInfo.password, connInfo.url)
        } else {
            throw new IllegalArgumentException("No connection information found for user: ${username}")
        }
    }


    static getConnectionTokenMap(username) {
        if (! connectionTokenMap.containsKey(username))
        {
            def conn = connectionMap.get(username)
            def tokenMap
            CloseableHttpClient client = HttpClients.createDefault()
            HttpGet httpGet = new HttpGet("https://${userUrlMap.get(username)}/id/${conn.getUserInfo().getOrganizationId()}/${conn.getUserInfo().getUserId()}")
            httpGet.setHeader("Authorization", "Bearer ${conn.getSessionHeader().getSessionId()}")
            httpGet.setHeader("Accept", "application/json")

            CloseableHttpResponse response = client.execute(httpGet)

            if (response.getStatusLine().getStatusCode() == 200)
            {
                HttpEntity entity = response.getEntity()
                tokenMap = new ObjectMapper().readValue(EntityUtils.toString(entity), Map.class)
                connectionTokenMap.put(username, tokenMap)
            }
        }

        return connectionTokenMap.get(username)
    }

    static getConnection(username) {
        return connectionMap.get(username)
    }

}
