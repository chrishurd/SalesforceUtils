package com.boltdata.library.salesforce

import com.boltdata.library.BoltException
import com.boltdata.library.utils.JSONUtils
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPatch
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

class RestAPI {
    static CloseableHttpClient client = HttpClients.createDefault()
    static version = "60.0"

    static updateRecords(username, records) {
        HttpPatch httpPatch = getHttpPatch(username, "composite/sobjects/".toString())
        def requestMap = [
            "allOrNone": true,
            "records": records
        ]
        httpPatch.setEntity(new StringEntity(JSONUtils.jsonMapper(requestMap), ContentType.APPLICATION_JSON))

        CloseableHttpResponse response = client.execute(httpPatch)

        if (response.getStatusLine().getStatusCode() == 200) {
            def httpResponseMap = getResponseData(response)
            def responseList = new ObjectMapper().readValue(httpResponseMap.resultText, List.class)
            def errors = []
            responseList.each { responseMap ->
                if (responseMap.success == false) {
                    errors.add(responseMap.errors)
                }
            }
            response.close()

            if (! errors.isEmpty()) {
                throw new BoltException(JSONUtils.jsonMapper(errors))
            }
            return true
        } else {
            throw new BoltException(EntityUtils.toString(response.getEntity()))
        }
    }

    static getResponseData(response) {
        def responseMap = [:]
        responseMap.put('status', response.getStatusLine().getStatusCode())
        responseMap.put('resultText', EntityUtils.toString(response.getEntity(), "UTF-8"))
        response.close()
        return responseMap
    }

    static getHttpPatch(username, url) {
        HttpPatch httpPatch = new HttpPatch(Connection.getConnectionTokenMap(username).get('urls').get('rest').replaceAll("\\{version\\}", version) + url)
        httpPatch.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpPatch.setHeader('Accept-Encoding', 'gzip')

        return httpPatch
    }
}
