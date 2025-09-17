package com.boltdata.library.salesforce

import com.boltdata.library.BoltException
import com.boltdata.library.utils.CSVUtils
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

class ToolingAPI {

    static CloseableHttpClient client = HttpClients.createDefault()
    static version = "56.0"

    static executeQuery(username, String query) {
        HttpGet httpGet = getHttpGet(username, "query/?q=${URLEncoder.encode(query, StandardCharsets.UTF_8.name())}".toString())
        CloseableHttpResponse response = client.execute(httpGet)
        def results
        def httpResponseMap = getResponseData(response)

        if (httpResponseMap.status == 200) {
            results = new ObjectMapper().readValue(httpResponseMap.resultText, Map.class)
            response.close()
        }
        else {
            throw new BoltException(EntityUtils.toString(response.getEntity()))
        }

        return results.records
    }

    static getResponseData(response) {
        def responseMap = [:]
        responseMap.put('status', response.getStatusLine().getStatusCode())
        responseMap.put('resultText', EntityUtils.toString(response.getEntity()))
        response.close()
        return responseMap
    }

    static getHttpGet(username, url) {
        HttpGet httpGet = new HttpGet(Connection.getConnectionTokenMap(username).get('urls').get('tooling_rest').replaceAll("\\{version\\}", version) + url)
        httpGet.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpGet.setHeader('Accept-Encoding', 'gzip')

        return httpGet
    }

    static getHttpPost(username, url) {
        HttpPost httpPost = new HttpPost(Connection.getConnectionTokenMap(username).get('urls').get('tooling_rest').replaceAll("\\{version\\}", version) + url)
        httpPost.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpPost.setHeader('Accept-Encoding', 'gzip')

        return httpPost
    }
}
