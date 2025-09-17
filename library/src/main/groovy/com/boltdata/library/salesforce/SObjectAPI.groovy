package com.boltdata.library.salesforce

import ch.qos.logback.core.util.FileUtil
import com.boltdata.library.BoltException
import com.boltdata.library.utils.JSONUtils
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.io.FileUtils
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPatch
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.entity.mime.MultipartEntityBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

class SObjectAPI {
    static CloseableHttpClient client = HttpClients.createDefault()
    static version = "56.0"

    static getBlob(username, objectType, objectId, field, tempDir) {
        def filename = "${tempDir}/${objectId}"

        HttpGet httpGet = getHttpGet(username, "${objectType}/${objectId}/${field}".toString())
        println "Get ${objectType} - ${objectId}"
        CloseableHttpResponse response = client.execute(httpGet)

        if (response.getStatusLine().getStatusCode() == 200) {
            FileUtils.copyInputStreamToFile(response.getEntity().getContent(), new File(filename))
            response.close()
        }
        else {
            throw new BoltException(EntityUtils.toString(response.getEntity()))
        }

        return filename
    }

    static saveBlobRecord(username, objectType, dataMap, blobFieldName, filename, dataFileName) {
        HttpPost httpPost = getHttpPost(username, objectType)
        MultipartEntityBuilder builder = MultipartEntityBuilder.create()

        builder.addTextBody("entity_content", JSONUtils.jsonMapper(dataMap), ContentType.APPLICATION_JSON)
        builder.addBinaryBody(blobFieldName, new File(filename), ContentType.APPLICATION_OCTET_STREAM, dataFileName)
        httpPost.setEntity(builder.build())

        CloseableHttpResponse response = client.execute(httpPost)

        if (response.getStatusLine().getStatusCode() == 201) {
            def resultMap = new ObjectMapper().readValue(EntityUtils.toString(response.getEntity()), Map.class)
            response.close()
            return resultMap
        } else {
            throw new BoltException(EntityUtils.toString(response.getEntity()))
        }

    }

    static updateRecord(username, objectType, objectId, dataMap) {
        HttpPatch httpPatch = getHttpPatch(username, "${objectType}/${objectId}".toString())
        httpPatch.setEntity(new StringEntity(JSONUtils.jsonMapper(dataMap), ContentType.APPLICATION_JSON))

        CloseableHttpResponse response = client.execute(httpPatch)

        if (response.getStatusLine().getStatusCode() == 204) {
            response.close()
            return true
        } else {
            throw new BoltException(EntityUtils.toString(response.getEntity()))
        }
    }

    static upsertRecord(username, objectType, externalIdField, externalId, dataMap) {
        HttpPatch httpPatch = getHttpPatch(username, "${objectType}/${externalIdField}/${externalId}".toString())
        httpPatch.setEntity(new StringEntity(JSONUtils.jsonMapper(dataMap), ContentType.APPLICATION_JSON))

        CloseableHttpResponse response = client.execute(httpPatch)

        if (response.getStatusLine().getStatusCode() == 200 || response.getStatusLine().getStatusCode() == 201) {
            response.close()
            return true
        } else {
            throw new BoltException(EntityUtils.toString(response.getEntity()))
        }
    }



    static getHttpGet(username, url) {
        HttpGet httpGet = new HttpGet(Connection.getConnectionTokenMap(username).get('urls').get('sobjects').replaceAll("\\{version\\}", version) + url)
        httpGet.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpGet.setHeader('Accept-Encoding', 'gzip')

        return httpGet
    }

    static getHttpPost(username, url) {
        HttpPost httpPost = new HttpPost(Connection.getConnectionTokenMap(username).get('urls').get('sobjects').replaceAll("\\{version\\}", version) + url)
        httpPost.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpPost.setHeader('Accept-Encoding', 'gzip')

        return httpPost
    }

    static getHttpPatch(username, url) {
        HttpPatch httpPatch = new HttpPatch(Connection.getConnectionTokenMap(username).get('urls').get('sobjects').replaceAll("\\{version\\}", version) + url)
        httpPatch.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpPatch.setHeader('Accept-Encoding', 'gzip')

        return httpPatch
    }
}
