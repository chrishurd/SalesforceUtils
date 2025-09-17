package com.boltdata.library.salesforce

import com.boltdata.library.BoltException
import com.boltdata.library.utils.CSVUtils
import com.boltdata.library.utils.FileUtils
import com.fasterxml.jackson.databind.MapperFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper
import org.apache.http.HttpEntity
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPatch
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.ContentType
import org.apache.http.entity.FileEntity
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import java.text.SimpleDateFormat

class Bulk2API {

    static CloseableHttpClient client = HttpClients.createDefault()
    static version = "56.0"
    static completedStates = ['JobComplete', 'Aborted'] as Set<String>
    static NO_DATA = 'NoData'

    static executeQuery(username, query, tempDir) {
        println "Executing Query: ${query}"
        return executeQuery(username, query, tempDir, 50000)
    }

    static executeQuery(username, query, tempDir, maxRecords) {

        def requestMap = [
                operation : 'query'
                , query : query
                , columnDelimiter : 'COMMA'
                , contentType : 'CSV'
        ]

        def jobInfoMap = createQueryJob(username, requestMap)
        checkQueryJob(username, jobInfoMap.get('id'))
        def filename = getQueryResults(username, jobInfoMap.get('object'), jobInfoMap.get('id'), maxRecords, tempDir)

        return filename
    }

    static executeUpsertFromArrayMap(username, objectType, externalId, data) {
        def requestMap = [
                columnDelimiter      : 'COMMA'
                , contentType        : 'CSV'
                , object             : objectType
                , externalIdFieldName: externalId
                , operation          : 'upsert'
        ]

        def results = [:]

        def jobInfoMap = createIngestJob(username, requestMap)
        updateDataFromArrayMap(username, data, jobInfoMap.id)
        closeJob(username, jobInfoMap.id)
        def jobResultMap = waitJobComplete(username, jobInfoMap.id)

        if (jobResultMap.numberRecordsProcessed > 0) {
            results.put('success', getProcessedResultsAsArrayMap(username, jobResultMap))
        }

        if (jobResultMap.numberRecordsFailed > 0) {
            results.put('error', getFailedResultsAsArrayMap(username, jobResultMap))
        }

        return results
    }

    static executeUpsert(username, objectType, externalId, tempDir, dataFilename) {

        def files = FileUtils.splitCsvFile(dataFilename, tempDir)
        def successfulFiles = []
        def failureFiles = []

        def results = [:]

        files.each { file ->
            def requestMap = [
                    columnDelimiter      : 'COMMA'
                    , contentType        : 'CSV'
                    , object             : objectType
                    , externalIdFieldName: externalId
                    , operation          : 'upsert'
            ]


            def jobInfoMap = createIngestJob(username, requestMap)
            uploadData(username, tempDir, file, jobInfoMap.id)
            closeJob(username, jobInfoMap.id)
            def jobResultMap = waitJobComplete(username, jobInfoMap.id)

            if (jobResultMap.numberRecordsProcessed > 0) {
                successfulFiles.add(getProcessedResults(username, jobResultMap, tempDir))
            }

            if (jobResultMap.numberRecordsFailed > 0) {
                failureFiles.add(getFailedResults(username, jobResultMap, tempDir))
            }
        }

        if (! successfulFiles.isEmpty())
        {
            results.successfulFile = CSVUtils.combineFiles(successfulFiles, "Success_${objectType}", tempDir)
        }

        if (! failureFiles.isEmpty()) {
            results.failedFile = CSVUtils.combineFiles(failureFiles, "Failure_${objectType}", tempDir)
        }

        return results

    }

    static executeDelete(username, objectType, tempDir, dataFilename) {

        def files = FileUtils.splitCsvFile(dataFilename, tempDir)
        def successfulFiles = []
        def failureFiles = []

        def results = [:]

        files.each { file ->
            def requestMap = [
                    columnDelimiter      : 'COMMA'
                    , contentType        : 'CSV'
                    , object             : objectType
                    , operation          : 'delete'
            ]


            def jobInfoMap = createIngestJob(username, requestMap)
            uploadData(username, tempDir, file, jobInfoMap.id)
            closeJob(username, jobInfoMap.id)
            def jobResultMap = waitJobComplete(username, jobInfoMap.id)

            if (jobResultMap.numberRecordsProcessed > 0) {
                successfulFiles.add(getProcessedResults(username, jobResultMap, tempDir))
            }

            if (jobResultMap.numberRecordsFailed > 0) {
                failureFiles.add(getFailedResults(username, jobResultMap, tempDir))
            }
        }

        if (! successfulFiles.isEmpty())
        {
            results.successfulFile = CSVUtils.combineFiles(successfulFiles, "Success_${objectType}", tempDir)
        }

        if (! failureFiles.isEmpty()) {
            results.failedFile = CSVUtils.combineFiles(failureFiles, "Failure_${objectType}", tempDir)
        }

        return results

    }

    static executeInsert(username, objectType, tempDir, dataFilename) {
        def files = FileUtils.splitCsvFile(dataFilename, tempDir)
        def successfuleFiles = []
        def failureFiles = []

        def results = [:]

        files.each { file ->

            def requestMap = [
                    columnDelimiter: 'COMMA'
                    , contentType: 'CSV'
                    , object : objectType
                    , operation: 'insert'
            ]

            def jobInfoMap = createIngestJob(username, requestMap)
            uploadData(username, tempDir, file, jobInfoMap.id)
            closeJob(username, jobInfoMap.id)
            def jobResultMap = waitJobComplete(username, jobInfoMap.id)

            if (jobResultMap.numberRecordsProcessed > 0) {
                successfuleFiles.add(getProcessedResults(username, jobResultMap, tempDir))
            }

            if (jobResultMap.numberRecordsFailed > 0) {
                failureFiles.add(getFailedResults(username, jobResultMap, tempDir))
            }
        }

        if (! successfuleFiles.isEmpty())
        {
            results.successfulFile = CSVUtils.combineFiles(successfuleFiles, "Success_${objectType}", tempDir)
        }

        if (! failureFiles.isEmpty()) {
            results.failedFile = CSVUtils.combineFiles(failureFiles, "Failure_${objectType}", tempDir)
        }

        return results

    }

    static getFailedResultsAsArrayMap(username, jobResultMap) {
        HttpGet httpGet = getBulkHttpGet(username, "jobs/ingest/${jobResultMap.id}/failedResults", "application/json")
        println "Getting Successful Results"
        CloseableHttpResponse response = client.execute(httpGet)

        def httpResponseMap = getResponseData(response)

        if (httpResponseMap.status == 200) {
            return CSVUtils.getCsvReader(httpResponseMap.resultText)
        }
        else {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static getFailedResults(username, jobResultMap, tempDir) {
        HttpGet httpGet = getBulkHttpGet(username, "jobs/ingest/${jobResultMap.id}/failedResults", "application/json")
        println "Getting Successful Results"
        CloseableHttpResponse response = client.execute(httpGet)

        def httpResponseMap = getResponseData(response)

        if (httpResponseMap.status == 200) {
            def filename = "${tempDir}/Failed_${jobResultMap.object}_${new SimpleDateFormat('yyyyMMddHHmmss').format(new Date())}.csv".toString()
            new File(filename).write(httpResponseMap.resultText, 'UTF-8')
            return filename
        }
        else {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static getProcessedResultsAsArrayMap(username, jobResultMap) {
        HttpGet httpGet = getBulkHttpGet(username, "jobs/ingest/${jobResultMap.id}/successfulResults", "application/json")
        println "Getting Successful Results"
        CloseableHttpResponse response = client.execute(httpGet)


        def httpResponseMap = getResponseData(response)

        if (httpResponseMap.status == 200) {
            return CSVUtils.getCsvReader(httpResponseMap.resultText)
        }
        else {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static getProcessedResults(username, jobResultMap, tempDir) {
        HttpGet httpGet = getBulkHttpGet(username, "jobs/ingest/${jobResultMap.id}/successfulResults", "application/json")
        println "Getting Successful Results"
        CloseableHttpResponse response = client.execute(httpGet)


        def httpResponseMap = getResponseData(response)

        if (httpResponseMap.status == 200) {
            def filename = "${tempDir}/Success_${jobResultMap.object}_${new SimpleDateFormat('yyyyMMddHHmmss').format(new Date())}.csv".toString()
            new File(filename).write(httpResponseMap.resultText, 'UTF-8')
            return filename
        }
        else {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static waitJobComplete(username, jobId) {
        while (true) {
            HttpGet httpGet = getBulkHttpGet(username, "jobs/ingest/${jobId}", "application/json")
            CloseableHttpResponse response = client.execute(httpGet)


            def httpResponseMap = getResponseData(response)

            if (httpResponseMap.status == 200) {

                def responseMap = new ObjectMapper().readValue(httpResponseMap.resultText, Map.class)
                if (completedStates.contains(responseMap.state)) {
                    return responseMap
                }
                else if (responseMap.state == 'Failed') {
                    throw new BoltException(responseMap.errorMessage)
                }
                else {
                    println "Waiting on job complete"
                    sleep(30000)
                }
            }
            else {
                throw new BoltException(httpResponseMap.resultText)
            }
        }
    }

    static closeJob(username, jobId) {
        HttpPatch httpPatch = getBulkHttpPatch(username, "jobs/ingest/${jobId}", "application/json")
        httpPatch.setEntity(new StringEntity(getJsonObjectMapper().writeValueAsString([state: "UploadComplete"]), ContentType.APPLICATION_JSON))
        println "Closing Job"
        CloseableHttpResponse response = client.execute(httpPatch)

        def httpResponseMap = getResponseData(response)


        if (httpResponseMap.status == 200) {
            def responseMap = new ObjectMapper().readValue(httpResponseMap.resultText, Map.class)

            return responseMap
        }
        else {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static updateDataFromArrayMap(username, data, jobId) {
        HttpPut httpPut = getBulkHttpPut(username, "jobs/ingest/${jobId}/batches", "text/csv;charset=utf-8")
        httpPut.setEntity(new StringEntity(CSVUtils.getCsvWriterFromArrayMap(data), ContentType.create("text/csv", "utf-8")))
        CloseableHttpResponse response = client.execute(httpPut)
        def httpResponseMap = getResponseData(response)

        if (httpResponseMap.status != 201) {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static uploadData(username, tempDir, dataFilename, jobId) {
        HttpPut httpPut = getBulkHttpPut(username, "jobs/ingest/${jobId}/batches", "text/csv;charset=utf-8")
        httpPut.setEntity(new FileEntity(new File(dataFilename)))
        println "Ingesting Data: ${dataFilename}"
        CloseableHttpResponse response = client.execute(httpPut)
        def httpResponseMap = getResponseData(response)

        if (httpResponseMap.status != 201) {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static createIngestJob(username, jobInfoMap) {
        HttpPost httpPost = getBulkHttpPost(username, "jobs/ingest", "application/json")
        httpPost.setEntity(new StringEntity(getJsonObjectMapper().writeValueAsString(jobInfoMap), ContentType.APPLICATION_JSON))
        println "Creating Bulk Job"
        CloseableHttpResponse response = client.execute(httpPost)

        def httpResponseMap = getResponseData(response)
        def responseMap = new ObjectMapper().readValue(httpResponseMap.resultText, Map.class)
        if (httpResponseMap.status == 200) {
            return responseMap
        }
        else {
            throw new BoltException(responseMap)
        }
    }


    static getQueryResults(username, objectType, queryJobId, maxRecords, tempDir) {
        def filename = "${tempDir}/${objectType}${UUID.randomUUID().toString()}.csv".toString()
        def columns = []
        def output = new File(filename).newWriter('UTF-8')
        def recordsFound = false
        def locatorId

        do {
            def params = ["maxRecords=${maxRecords}"]
            if (locatorId) {
                params.add("locator=${locatorId}")
            }
            HttpGet httpGet = getBulkHttpGet(username, "jobs/query/${queryJobId}/results?${String.join('&', params)}", 'text/csv')
            println 'Getting Job'
            CloseableHttpResponse response = client.execute(httpGet)

            def httpResponseMap = getResponseData(response)

            if (httpResponseMap.status == 200) {
                def resultText = httpResponseMap.resultText
                def cvsDataMap = CSVUtils.getCsvReader(resultText)
                if (! cvsDataMap.isEmpty()) {
                    if (!columns) {
                        columns = cvsDataMap.get(0).keySet()
                        output.write(CSVUtils.getCsvWriter(cvsDataMap, columns.asList()))
                    } else {
                        output.write(CSVUtils.getCsvWriter(cvsDataMap, columns, false))
                    }
                    recordsFound = true
                }
                locatorId = response.getFirstHeader('Sforce-Locator')?.value
            } else {
                if (httpResponseMap.resultText.contains('INVALID_SESSION_ID')) {
                    Connection.refreshConnection(username)
                    getQueryResults(username, objectType, queryJobId, maxRecords, tempDir) // Retry the request
                }
                else {
                    throw new BoltException(httpResponseMap.resultText)
                }
            }
        }
        while (locatorId
                && locatorId != "null")

        output.close()
        return (recordsFound ? filename : NO_DATA)

    }

    static createQueryJob(username, jobInfoMap) {

        HttpPost httpPost = getBulkHttpPost(username, "jobs/query", "application/json")
        httpPost.setEntity(new StringEntity(getJsonObjectMapper().writeValueAsString(jobInfoMap), ContentType.APPLICATION_JSON))
        println "Creating Bulk Job"
        CloseableHttpResponse response = client.execute(httpPost)

        def httpResponseMap = getResponseData(response)
        def responseMap = new ObjectMapper().readValue(httpResponseMap.resultText, Map.class)

        if (httpResponseMap.status == 200) {
            return responseMap
        }
        else {
            throw new BoltException(httpResponseMap.resultText)
        }
    }

    static checkQueryJob(username, jobId) {
        def isComplete = false
        println 'Checking Query Job Status'
        while (! isComplete) {
            HttpGet httpGet = getBulkHttpGet(username, "jobs/query/${jobId}", 'application/json')

            CloseableHttpResponse response = client.execute(httpGet)


            def httpResponseMap = getResponseData(response)

            if (httpResponseMap.resultText.contains('INVALID_SESSION_ID')) {
                Connection.refreshConnection(username)
            }
            else {
                def responseMap = new ObjectMapper().readValue(httpResponseMap.resultText, Map.class)

                if (httpResponseMap.status == 200) {

                    if (responseMap.get('state') == 'JobComplete') {
                        isComplete = true
                        println "Query Job Processing Complete"
                    } else {
                        println "Waiting on Query Job"
                        sleep(30000)
                    }

                } else {
                    throw new BoltException(responseMap)
                }
            }
        }
    }





    static getBulkHttpPost(username, url, contentType) {
        HttpPost httpPost = new HttpPost(Connection.getConnectionTokenMap(username).get('urls').get('rest').replaceAll("\\{version\\}", version) + url)
        httpPost.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpPost.setHeader("Content-Type", contentType)
        httpPost.setHeader('Accept-Encoding', 'gzip')

        return httpPost
    }

    static getBulkHttpPut(username, url, contentType) {
        HttpPut httpPut = new HttpPut(Connection.getConnectionTokenMap(username).get('urls').get('rest').replaceAll("\\{version\\}", version) + url)
        httpPut.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpPut.setHeader("Content-Type", contentType)
        httpPut.setHeader('Accept-Encoding', 'gzip')

        return httpPut
    }

    static getBulkHttpPatch(username, url, contentType) {
        HttpPatch httpPatch = new HttpPatch(Connection.getConnectionTokenMap(username).get('urls').get('rest').replaceAll("\\{version\\}", version) + url)
        httpPatch.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpPatch.setHeader("Content-Type", contentType)

        return httpPatch
    }

    static getBulkHttpGet(username, url, contentType) {
        HttpGet httpGet = new HttpGet(Connection.getConnectionTokenMap(username).get('urls').get('rest').replaceAll("\\{version\\}", version) + url)
        httpGet.setHeader("Authorization", "Bearer ${Connection.getConnection(username).getSessionHeader().getSessionId()}")
        httpGet.setHeader("Content-Type", contentType)
        httpGet.setHeader('Accept-Encoding', 'gzip')

        return httpGet
    }

    static getResponseData(response) {
        def responseMap = [:]
        responseMap.put('status', response.getStatusLine().getStatusCode())
        responseMap.put('resultText', EntityUtils.toString(response.getEntity(), "UTF-8"))
        response.close()
        return responseMap
    }

    static getJsonObjectMapper() {
        return JsonMapper.builder().configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true).build()
    }
}
