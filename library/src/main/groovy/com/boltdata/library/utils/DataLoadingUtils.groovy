package com.boltdata.library.utils

import com.boltdata.library.salesforce.Bulk2API
import com.boltdata.library.salesforce.SFConstants

class DataLoadingUtils {


    static processFile(username, stateMap) {
        processFile(username, stateMap, 'originalFile')
    }

    static processFile(username, stateMap, fileKey) {

        if (fileKey == 'failedFile') {
            stateMap.failedFile = CSVUtils.dropCSVFields(stateMap.failedFile, stateMap.tempDir, SFConstants.errorCSVFields)
        }


        def dataResultMap = Bulk2API.executeUpsert(username, stateMap.objectType, stateMap.externalId, stateMap.tempDir, stateMap.get(fileKey))
        if (dataResultMap.successfulFile) {
            stateMap.successFiles.add(dataResultMap.successfulFile)
        }

        if (dataResultMap.failedFile) {
            stateMap.failedFile = dataResultMap.failedFile
            print "Error processing data. Review file ${stateMap.failedFile} fix data and rerun process"
            stateMap.succeeded = false
        }
        else {
            stateMap.failedFile = ''
            stateMap.succeeded = true
        }

        JSONUtils.saveMapToFile(stateMap.stateFilename, stateMap)

    }

    static getStateMap(stateFilename, objectType, externalId, tempDir) {
        def stateMap = JSONUtils.parseJSONFile(stateFilename)

        if (! stateMap) {
            stateMap = [
                    externalId    : externalId
                    , objectType    : objectType
                    , successFiles: []
                    , failedFile  : ''
                    , originalFile: ''
                    , succeeded    : false
                    , tempDir      : tempDir
                    , stateFilename: stateFilename
            ]
            JSONUtils.saveMapToFile(stateFilename, stateMap)
        }

        return stateMap
    }

    static handleFile(username, objectType, externalId, getDataMethod, destDir) {
        return handleFile(username, objectType, objectType, externalId, getDataMethod, destDir)
    }

    static handleFile(username, uniqueName, objectType, externalId, getDataMethod, destDir) {
        def stateFilename = "${destDir}/${uniqueName}.json".toString()
        def stateMap = getStateMap(stateFilename, objectType, externalId, destDir)

        if (! stateMap.originalFile) {
            stateMap.originalFile = getDataMethod()
        }

        if (! stateMap.failedFile
                && ! stateMap.successFiles) {
            processFile(username, stateMap)
        }
        else if (stateMap.failedFile) {
            processFile(username, stateMap, 'failedFile')
        }
        else {
            stateMap.succeeded = true
            JSONUtils.saveMapToFile(stateMap.stateFilename, stateMap)
        }

        return stateMap.succeeded
    }

}
