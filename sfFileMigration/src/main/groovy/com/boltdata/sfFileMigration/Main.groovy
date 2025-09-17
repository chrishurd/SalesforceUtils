package com.boltdata.sfFileMigration

import com.boltdata.library.salesforce.Bulk2API
import com.boltdata.library.salesforce.Connection
import com.boltdata.library.salesforce.PartnerAPI
import com.boltdata.library.salesforce.SObjectAPI
import com.boltdata.library.utils.CSVUtils
import com.boltdata.library.utils.FieldUtils
import com.boltdata.library.utils.FileUtils
import com.boltdata.library.utils.JSONUtils
import com.boltdata.library.utils.SFObjectUtils

class Main {
    static fromUsername
    static toUsername
    static configDirectory
    static workingDirectory
    static configMap
    static stateMap
    static stateMapFilename

    static main(args) {
        def action = (args ? args[1] : '--linkFiles')
        configDirectory = (args ? args[0] : '/Users/chrishurd/Downloads/fileMigration')
        def filePart = (args && args.size() > 2 ? args[2] : 'attachment1')
        initiate()

        if (action == '--retrieveFiles') {
            retrieveFiles(filePart)
        }
        else if (action == '--uploadFiles') {
            uploadFiles(filePart)
        }
        else if (action == '--initializeData') {
            initializeData(false)
        }
        else if (action == '--verifyData') {
            stateMap.primaryContentVersionFilename = null
            stateMap.secondaryContentVersionFilename = null
            stateMap.contentVersionFilename = null
            stateMap.contentDocumentLinkFilename = null
            stateMap.attachmentFilename = null
            stateMap.processedContentVersionComplete = false
            stateMap.processedAttachmentComplete = false
            stateMap.linkFilesVersionFile = null

            configMap.objects.each { sfObject, sfObjectMap ->
                def stateObjectMap = stateMap.objects.get(sfObject)
                if (stateObjectMap) {
                    stateObjectMap.recordIdFilename = null
                    stateObjectMap.destObjectIdFile = null
                    stateObjectMap.linkingComplete = false
                    stateObjectMap.initializeComplete = false
                }
            }
            updateStateMap()



            initializeData(true)
        }
        else if (action == '--linkFiles') {

            configMap.objects.each { sfObject, sfObjectMap ->
                def stateObjectMap = stateMap.objects.get(sfObject)
                stateObjectMap.resultLinkErrorFiles = null
                stateObjectMap.destObjectIdFile = null
            }
            updateStateMap()
            linkFiles()
        }
    }

    static linkFiles() {
        Connection.addConnection(configMap.toOrg.username, configMap.toOrg.password, configMap.toOrg.url)
        toUsername = configMap.toOrg.username
        def mappingIterator
        def docIdMap = getSourceDocIdMap("linkFilesVersionFile")

        if (stateMap.contentDocumentLinkFilename
                && stateMap.contentDocumentLinkFilename != 'NoData') {

            configMap.objects.each { sfObject, sfObjectMap ->
                def stateObjectMap = stateMap.objects.get(sfObject)

                if (! stateObjectMap.linkingComplete) {
                    def recordIds = [] as Set<String>
                    def rows = []
                    def key = "${sfObjectMap.destinationObject}:LinkedEntity.${sfObjectMap.externalId}".toString()
                    def processedIds = [] as Set<String>
                    def tempDir = "${workingDirectory}/${sfObject}".toString()

                    FileUtils.createDir(tempDir)

                    println "Linking files for ${sfObjectMap.sourceObject}"

                    if (!stateObjectMap.destObjectIdFile) {
                        stateObjectMap.destObjectIdFile = Bulk2API.executeQuery(toUsername, "SELECT Id, ${sfObjectMap.externalId} FROM ${sfObjectMap.destinationObject} WHERE ${sfObjectMap.externalId} != null".toString(), tempDir)
                        updateStateMap()
                    }

                    if (stateObjectMap.destObjectIdFile != 'NoData') {

                        mappingIterator = CSVUtils.getCsvFileReader(stateObjectMap.destObjectIdFile)
                        while (mappingIterator.hasNext()) {
                            def record = mappingIterator.next()
                            recordIds.add(record.get(sfObjectMap.externalId))
                        }


                        if (stateObjectMap.resultLinkFiles
                                && stateObjectMap.resultLinkFiles.size() > 0) {
                            stateObjectMap.resultLinkFiles.each { file ->
                                mappingIterator = CSVUtils.getCsvFileReader(file)

                                while (mappingIterator.hasNext()) {
                                    def record = mappingIterator.next()
                                    processedIds.add("${record.get(key)}:${record.ContentDocumentId}".toString())
                                }
                            }
                        }

                        mappingIterator = CSVUtils.getCsvFileReader(stateMap.contentDocumentLinkFilename)

                        while (mappingIterator.hasNext()) {
                            def record = mappingIterator.next()
                            def processedId = "${record.LinkedEntityId}:${docIdMap.get(record.ContentDocumentId)}".toString()

                            if (rows.size() >= 1000000) {
                                processLinkRows(rows, sfObjectMap, stateObjectMap, tempDir)
                                rows = []
                            }

                            if (docIdMap.get(record.ContentDocumentId)
                                    && recordIds.contains(record.LinkedEntityId)
                                    && !processedIds.contains(processedId)
                                    && sfObjectMap.sourceObject == record.'LinkedEntity.Type') {
                                def row = [
                                        ContentDocumentId: docIdMap.get(record.ContentDocumentId)
                                        , ShareType      : record.ShareType
                                        , Visibility     : record.Visibility
                                        , Id             : ''
                                ]

                                row.put(key, record.LinkedEntityId)

                                rows.add(row)
                            }
                        }

                        mappingIterator = CSVUtils.getCsvFileReader(stateMap.attachmentFilename)

                        while (mappingIterator.hasNext()) {
                            def record = mappingIterator.next()
                            def processedId = "${record.ParentId}:${docIdMap.get(record.Id)}".toString()

                            if (rows.size() >= 1000000) {
                                processLinkRows(rows, sfObjectMap, stateObjectMap, tempDir)
                                rows = []
                            }

                            if (docIdMap.get(record.Id)
                                    && recordIds.contains(record.ParentId)
                                    && !processedIds.contains(processedId)
                                    && sfObjectMap.sourceObject == record.'Parent.Type') {
                                def row = [
                                        ContentDocumentId: docIdMap.get(record.Id)
                                        , ShareType      : 'V'
                                        , Visibility     : 'AllUsers'
                                        , Id             : ''
                                ]

                                row.put(key, record.ParentId)

                                rows.add(row)
                            }
                        }

                        if (!rows.isEmpty()) {
                            processLinkRows(rows, sfObjectMap, stateObjectMap, tempDir)
                        }

                        if (!stateObjectMap.resultLinkErrorFiles
                                || stateObjectMap.resultLinkErrorFiles.isEmpty()) {
                            stateObjectMap.linkingComplete = true
                            updateStateMap()
                        }
                    }
                }
            }
        }
    }

    static processLinkRows(rows, sfObjectMap, stateObjectMap, tempDir) {
        println "Writing ${rows.size()} rows to file"
        def filename = "${tempDir}/contentDocumentLink.csv".toString()
        CSVUtils.writeCsvWriterFromArrayMap(filename, rows)
        def resultMap = Bulk2API.executeUpsert(toUsername, 'ContentDocumentLink', 'Id', tempDir, filename)

        if (resultMap.successfulFile) {
            if (! stateObjectMap.resultLinkFiles) {
                stateObjectMap.resultLinkFiles = []
            }

            stateObjectMap.resultLinkFiles.add(resultMap.successfulFile)
            updateStateMap()
        }

        if (resultMap.failedFile) {
            if (! stateObjectMap.resultLinkErrorFiles) {
                stateObjectMap.resultLinkErrorFiles = []
            }

            stateObjectMap.resultLinkErrorFiles.add(resultMap.failedFile)
            updateStateMap()
        }
    }

    static getSourceDocIdMap(stateFilename) {
        def docIdMap = [:]
        def documentMapFile

        if (! stateFilename
                || ! stateMap."${stateFilename}") {
            documentMapFile = Bulk2API.executeQuery(toUsername
                , "SELECT ContentDocumentId, ${configMap.ContentVersion.externalId} FROM ContentVersion WHERE ${configMap.ContentVersion.externalId} != null".toString(), workingDirectory)

            if (stateFilename) {
                stateMap."${stateFilename}" = documentMapFile
                updateStateMap()
            }
        }
        else {
            documentMapFile = stateMap."${stateFilename}"
        }


        def mappingIterator = CSVUtils.getCsvFileReader(documentMapFile)

        while (mappingIterator.hasNext()) {
            def record = mappingIterator.next()
            docIdMap.put(record.get(configMap.ContentVersion.externalId), record.ContentDocumentId)
        }

        return docIdMap
    }

    static initializeData(verify) {
        Connection.addConnection(configMap.fromOrg.username, configMap.fromOrg.password, configMap.fromOrg.url)
        fromUsername = configMap.fromOrg.username

        if (!stateMap.primaryContentVersionFilename) {
            def whereClause = "VersionNumber = '1' AND IsLatest = false"
            if (configMap.ContentVersion.whereClause) {
                whereClause = "(${configMap.ContentVersion.whereClause}) AND ${whereClause}"
            }
            stateMap.primaryContentVersionFilename = SFObjectUtils.queryObject(fromUsername, 'ContentVersion', whereClause, "CreatedDate ASC", configMap.ContentVersion.excludedFields, configMap.ContentVersion.includeFields, workingDirectory)
            updateStateMap()
        }

        if (!stateMap.secondaryContentVersionFilename) {
            def whereClause = "VersionNumber != '1' "
            if (configMap.ContentVersion.whereClause) {
                whereClause = "(${configMap.ContentVersion.whereClause}) AND ${whereClause}"
            }
            stateMap.secondaryContentVersionFilename = SFObjectUtils.queryObject(fromUsername, 'ContentVersion', whereClause, "CreatedDate ASC", configMap.ContentVersion.excludedFields, configMap.ContentVersion.includeFields, workingDirectory)
            updateStateMap()
        }

        // Get all contentVersion
        if (!stateMap.contentVersionFilename) {
            def whereClause = "VersionNumber = '1' AND IsLatest = true"
            if (configMap.ContentVersion.whereClause) {
                whereClause = "(${configMap.ContentVersion.whereClause}) AND ${whereClause}"
            }

            stateMap.contentVersionFilename = SFObjectUtils.queryObject(fromUsername, 'ContentVersion', whereClause, "CreatedDate DESC", configMap.ContentVersion.excludedFields, configMap.ContentVersion.includeFields, workingDirectory)
            updateStateMap()
        }

        if (!stateMap.contentDocumentLinkFilename) {
            stateMap.contentDocumentLinkFilename = SFObjectUtils.queryObject(fromUsername, 'ContentDocumentLink', configMap.ContentDocumentLink.whereClause, configMap.ContentDocumentLink.orderBy, configMap.ContentDocumentLink.excludedFields, configMap.ContentDocumentLink.includeFields, workingDirectory)
            updateStateMap()
        }

        if (!stateMap.attachmentFilename) {
            stateMap.attachmentFilename = SFObjectUtils.queryObject(fromUsername, 'Attachment', configMap.Attachment.whereClause, 'CreatedDate DESC', configMap.Attachment.excludedFields, configMap.Attachment.includeFields, workingDirectory)
            updateStateMap()
        }

        if (!stateMap.objects) {
            stateMap.objects = [:]
        }


        configMap.objects.each { sfObject, sfObjectMap ->

            if (!stateMap.objects.containsKey(sfObject)) {
                stateMap.objects.put(sfObject, [:])
            }

            def objectStateMap = stateMap.objects.get(sfObject)

            if (!objectStateMap.initializeComplete
                    && !objectStateMap.recordIdFilename) {

                def objectDirectory = "${workingDirectory}/${sfObject}"
                FileUtils.createDir(objectDirectory.toString())

                objectStateMap.recordIdFilename = SFObjectUtils.queryRecordIds(fromUsername, sfObjectMap.sourceObject, sfObjectMap.whereClause, objectDirectory)
                updateStateMap()
            }

            objectStateMap.initializeComplete = true
            updateStateMap()
        }

        if (! stateMap.processedContentVersionComplete
                || ! stateMap.processedAttachmentComplete) {

            def recordIds = getRecordIds()
            def existingIds = [] as Set<String>
            Connection.addConnection(configMap.toOrg.username, configMap.toOrg.password, configMap.toOrg.url)
            toUsername = configMap.toOrg.username

            if (verify) {
                def existingVersionsFile = Bulk2API.executeQuery(toUsername, "SELECT ${configMap.ContentVersion.externalId} FROM ContentVersion WHERE ${configMap.ContentVersion.externalId} != null".toString(), workingDirectory)

                def existingMappingIterator = CSVUtils.getCsvFileReader(existingVersionsFile)
                while (existingMappingIterator.hasNext()) {
                    def record = existingMappingIterator.next()
                    existingIds.add(record.get(configMap.ContentVersion.externalId))
                }
            }

            if (stateMap.contentVersionFilename
                    && stateMap.contentVersionFilename != 'NoData'
                    && ! stateMap.processedContentVersionComplete) {

                def documentIds = [] as Set<String>

                def mappingIterator = CSVUtils.getCsvFileReader(stateMap.contentDocumentLinkFilename)

                while (mappingIterator.hasNext()) {
                    def record = mappingIterator.next()
                    if (recordIds.contains(record.get('LinkedEntityId'))) {
                        documentIds.add(record.ContentDocumentId)
                    }
                }

                if (stateMap.primaryContentVersionFilename
                        && stateMap.primaryContentVersionFilename != 'NoData') {
                    createFile('contentVersionPrimary', stateMap.primaryContentVersionFilename, 'ContentDocumentId', documentIds, 'ContentDocumentId', existingIds)
                }

                if (stateMap.secondaryContentVersionFilename
                        && stateMap.secondaryContentVersionFilename != 'NoData') {
                    createFile('contentVersionSecondary', stateMap.secondaryContentVersionFilename, 'ContentDocumentId', documentIds, 'Id', existingIds)
                }

                if (stateMap.contentVersionFilename
                        && stateMap.contentVersionFilename != 'NoData') {
                    createFile('contentVersion', stateMap.contentVersionFilename, 'ContentDocumentId', documentIds, 'ContentDocumentId', existingIds)
                }

                stateMap.processedContentVersionComplete = true
                updateStateMap()
            }

            if (stateMap.attachmentFilename
                    && stateMap.attachmentFilename != 'NoData'
                    && ! stateMap.processedAttachmentComplete) {

                createFile('attachment', stateMap.attachmentFilename, 'ParentId', recordIds, 'Id', existingIds)

                stateMap.processedAttachmentComplete = true
                updateStateMap()
            }

        }


    }

    static getRecordIds() {
        def recordIds = [] as Set<String>

        stateMap.objects.each { sfObject, sfObjectMap ->
            if (sfObjectMap.recordIdFilename
                    && sfObjectMap.recordIdFilename != 'NoData') {
                def mappingIterator = CSVUtils.getCsvFileReader(sfObjectMap.recordIdFilename)
                while (mappingIterator.hasNext()) {
                    def record = mappingIterator.next()
                    recordIds.add(record.get('Id'))
                }
            }
        }

        return recordIds
    }

    static createFile(filePrefix, filename, field, idSet) {
        createFile(filePrefix, filename, field, idSet, null, null)
    }

    static createFile(filePrefix, filename, field, idSet, excludeField, excludeIds) {
        def records = []
        def count = 0

        def mappingIterator = CSVUtils.getCsvFileReader(filename)

        while (mappingIterator.hasNext()) {
            def record = mappingIterator.next()

            if (records.size() > configMap.batchSize) {
                saveDocumentFile("${filePrefix}${++count}".toString(), records)
                records = []
            }

            if (idSet.contains(record.get(field))
                    && (excludeField == null || ! excludeIds.contains(record.get(excludeField)))) {
                records.add(record)
            }
        }

        if (records.size() > 0) {
            saveDocumentFile("${filePrefix}${++count}".toString(), records)
        }
    }

    static saveDocumentFile(filePrefix, records ) {
        def dir = "${workingDirectory}/files/${filePrefix}".toString()
        if (! FileUtils.doesExist(dir)) {
            FileUtils.createDir(dir)
        }
        def filename = "${dir}/${filePrefix}.csv".toString()
        CSVUtils.writeCsvWriterFromArrayMap(filename, records)
    }

    static uploadFiles(filePart) {


        Connection.addConnection(configMap.toOrg.username, configMap.toOrg.password, configMap.toOrg.url)
        toUsername = configMap.toOrg.username
        def mappingIterator

        def filePartDirectory = "${workingDirectory}/files/${filePart}".toString()
        def fileDirectory = "${filePartDirectory}/files".toString()
        def loadedDirectory = "${filePartDirectory}/loadedFiles".toString()
        def docIdMap = [:]
        def userMap = [:]
        def recordTypeMap = [:]

        PartnerAPI.querySFDC(toUsername, "SELECT Id, Name FROM RecordType WHERE SObjectType = 'ContentVersion'").each { recordType ->
            recordTypeMap.put(recordType.getField('Name'), recordType.getId())
        }

        if (configMap.ContentVersion.mapCreatedBy) {
            PartnerAPI.querySFDC(toUsername, "SELECT Id, ${configMap.User.externalId} FROM User WHERE ${configMap.User.externalId} != null".toString()).each { user ->
                userMap.put(user.getField(configMap.User.externalId), user.getField('Id'))
            }
        }

        FileUtils.createDir(loadedDirectory)

        if (filePart.contains('contentVersionSecondary')) {
            docIdMap = getSourceDocIdMap(null)
        }


        mappingIterator = CSVUtils.getCsvFileReader("${filePartDirectory}/${filePart}.csv".toString())

        while (mappingIterator.hasNext()) {
            def record = mappingIterator.next()
            def filename = "${fileDirectory}/${record.Id}".toString()

            if (FileUtils.doesExist(filename)) {

                def row

                if (filePart.contains('contentVersion')) {
                    row = [
                            ContentDocumentId: docIdMap.get(record.ContentDocumentId)
                            , ReasonForChange  : record.ReasonForChange
                            , PathOnClient     : record.PathOnClient
                            , CreatedDate      : record.CreatedDate
                            , LastModifiedDate : record.LastModifiedDate
                            , ContentUrl       : record.ContentUrl
                            , IsMajorVersion   : record.IsMajorVersion
                            , Origin           : record.Origin
                            , Title            : record.Title
                            , Description      : record.Description
                            , TagCsv           : record.TagCsv
                            , SharingOption    : record.SharingOption
                            , SharingPrivacy   : record.SharingPrivacy
                    ]

                    row."${configMap.ContentVersion.externalId}" = (docIdMap.get(record.ContentDocumentId) ? record.Id : record.ContentDocumentId)

                    if (record.'RecordType.Name') {
                        row.put('RecordTypeId', recordTypeMap.get(record.'RecordType.Name'))
                    }

                }
                else if (filePart.contains('attachment')) {
                    row = [
                            PathOnClient    : record.Name
                            , CreatedDate     : record.CreatedDate
                            , LastModifiedDate: record.LastModifiedDate
                            , Title           : record.Name
                            , Description     : record.Description
                            , SharingOption   : 'A'
                            , SharingPrivacy  : 'N'
                    ]

                    row."${configMap.ContentVersion.externalId}" = record.Id

                }

                if (configMap.ContentVersion.mapCreatedBy
                        && userMap.get(record.CreatedById)) {
                    row.CreatedById = userMap.get(record.CreatedById)
                    row.OwnerId = userMap.get(record.CreatedById)
                }

                try {
                    def resultMap = SObjectAPI.saveBlobRecord(toUsername, 'ContentVersion', row, 'VersionData', filename, record.Title)
                    println resultMap
                    FileUtils.moveTo(filename, loadedDirectory)
                }
                catch (Exception e) {
                    println "Error saving file ${record.Id}"
                    println e
                    if (e.getMessage().contains('Session expired or invalid')) {
                        uploadFiles(filePart)
                    }
                    else if (e.getMessage().contains('ContentPublication Limit exceeded')) {
                        sleep(3600000)
                        uploadFiles(filePart)
                    }
                }
            }
        }

        updateStateMap()
    }



    static retrieveFiles(filePart) {


        Connection.addConnection(configMap.fromOrg.username, configMap.fromOrg.password, configMap.fromOrg.url)
        fromUsername = configMap.fromOrg.username
        def filePartDirectory = "${workingDirectory}/files/${filePart}".toString()
        def fileDirectory = "${filePartDirectory}/files".toString()

        println "Downloading Files..."
        FileUtils.createDir(fileDirectory)

        println "Downloading Attachment Files..."
        def mappingIterator = CSVUtils.getCsvFileReader("${filePartDirectory}/${filePart}.csv".toString())
        while (mappingIterator.hasNext()) {
            def record = mappingIterator.next()

            if (! FileUtils.doesExist("${fileDirectory}/${record.get('Id')}")) {

                try {
                    if (filePart.contains('attachment')) {
                        SObjectAPI.getBlob(fromUsername, 'Attachment', record.get('Id'), 'Body', fileDirectory)
                    }
                    else {
                        SObjectAPI.getBlob(fromUsername, 'ContentVersion', record.get('Id'), 'VersionData', fileDirectory)
                    }
                }
                catch (Exception e) {
                    println "Error retrieving file ${record.get('Id')}"
                    println e
                    if (e.getMessage().contains('Session expired or invalid')) {
                        retrieveFiles(filePart)
                    }
                }
            }
        }

        stateMap."${filePart}FileRetrievalComplete" = true
        updateStateMap()
    }

    static updateStateMap() {
        JSONUtils.saveMapToFile(stateMapFilename, stateMap)
    }

    static initiate() {
        stateMapFilename = "${configDirectory}/state.json".toString()
        configMap = JSONUtils.parseJSONFile("${configDirectory}/config.json")
        workingDirectory = FileUtils.createDir("${configDirectory}/runningData".toString())
        stateMap = JSONUtils.parseJSONFile(stateMapFilename)
    }



}
