package com.boltdata.library.utils

import com.boltdata.library.salesforce.Bulk2API

class SFObjectUtils {

    static queryRecordIds(username, objectName, whereClause, destinationDirectory) {
        def query = "SELECT Id FROM ${objectName} "
        if (whereClause) {
            query += " WHERE ${whereClause} "
        }

        return Bulk2API.executeQuery(username, query.toString(), destinationDirectory)
    }

    static queryObject(username, objectName, whereClause, orderBy, excludeFields, destinationDirectory) {
        return queryObject(username, objectName, whereClause, orderBy, excludeFields, null, destinationDirectory)
    }

    static queryObject(username, objectName, whereClause, orderBy, excludeFields, includeFields, destinationDirectory) {
        def objectMetadata = FieldUtils.getMetadataFields(username, objectName, excludeFields)
        def queryFiles = []

        if (includeFields) {
            objectMetadata.fields.addAll(includeFields)
        }

        def query = "SELECT ${objectMetadata.fields.join(',')} FROM ${objectName} "

        if (whereClause) {
            query += " WHERE ${whereClause} "
        }

        if (orderBy) {
            query += " ORDER BY ${orderBy} "
        }

        queryFiles.add(Bulk2API.executeQuery(username, query.toString(), destinationDirectory))


        return CSVUtils.combineFiles(queryFiles, objectName, destinationDirectory)

    }
}
