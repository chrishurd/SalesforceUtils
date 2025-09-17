package com.boltdata.library.utils

import com.boltdata.library.salesforce.ToolingAPI
import com.sforce.soap.metadata.CustomField

class FieldUtils {

    static recordTypeFields = ['RecordTypeId', 'RecordType.DeveloperName'] as Set<String>
    static auditFields = ['OwnerId', 'CreatedDate', 'CreatedById', 'LastModifiedDate', 'LastModifiedById', 'RecordTypeId'] as Set<String>


    static getRelationshipField(String field) {
        if (field.contains('__c')) {
            return field.replaceAll('__c', '__r')
        }
        else if (field.contains('Id')) {
            return field.replaceAll('Id', '')
        }
        else {
            return field
        }
    }

    static getMetadataFields(username, sObj, excludeFields) {
        def objectMetadata = ToolingAPI.executeQuery(username, getMetadataQuery(sObj))
        def results = [
                fields : ['Id'],
                fieldDefs : [:]
        ]
        objectMetadata.each { fieldMetadata ->
            if (! excludeFields.contains(fieldMetadata.QualifiedApiName)) {
                if ((!fieldMetadata.IsCompound
                        && fieldMetadata.IsCreatable
                        && !fieldMetadata.IsCalculated)
                        || (auditFields.contains(fieldMetadata.QualifiedApiName))) {
                    results.fields.add(fieldMetadata.QualifiedApiName)
                }
                results.fieldDefs.put(fieldMetadata.QualifiedApiName, getFieldMap(fieldMetadata))
            }
        }

        return results
    }

    static getFieldMap(fieldMetadata) {
        def fieldMap = [:]
        fieldMap.put('DataType', fieldMetadata.FieldDefinition.DataType)
        fieldMap.put('QualifiedApiName', fieldMetadata.QualifiedApiName)

        if (fieldMetadata.ReferenceTo.referenceTo) {
            fieldMap.put('ReferenceTo', fieldMetadata.ReferenceTo.referenceTo[0])
        }

        return fieldMap
    }

    static getMetadataQuery(sObj) {
        return "SELECT  DataType, EntityDefinition.DeveloperName, IsCompound, DeveloperName, " +
                "QualifiedApiName, FieldDefinition.QualifiedApiName, FieldDefinition.DataType, IsCalculated, IsComponent, IsCreatable, " +
                "IsUpdatable,  ReferenceTo, ValueType.DeveloperName, IsNameField FROM EntityParticle " +
                "WHERE EntityDefinition.QualifiedApiName ='${sObj}' AND IsCompound = false AND QualifiedApiName != 'Id'".toString()
    }
}
