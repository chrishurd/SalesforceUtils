package com.boltdata.library.salesforce

class SFConstants {
    static NO_DATA_FILE = 'NO DATA'
    static UPSERT = 'upsert'
    static INSERT = 'insert'
    static auditFields = ['CreatedDate', 'CreatedById', 'LastModifiedById', 'LastModifiedDate'] as Set<String>
    static errorCSVFields = ['sf__Id', 'sf__Error'] as Set<String>
}
