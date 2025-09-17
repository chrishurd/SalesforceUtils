{
    "fromOrg" : {
      "username": "servicemax.batch@servicemax.com.pkiprod",
      "password": "0li8naYpCiH0",
      "url" : "login.salesforce.com"
    },
    "toOrg" : {
		"username": "sfs.mgr@revvity.com",
		"password": "ztd5faq7kzx_VHT4mny",
        "url" : "login.salesforce.com"
    },
    "objects" : {
    	"workOrder" : {
    		"sourceObject" : "SVMXC__Service_Order__c",
			"destinationObject" : "WorkOrder",
			"whereClause" : "SMAX_PS_Owned_By_Company__c = 'LSDX'",
			"externalId" : "Legacy_SF_ID__c"
    	}
    },
	"ContentVersion" : {
		"excludedFields" : ["VersionData", "CreatedById", "LastModifiedById", "OwnerId", "FirstPublishLocationId","RecordTypeId"],
		"includeFields" : ["VersionNumber", "FileType"],
		"externalId" : "Legacy_SF_ID__c"
	},
	"Attachment" : {
		"excludedFields" : ["Body", "CreatedById", "LastModifiedById", "OwnerId"],
		"externalId" : "Legacy_SF_ID__c",
		"ShareType" : "V",
		"Visibility" : "InternalUsers"
	},
	"batchSize" : 50000

}