package com.boltdata.library.salesforce

class PartnerAPI {

    static querySFDC(username, query) {
        def results = []
        def isDone = false

        def conn = Connection.getConnection(username)

        def qResults = conn.query(query)

        while (!isDone) {
            results.addAll(qResults.getRecords())

            if (!qResults.isDone()) {
                qResults = conn.queryMore(qResults.getQueryLocator())
            } else {
                isDone = true
            }
        }

        return results
    }


}
