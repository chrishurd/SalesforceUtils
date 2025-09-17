package com.boltdata.library.utils

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.dataformat.csv.CsvSchema

import java.text.SimpleDateFormat

class CSVUtils {


    static getCsvWriter(dataRows, columns) {
        return getCsvWriter(dataRows, columns, true)
    }

    static getCsvWriterFromArrayMap(dataRows) {
        return getCsvWriter(dataRows, dataRows.get(0).keySet().toList(), true)
    }

    static writeCsvWriterFromArrayMap(filename, dataRows) {
        new File(filename).write(getCsvWriterFromArrayMap(dataRows), 'UTF-8')
    }

    static getCsvWriter(dataRows, columns, useHeader) {
        def builder = CsvSchema.builder()
        columns.each { c ->
            builder.addColumn(c)
        }
        builder.setUseHeader(useHeader)

        CsvSchema schema = builder.build()
        return new CsvMapper().writer(schema).writeValueAsString(dataRows)
    }

    static getCsvWriter(dataRows) {
        CsvMapper csvMapper = new CsvMapper()
        return csvMapper.writeValueAsString(dataRows)
    }

    static getCsvReader(csvDataString) {
        def mapper = new CsvMapper()
        def mappingIterator = mapper.readerFor(Map.class).with(CsvSchema.emptySchema().withHeader()).readValues(csvDataString)
        return mappingIterator.readAll()
    }

    static getCsvFileReader(dataFileName) {
        def mapper = new CsvMapper()
        return mapper.readerFor(Map.class).with(CsvSchema.emptySchema().withHeader()).readValues(new File(dataFileName))
    }

    static dropCSVFields(filename, tempDir, dropFields) {
        def newFilename = "${tempDir}/dropFields_${Utils.getCurrentTimestamp()}.csv".toString()
        def dataRows = []

        def mappingIterator = getCsvFileReader(filename)

        while (mappingIterator.hasNext()) {
            def rowMap = mappingIterator.next()

            dropFields.each { field ->
                rowMap.remove(field)
            }

            dataRows.add(rowMap)
        }
        new File(newFilename).write(getCsvWriterFromArrayMap(dataRows), 'UTF-8')
        return newFilename

    }

    static splitCSVFields(filename, tempDir, splitFields, additionalFields) {

        def newSplitFilename = "${tempDir}/SplitFieldFiles_${Utils.getCurrentTimestamp()}.csv".toString()
        def updateFilename = "${tempDir}/UpdatedFile_${Utils.getCurrentTimestamp()}.csv".toString()
        def fileMap = [:]

        def dataRows = []
        def splitFileRows = []

        def mappingIterator = getCsvFileReader(filename)

        while (mappingIterator.hasNext()) {
            def rowMap = mappingIterator.next()
            def newRowMap = [:]
            def hasSplitFieldValue = false

            additionalFields.each { field ->
                newRowMap.put(field, rowMap.get(field))
            }

            splitFields.each { field ->
                if (rowMap.get(field)) {
                    hasSplitFieldValue = true
                }
                newRowMap.put(field, rowMap.get(field))
                rowMap.remove(field)
            }

            if (hasSplitFieldValue) {
                splitFileRows.add(newRowMap)
            }
            dataRows.add(rowMap)
        }

        if (! splitFileRows.isEmpty()) {
            new File(newSplitFilename).write(getCsvWriterFromArrayMap(splitFileRows), 'UTF-8')
            fileMap.newSplitFilename = newSplitFilename
        }
        new File(updateFilename).write(getCsvWriterFromArrayMap(dataRows), 'UTF-8')
        fileMap.updatedFilename = updateFilename
        return fileMap
    }


    static combineFiles(files, namePrefix, tempDir)
    {
        if (files.size() > 1) {
            def dataRows = []
            files.each { filename ->
                dataRows.addAll(getCsvFileReader(filename).readAll())
            }

            def newFilename = "${tempDir}/${namePrefix}_${new SimpleDateFormat('yyyyMMddHHmmss').format(new Date())}.csv".toString()

            if (!dataRows.isEmpty()) {
                new File(newFilename).write(getCsvWriterFromArrayMap(dataRows), 'UTF-8')
            }


            files.each { filename ->
                new File(filename).delete()
            }

            return newFilename
        }
        else {
            return files.get(0)
        }


    }


}
