package com.boltdata.library.utils

class FileUtils {

    static splitCsvFile(filename, tempDir) {
        return splitCsvFile(filename, tempDir,50000000)
    }

    static splitCsvFile(filename, tempDir, size) {
        def files = []
        def dataFile = new File(filename)
        def numberOfFiles = Math.ceil(dataFile.size() / size)

        if (numberOfFiles > 1) {
            def tempDirName = "${tempDir}/${UUID.randomUUID() as String}"
            new File(tempDirName).mkdir()

            def dataRows = CSVUtils.getCsvFileReader(filename).readAll()
            def lineCount = Math.ceil(dataRows.size() / numberOfFiles)

            dataRows.collate(lineCount.intValue()).eachWithIndex { tempDataRows, int i ->
                def newFilename = "${tempDirName}/split${i}.csv".toString()
                files.add(newFilename)
                new File(newFilename).write(CSVUtils.getCsvWriterFromArrayMap(tempDataRows), 'UTF-8')
            }
        }
        else {
            files.add(filename)
        }

        return files
    }

    static doesExist(filename) {
        def file = new File(filename)
        return file.exists()
    }

    static moveTo(filename, destDir) {
        def file = new File(filename)
        def destDirFile = new File(destDir)

        if (file.exists()) {
            if (! destDirFile.exists()) {
                destDirFile.mkdirs()
            }

            file.renameTo(new File("${destDir}/${file.name}".toString()))
        }
    }

    static createDir(dir) {
        def dirFile = new File(dir)

        if (! dirFile.exists()) {
            dirFile.mkdirs()
        }

        return dir
    }

    static addToFile(filename, data) {
        def file = new File(filename)
        file.append(data)
    }

    static refreshDir(dir) {
        def dirFile = new File(dir)

        if (dirFile.exists()) {
            dirFile.deleteDir()
        }

        return createDir(dir)
    }

}
