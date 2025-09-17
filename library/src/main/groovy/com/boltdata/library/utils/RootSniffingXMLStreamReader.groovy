package com.boltdata.library.utils

import javax.xml.stream.XMLStreamException
import javax.xml.stream.XMLStreamReader
import javax.xml.stream.util.StreamReaderDelegate

class RootSniffingXMLStreamReader extends StreamReaderDelegate {
    private String _rootElementLocalName;
    public RootSniffingXMLStreamReader (XMLStreamReader streamReader) {
        super(streamReader);
    }

    @Override
    public int next() throws XMLStreamException {
        int v = super.next();
        if (_rootElementLocalName == null) {
            _rootElementLocalName = super.getLocalName();
        }
        return v;
    }

    public String getLocalNameForRootElement() {
        return _rootElementLocalName;
    }


}
