package de.fiz.akubra.hdfs;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.akubraproject.BlobStore;
import org.akubraproject.map.IdMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSIdMapper implements IdMapper {
    private static final Logger log = LoggerFactory.getLogger(HDFSIdMapper.class);
    private final String internal;

    public HDFSIdMapper(final BlobStore store) {
        this.internal=store.getId().toASCIIString();
    }

    @Override
    public URI getExternalId(URI internalId) throws NullPointerException {
        String idString = internalId.toASCIIString();
        int colonPos = idString.indexOf(':');
        URI externalId = URI.create("hdfs:" + decode(idString.substring(colonPos+1)));
        log.debug("mapping internal id " + internalId + " to " + externalId);
        return externalId;
    }

    private String encode(final String s) {
        try {
            String tmp=URLEncoder.encode(s,"UTF-8");
            return tmp;
        } catch (UnsupportedEncodingException e) {
            log.error("unable to encode " + s,e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public URI getInternalId(URI externalId) throws NullPointerException {
        String idString = externalId.toASCIIString();
        int colonPos=idString.indexOf(':');
        URI internalUri = URI.create(internal + encode(idString.substring(colonPos+1)));
        log.debug("mapping external id " + externalId + " to " + internalUri.toASCIIString());
        return internalUri;
    }

    private String decode(String s) {
        try {
            String tmp=URLDecoder.decode(s,"UTF-8");
            return tmp;
        } catch (UnsupportedEncodingException e) {
            log.error("unable to encode " + s,e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getInternalPrefix(String externalPrefix) throws NullPointerException {
        if (externalPrefix == null) {
            throw new NullPointerException();
        }
        return internal + externalPrefix;
    }

}
