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
    private final String storeId;

    public HDFSIdMapper(final BlobStore store) {
        this.storeId=store.getId().toASCIIString();
    }

    @Override
    public URI getExternalId(URI internalId) throws NullPointerException {
        String path=internalId.toASCIIString();
        if (path.startsWith(storeId)){
            path=path.substring(storeId.length());
        }
        int slash=path.lastIndexOf('/');
        if (slash!=-1){
            path=path.substring(0,slash) + decode(path.substring(slash));
        }else{
            path=decode(path);
        }
        URI externalId = URI.create("hdfs:" + path);
        log.debug("mapping internal id " + internalId + " to " + externalId);
        return externalId;
    }

    @Override
    public URI getInternalId(URI externalId) throws NullPointerException {
        String path = externalId.toASCIIString();
        log.debug("external path " + path);
        int colon=path.indexOf(':');
        int slash=path.lastIndexOf('/');
        String fileName=path.substring(slash+1);
        path=path.substring(colon + 1,slash+1).replaceAll(":", "_");
        URI internalUri;
        if ("new".equals(fileName)){
            internalUri = URI.create(storeId + path.substring(0,path.length() - 1) + "_NEW_");
        }else if("old".equals(fileName)){
            internalUri = URI.create(storeId + path.substring(0,path.length() - 1) + "_OLD_");
        }else{
            internalUri = URI.create(storeId + path + encode(fileName));
        }
        log.debug("mapping external id " + externalId + " to " + internalUri.toASCIIString());
        return internalUri;
    }

    private String encode(final String s) {
        try {
            return URLEncoder.encode(s.replaceAll(":", "_"),"UTF-8").replaceAll("\\+", "%20");
        } catch (UnsupportedEncodingException e) {
            log.error("unable to encode " + s,e);
            throw new RuntimeException(e);
        }
    }

    private String decode(String s) {
        try {
            String tmp=URLDecoder.decode(s.replaceAll(":","_"),"UTF-8");
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
        return storeId + externalPrefix;
    }

}
