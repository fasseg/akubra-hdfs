/*
   Copyright 2011 FIZ Karlsruhe 

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package de.fiz.akubra.hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.akubraproject.Blob;
import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.UnsupportedIdException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link BlobStoreConnection} that represents a connection to
 * a Hadoop filesystem
 * 
 * @author frank asseg
 * 
 */
class HDFSBlobStoreConnection implements BlobStoreConnection {

	private final HDFSBlobStore store;
	private boolean closed = false;
	private static final Logger log = LoggerFactory.getLogger(HDFSBlobStoreConnection.class);

	/**
	 * create a new {@link HDFSBlobStoreConnection} to specified HDFs namenode
	 * 
	 * @param store
	 *            the {@link HDFSBlobStore} this {@link HDFSBlobStoreConnection}
	 *            will try to connect to
	 * @throws IOException
	 *             if the connection did not succeed
	 */
	public HDFSBlobStoreConnection(final HDFSBlobStore store) throws IOException {
		this.store = store;
	}

	/**
	 * close this connection
	 */
	public void close() {
		this.closed = true;
	}

	/**
	 * fetch a {@link HDFSBlob} from the {@link HDFSBlobStore}
	 * 
	 * @param uri
	 *            the {@link URI} of the {@link HDFSBlob}
	 * @param hints
	 *            not used
	 * @throws UnsupportedIdException
	 *             if the supplied {@link URI} was not valid
	 */
	public Blob getBlob(final URI uri, final Map<String, String> hints) throws UnsupportedIdException,IOException {
		if (uri == null) {
			URI tmp=URI.create("file:" + UUID.randomUUID().toString());
			log.debug("creating new Blob uri " + tmp.toASCIIString());
			//return getBlob(new ByteArrayInputStream(new byte[0]),0, null);
			return new HDFSBlob(tmp, this);
		}
		log.debug("fetching blob " + uri.toASCIIString());
		if (!uri.toASCIIString().startsWith("file:")) {
			throw new UnsupportedIdException(uri, "URIs have to start with 'file:'");
		}
		HDFSBlob blob = new HDFSBlob(uri, this);
		return blob;
	}

	/**
	 * create a new {@link HDFSBlob} in the {@link HDFSBlobStore}
	 * 
	 * @param in
	 *            the {@link InputStream} pointing to the new {@link HDFSBlob}'s
	 *            data
	 * @param estimatedSize
	 *            not used
	 * @param hints
	 *            not used
	 * @throws IOException
	 *             if the operation did not succeed
	 */
	public Blob getBlob(final InputStream in, final long estimatedSize, final Map<String, String> hints) throws IOException {
		if (in == null) {
			throw new NullPointerException("inputstream can not be null");
		}
		HDFSBlob blob;
		OutputStream out = null;
		try {
			blob = new HDFSBlob(URI.create("file:" + UUID.randomUUID().toString()), this);
			log.debug("creating file with uri "+ blob.getId().toASCIIString());
			out = blob.openOutputStream(estimatedSize, false);
			IOUtils.copy(in, out);
			return blob;
		} finally {
			IOUtils.closeQuietly(in);
			IOUtils.closeQuietly(out);
		}
	}

	/**
	 * get the associated {@link HDFSBlobStore}
	 * 
	 * @return the {@link HDFSBlobStore} used
	 */
	public BlobStore getBlobStore() {
		return store;
	}

	/**
	 * check the connection state
	 * 
	 * @return true if the connection is open
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * create a new {@link HDFSIdIterator} over all the {@link HDFSBlob}s in the
	 * {@link HDFSBlobStore}
	 * 
	 * @return an {@link Iterator} for the collection of filesystem entries
	 * @throws IOException
	 *             if the operation did not succeed
	 */
	public Iterator<URI> listBlobIds(final String filterPrefix) throws IOException {
		if (filterPrefix==null || filterPrefix.length() == 0){
			// complete filesystem scan
			return new HDFSIdIterator(getFiles(new Path(this.store.getId().toASCIIString() + "/"), new ArrayList<FileStatus>(), true));
		}
		int delim = filterPrefix.lastIndexOf('/');
		List<FileStatus> files=new ArrayList<FileStatus>();
		// check all the files in the path vs. the filter
		Path path=new Path(this.store.getId().toASCIIString() + "/" + (delim > -1 ? filterPrefix.substring(0, delim) : ""));
		List<FileStatus> tmpFiles = getFiles(path, new ArrayList<FileStatus>(), false);
		for (FileStatus f : tmpFiles) {
			log.debug("checking name to add to filter " + f.getPath().getName());	
			if (f.getPath().getName().startsWith(filterPrefix)){
				files.add(f);
			}
		}
		return new HDFSIdIterator(files);
	}

	/*
	 * Utility method for recursively fetching the directory contents in the
	 * hadoop filesystem. Calls itself on the subdirectories
	 */
	private List<FileStatus> getFiles(final Path p, List<FileStatus> target, boolean recursive) throws IOException {
		for (FileStatus f : getFileSystem().listStatus(p)) {
			if (f.isFile()) {
				target.add(f);
			}
			if (f.isDirectory() && recursive) {
				getFiles(f.getPath(), target, recursive);
			}
		}
		return target;
	}

	/**
	 * This is not implemented. Throws an exception when used!
	 * 
	 * @throws UnsupportedOperationException
	 *             always!
	 */
	public void sync() throws UnsupportedOperationException {
		throw new UnsupportedOperationException("not yet implemented");
	}

	/**
	 * get the underlying FileSystem.
	 * 
	 * @return the Hadoop API {@link FileSystem} object used for interaction
	 */
	public FileSystem getFileSystem() throws IOException {
		return store.getFileSystem();
	}

}
