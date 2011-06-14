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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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
	private FileSystem hdfs;
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
		try {
			getFileSystem().close();
		} catch (IOException e) {
			log.error("Exception while closing hdfs connection");
		}
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
	public Blob getBlob(final URI uri, final Map<String, String> hints) throws UnsupportedIdException, IOException {
		if (uri == null) {
			URI tmp = URI.create("hdfs:" + UUID.randomUUID().toString());
			log.debug("creating new Blob uri " + tmp.toASCIIString());
			// return getBlob(new ByteArrayInputStream(new byte[0]),0, null);
			return new HDFSBlob(tmp, this);
		}
		log.debug("fetching blob " + uri.toASCIIString());
		if (!uri.toASCIIString().startsWith("hdfs:")) {
			throw new UnsupportedIdException(uri, "HDFS URIs have to start with 'hdfs:'");
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
			blob = new HDFSBlob(URI.create("hdfs:" + UUID.randomUUID().toString()), this);
			log.debug("creating file with uri " + blob.getId().toASCIIString());
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
		if (filterPrefix == null || filterPrefix.length() == 0) {
			// complete filesystem scan
			return new HDFSIdIterator(getFiles(new Path(this.store.getId().toASCIIString() + "/"), null));
		}
		// check all the files in the path vs. the filter
		return new HDFSIdIterator(getFiles(new Path(this.store.getId().toASCIIString() + "/"), filterPrefix));
	}

	/*
	 * Utility methods for recursively fetching the directory contents in the
	 * hadoop filesystem. Calls itself on the subdirectories
	 */
	private List<URI> getFiles(final Path p, String prefix) throws IOException {
		return getFiles(p, new ArrayList<URI>(), prefix);
	}

	private List<URI> getFiles(Path p, ArrayList<URI> target, String prefix) throws IOException {
		for (FileStatus f : getFileSystem().listStatus(p)) {
			if (f.isFile() && f.getPath().getName().startsWith(prefix)) {
				target.add(URI.create("hdfs:" +  f.getPath().getName()));
			}
			if (f.isDirectory()) {
				getFiles(f.getPath(), target, prefix);
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

	FileSystem getFileSystem() throws IOException {
		// lazy init for testability
		if (hdfs == null) {
			hdfs = store.openHDFSConnection();
		}
		return hdfs;
	}
}
