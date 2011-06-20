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
import java.util.Map;
import java.util.UUID;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.DuplicateBlobException;
import org.akubraproject.MissingBlobException;
import org.akubraproject.UnsupportedIdException;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a {@link Blob} for using the Hadoop filesystem
 * 
 * @author frank asseg
 * 
 */
class HDFSBlob implements Blob {
	private final HDFSBlobStoreConnection conn;
	private final URI storeId;
	private Path path;
	private URI uri;
	private static final Logger log = LoggerFactory.getLogger(HDFSBlob.class);

	/**
	 * creates a new {@link HDFSBlob} using the supplied uri as an identifier
	 * for the underlying {@link HDFSBlobStoreConnection}
	 * 
	 * @param uri
	 *            the identifier of the {@link HDFSBlob}
	 * @param conn
	 *            the {@link HDFSBlobStoreConnection} that should be used to
	 *            manipulate this {@link HDFSBlob}
	 * @throws UnsupportedIdException
	 */
	public HDFSBlob(final URI uri, final HDFSBlobStoreConnection conn) {
		this.conn = conn;
		this.storeId = this.conn.getBlobStore().getId();
		this.uri = uri;
		this.path = new Path(this.uri);
		log.debug("opening blob " + uri.toASCIIString() + " at " + this.path.toString());
	}

	/**
	 * delete this {@link HDFSBlob} from the underlying Hadoop filesystem
	 * 
	 * @throws IOException
	 *             if the operation did not succeed
	 */
	public void delete() throws IOException {
		if (this.conn.isClosed()) {
			throw new IllegalStateException("Unable to open Inputstream, because connection is closed");
		}
		this.conn.getFileSystem().delete(path, false);
	}

	/**
	 * check if this {@link HDFSBlob} exists in the underlying Hadoop filesystem
	 * 
	 * @throws IOException
	 *             if the operation did not succeed
	 */
	public boolean exists() throws IOException {
		if (this.conn.isClosed()) {
			throw new IllegalStateException("Unable to open Inputstream, because connection is closed");
		}
		return this.conn.getFileSystem().exists(path);
	}

	/**
	 * get the canonical id
	 * 
	 * @return a {@link URI} with the {@link HDFSBlob}'s id
	 */
	public URI getCanonicalId() {
		return uri;
	}

	/**
	 * get the {@link HDFSBlobStoreConnection} which is used to manipulate this
	 * {@link HDFSBlob}
	 * 
	 * @return the underlying {@link HDFSBlobStoreConnection}
	 */
	public BlobStoreConnection getConnection() {
		return conn;
	}

	/**
	 * get the id
	 * 
	 * @return a {@link URI} with this {@link HDFSBlob}'s id
	 */
	public URI getId() {
		return uri;
	}

	/**
	 * get the size of the {@link HDFSBlob}
	 * 
	 * @return the size of the {@link HDFSBlob}
	 * @throws IOException
	 *             if the operation did not succeed
	 * @throws MissingBlobException
	 *             if this {@link HDFSBlob} does not exist
	 */
	public long getSize() throws IOException, MissingBlobException {
		if (this.conn.isClosed()) {
			throw new IllegalStateException("Unable to open Inputstream, because connection is closed");
		}
		if (!this.exists()) {
			throw new MissingBlobException(uri);
		}
		return this.conn.getFileSystem().getFileStatus(path).getLen();
	}

	/**
	 * move a {@link HDFSBlob} to another location on the Hadoop filesystem
	 * 
	 * @param toUri
	 *            the {@link URI} of the new location where this
	 *            {@link HDFSBlob} should be moved to
	 * @param hints
	 *            hints are currently ignored
	 * @throws DuplicateBlobException
	 *             if another file exists with the same {@link URI}
	 * @throws IOException
	 *             if the move did not succeed on the underlying filesystem
	 * @throws MissingBlobException
	 *             if this {@link HDFSBlob} does not exist
	 */
	public Blob moveTo(URI toUri, final Map<String, String> hints) throws DuplicateBlobException, IOException, MissingBlobException {
		if (toUri==null){
			log.debug("creating new random URI " + toUri);
			toUri=URI.create(this.getConnection().getBlobStore().getId() + UUID.randomUUID().toString());
		}
		if (!toUri.toASCIIString().startsWith("hdfs://")){
			log.error("invalid scheme: " + toUri.getRawSchemeSpecificPart());
			throw new UnsupportedIdException(toUri);
		}
		log.debug("moving " + this.getId() + " to " + toUri);
		if (this.conn.isClosed()) {
			throw new IllegalStateException("Unable to open Inputstream, because connection is closed");
		}
		if (!this.exists()) {
			throw new MissingBlobException(uri);
		}
		if (this.conn.getFileSystem().exists(new Path(toUri))) {
			throw new DuplicateBlobException(toUri);
		}
		String path=toUri.toASCIIString();
		if (path.startsWith(storeId.toASCIIString())){
			path=path.substring(storeId.toASCIIString().length());
		}else if (path.startsWith("hdfs://")){
			path=path.substring(7);
		}
		String[] directories=path.split("/");
		log.debug("" + directories.length);
		path="";
		for (int i=0;i<directories.length-1;i++){
			path+=directories[i] + "/";
			log.debug("checking path " + path);
			if (!this.conn.getFileSystem().exists(new Path(storeId + path))){
				log.debug("creating " + path);
				this.conn.getFileSystem().mkdirs(new Path(storeId + path));
			}
		}
		if (this.conn.getFileSystem().rename(new Path(uri), new Path(toUri))) {
			log.debug("file has been moved succesfully to " + toUri);
			return this.conn.getBlob(toUri, null);
		} else {
			throw new IOException("Unable to rename " + uri + " to " + toUri);
		}
	}

	/**
	 * open a new {@link InputStream} for this {@link HDFSBlob}
	 * 
	 * @throws IOException
	 *             if the operation did not succeed
	 * @throws MissingBlobException
	 *             if this {@link HDFSBlob} does not exist.
	 */
	public InputStream openInputStream() throws IOException, MissingBlobException {
		if (this.conn.isClosed()) {
			throw new IllegalStateException("Unable to open Inputstream, because connection is closed");
		}
		if (this.exists()) {
			return this.conn.getFileSystem().open(path);
		}
		throw new MissingBlobException(uri);
	}

	/**
	 * open a new {@link OutputStream} for writing on the underlying Hadoop
	 * filesystem
	 * 
	 * @param estimatedSize
	 *            currently unused
	 * @param overWrite
	 *            if true existing {@link HDFSBlob}s will be overwritten
	 * @throws IOException
	 *             if the operation did not succeed
	 * @throws DuplicateBlobException
	 *             if overwrite == false and the {@link HDFSBlob} already exist
	 */
	public OutputStream openOutputStream(final long estimatedSize, final boolean overWrite) throws IOException, DuplicateBlobException {
		if (this.conn.isClosed()) {
			throw new IllegalStateException("Unable to open Inputstream, because connection is closed");
		}
		if (this.exists()) {
			if (overWrite) {
				// return a stream that will
				// overwrite this blobs content
				return this.conn.getFileSystem().create(path, true);
			} else {
				throw new DuplicateBlobException(uri);
			}
		} else {
			// create a new file for this blob's
			// data on the hdfs
			return this.conn.getFileSystem().create(path);
		}
	}
}
