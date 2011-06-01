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
import java.net.URISyntaxException;
import java.util.Map;

import org.akubraproject.Blob;
import org.akubraproject.BlobStoreConnection;
import org.akubraproject.DuplicateBlobException;
import org.akubraproject.MissingBlobException;
import org.akubraproject.UnsupportedIdException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a {@link Blob} for using the Hadoop filesystem
 * 
 * @author frank asseg
 * 
 */
public class HDFSBlob implements Blob {
	private final Path path;
	private final URI uri;
	private FileSystem hdfs;
	private HDFSBlobStoreConnection conn;
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
	public HDFSBlob(final URI uri, final HDFSBlobStoreConnection conn) throws UnsupportedIdException {
		if (uri.getScheme() == null) {
			// seems the supplied uri is invalid and doesn't include a scheme
			// like "http" or "file"
			throw new UnsupportedIdException(uri);
		}
		this.conn = conn;
		try {
			if (uri.getPath() == null && uri.toString().startsWith("file:")) {
				// from:
				// https://groups.google.com/group/akubra-dev/browse_thread/thread/0c4127b3f69073ac
				// (a) "file:relative/path"
				// This syntax is rfc3986-compliant, but violates rfc1738. Of course,
				// there's a great tradition of violating rfc1738. In this case, the
				// implication is that generic URI parsers will have no problem, but
				// those that attempt to validate file: URIs with scheme-specific
				// syntax rules may barf.
				this.uri = new URI(conn.getBlobStore().getId() + (conn.getBlobStore().getId().toASCIIString().endsWith("/") ? "" : "/")
						+ uri.getRawSchemeSpecificPart());
			} else {
				// concatenate the path to the blob with the store id which
				// should be something
				// like "hdfs://example.com:9000/"
				this.uri = uri;
			}
			this.path = new Path(this.uri.toASCIIString());
		} catch (URISyntaxException e) {
			// hey, thats not an URI!
			throw new UnsupportedIdException(uri, e.getLocalizedMessage());
		}
	}

	/**
	 * delete this {@link HDFSBlob} from the underlying Hadoop filesystem
	 * 
	 * @throws IOException
	 *             if the operation did not succeed
	 */
	public void delete() throws IOException {
		getFileSystem().delete(path, false);
	}

	/**
	 * check if this {@link HDFSBlob} exists in the underlying Hadoop filesystem
	 * 
	 * @throws IOException
	 *             if the operation did not succeed
	 */
	public boolean exists() throws IOException {
		try{
			return this.getFileSystem().exists(path);	
		}catch(IOException e){
			// try reconnect
			log.debug(e.getLocalizedMessage() + " has been thrown trying to reconnect...",e);
			this.conn=(HDFSBlobStoreConnection) this.getConnection().getBlobStore().openConnection(null, null);
			this.hdfs=this.conn.getFileSystem();
			return this.getFileSystem().exists(path);
		}
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
		log.debug("checking size of " + this.getId().toASCIIString());
		if (!this.exists()) {
			throw new MissingBlobException(uri);
		}
		return getFileSystem().getFileStatus(path).getLen();
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
	public Blob moveTo(final URI toUri, final Map<String, String> hints) throws DuplicateBlobException, IOException, MissingBlobException {
		log.debug("moving blob " + this.getId().toASCIIString() + " to " + toUri.toASCIIString());
		if (!this.exists()) {
			throw new MissingBlobException(uri);
		}
		// it seems necessary to create a new file on the hdfs
		// and copy this blob's content to the new file
		// since there is no support for move in the hdfs api
		HDFSBlob newBlob = (HDFSBlob) this.getConnection().getBlob(toUri, null);
		if (newBlob.exists()) {
			throw new DuplicateBlobException(toUri);
		}
		// copy the contents of this blob into the newly created blob
		InputStream in = this.openInputStream();
		OutputStream out = newBlob.openOutputStream(this.getSize(), false);
		IOUtils.copy(in, out);
		in.close();
		out.close();
		this.delete();
		return newBlob;
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
		if (this.exists()) {
			return getFileSystem().open(path);
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
		if (this.exists()) {
			if (overWrite) {
				// return a stream that will
				// overwrite this blobs content
				return getFileSystem().create(path, true);
			} else {
				throw new DuplicateBlobException(uri);
			}
		} else {
			// create a new file for this blob's
			// data on the hdfs
			return getFileSystem().create(path);
		}
	}

	private FileSystem getFileSystem() throws IOException {
		return this.conn.getFileSystem();
	}
}
