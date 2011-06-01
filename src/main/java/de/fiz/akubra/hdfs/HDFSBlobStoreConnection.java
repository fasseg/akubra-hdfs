package de.fiz.akubra.hdfs;

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
import org.apache.hadoop.conf.Configuration;
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
public class HDFSBlobStoreConnection implements BlobStoreConnection {

	private static final Logger log = LoggerFactory.getLogger(HDFSBlobStoreConnection.class);

	private final HDFSBlobStore store;
	private boolean closed=false;

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
		this.closed=true;
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
	public Blob getBlob(final URI uri, final Map<String, String> hints) throws UnsupportedIdException {
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
		HDFSBlob blob;
		try {
			blob = new HDFSBlob(new URI(this.store.getId() + UUID.randomUUID().toString()), this);
			OutputStream out = blob.openOutputStream(estimatedSize, false);
			IOUtils.copy(in, out);
			in.close();
			out.close();
			return blob;
		} catch (URISyntaxException e) {
			throw new IOException(e);
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
		return new HDFSIdIterator(getFiles(new Path(filterPrefix), new ArrayList<FileStatus>()));
	}

	/*
	 * Utility method for recursively fetching the directory contents in the
	 * hadoop filesystem. Calls itself on the subdirectories
	 */
	private List<FileStatus> getFiles(final Path p, List<FileStatus> target) throws IOException {
		for (FileStatus f : getFileSystem().listStatus(p)) {
			if (f.isFile()) {
				target.add(f);
			}
			if (f.isDirectory()) {
				getFiles(f.getPath(), target);
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
