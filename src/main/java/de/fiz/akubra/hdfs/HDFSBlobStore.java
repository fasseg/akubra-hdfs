package de.fiz.akubra.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;

import org.akubraproject.BlobStore;
import org.akubraproject.BlobStoreConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link BlobStore} implementation for the Hadoop filesystem.
 * 
 * @author frank asseg
 * 
 */
public class HDFSBlobStore implements BlobStore {
	private static final Logger log = LoggerFactory.getLogger(HDFSBlobStore.class);

	private final URI id;
	private FileSystem fileSystem;

	/**
	 * create a new {@link HDFSBlobStore} at a specific URI in {@link String}
	 * format
	 * 
	 * @param id
	 *            the {@link URI} pointing to the HDFS namenode
	 * @throws URISyntaxException
	 *             if the supplied {@link URI} was not valid
	 */
	public HDFSBlobStore(final String uri) throws URISyntaxException {
		this.id = new URI(uri);
	}

	/**
	 * get the id
	 * 
	 * @return this {@link HDFSBlobStore}'s id
	 */
	public URI getId() {
		return id;
	}

	/**
	 * open a new {@link HDFSBlobStoreConnection} to a HDFS namenode
	 * 
	 * @param tx
	 *            since transactions are not supported. this must be set to null
	 * @param hints
	 *            not used
	 * @return a new {@link HDFSBlobStoreConnection} th this
	 *         {@link HDFSBlobStore}'s id
	 * @throws UnsupportedOperationException
	 *             if the transaction parameter was not null
	 * @throws IOException
	 *             if the operation did not succeed
	 */
	public BlobStoreConnection openConnection(final Transaction tx, final Map<String, String> hints) throws UnsupportedOperationException, IOException {
		return new HDFSBlobStoreConnection(this);
	}

	public FileSystem getFileSystem() throws IOException {
		if (this.fileSystem == null) {
			setFileSystem(FileSystem.get(this.getId(), new Configuration()));
		}
		return this.fileSystem;
	}

	public void setFileSystem(FileSystem fileSystem) {
		this.fileSystem = fileSystem;
	}
}
