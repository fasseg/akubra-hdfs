package de.fiz.akubra.hdfs.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fiz.akubra.hdfs.HDFSBlobStore;
import de.fiz.akubra.hdfs.HDFSBlobStoreConnection;

public class HDFSBlobStoreTest {

	private static URI storeUri;
	private FileSystem mockFs;
	
	@BeforeClass
	public static void setup() throws Exception{
		storeUri=new URI("hdfs://localhost:9000/");
	}
	
	
	@Test
	public void testHDFSBlobStoreString() throws Exception{
		HDFSBlobStore store=new HDFSBlobStore(storeUri.toASCIIString());
		assertNotNull(store);
	}

	@Test
	public void testGetId()throws Exception {
		HDFSBlobStore store=new HDFSBlobStore(storeUri.toASCIIString());
		assertNotNull(store);
		assertEquals(storeUri, store.getId());
	}

	@Test
	public void testOpenConnection() throws Exception{
		HDFSBlobStore store=new HDFSBlobStore(storeUri.toASCIIString());
		assertNotNull(store.openConnection(null, null));
		assertFalse(store.openConnection(null, null).isClosed());
	}

	@Test
	public void testGetFileSystem() throws Exception{
		HDFSBlobStore store=new HDFSBlobStore(storeUri.toASCIIString());
		store.setFileSystem(mockFs);
		assertNotNull(store.getFileSystem());
	}
}
