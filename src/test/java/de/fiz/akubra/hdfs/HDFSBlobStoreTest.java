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

import static org.easymock.EasyMock.createMock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.lang.reflect.Field;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.fiz.akubra.hdfs.HDFSBlobStore;

public class HDFSBlobStoreTest {

	private static URI storeUri;
	private FileSystem mockFs;
	private HDFSBlobStore store;

	@BeforeClass
	public static void setup() throws Exception {
		storeUri = new URI("hdfs://example.com:9000");
	}

	@Before
	public void init() throws Exception {
		mockFs = createMock(FileSystem.class);
		store = new HDFSBlobStore(storeUri.toASCIIString());
		Field f = HDFSBlobStore.class.getDeclaredField("fileSystem");
		f.setAccessible(true);
		f.set(store, mockFs);
	}

	@Test
	public void testHDFSBlobStoreString() throws Exception {
		assertNotNull(store);
	}

	@Test
	public void testGetId() throws Exception {
		HDFSBlobStore store = new HDFSBlobStore(storeUri.toASCIIString());
		assertNotNull(store);
		assertEquals(storeUri, store.getId());
	}

	@Test
	public void testOpenConnection() throws Exception {
		HDFSBlobStore store = new HDFSBlobStore(storeUri.toASCIIString());
		assertNotNull(store.openConnection(null, null));
		assertFalse(store.openConnection(null, null).isClosed());
	}

}
