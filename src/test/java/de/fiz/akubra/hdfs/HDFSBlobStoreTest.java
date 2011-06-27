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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.net.URI;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HDFSBlobStoreTest {

    private static URI storeUri;
    private HDFSBlobStore store;

    @BeforeClass
    public static void setup() throws Exception {
        storeUri = new URI("hdfs://example.com:9000");
    }

    @Before
    public void init() throws Exception {
        store = new HDFSBlobStore(storeUri);
    }

    @Test
    public void testHDFSBlobStoreString() throws Exception {
        assertNotNull(store);
    }

    @Test
    public void testGetId() throws Exception {
        assertNotNull(store);
        assertEquals(storeUri, store.getId());
    }

    @Test
    public void testOpenConnection() throws Exception {
        assertNotNull(store.openConnection(null, null));
        assertFalse(store.openConnection(null, null).isClosed());
    }

}
