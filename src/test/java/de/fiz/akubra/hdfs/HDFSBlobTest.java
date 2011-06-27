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

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.Random;

import org.akubraproject.DuplicateBlobException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class HDFSBlobTest {
    private HDFSBlobStore mockStore;
    private FileSystem mockFs;
    private HDFSBlobStoreConnection mockConnection;
    private static URI blobUri;
    private static URI blobStoreUri;

    @BeforeClass
    public static void init() throws Exception {
        blobUri = new URI("hdfs:6f/allestest");
        blobStoreUri = new URI("hdfs://localhost:9000/");
    }

    @Before
    public void setup() {
        mockFs = createMock(FileSystem.class);
        mockStore = createMock(HDFSBlobStore.class);
        mockConnection = createMock(HDFSBlobStoreConnection.class);
    }

    @Test
    public void testHDFSBlob() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertNotNull(b);
    }

    @Test
    public void testDelete() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore);
        expect(mockConnection.isClosed()).andReturn(false);
        expect(mockStore.getId()).andReturn(blobStoreUri);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(2);
        expect(mockFs.delete((Path) anyObject(), anyBoolean())).andReturn(true);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        b.delete();
    }

    @Test
    public void testExists() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockConnection.isClosed()).andReturn(false);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs);
        expect(mockFs.exists((Path) anyObject())).andReturn(true);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertTrue(b.exists());
    }

    @Test
    public void testGetCanonicalId() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertEquals(blobUri, b.getCanonicalId());
    }

    @Test
    public void testGetConnection() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertEquals(mockConnection, b.getConnection());
    }

    @Test
    public void testGetId() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertEquals(blobUri, b.getId());
    }

    @Test
    public void testGetSize() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockConnection.isClosed()).andReturn(false).times(2);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(2);
        expect(mockFs.getFileStatus((Path) anyObject())).andReturn(createTestFileStatus());
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertTrue(b.getSize() == 1024);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMoveTo() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore);
        expect(mockStore.getId()).andReturn(blobStoreUri);
        expect(mockConnection.isClosed()).andReturn(false).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(9);
        expect(mockFs.mkdirs(anyObject(Path.class))).andReturn(true).times(1);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(false);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(true);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(true);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(false);
        expect(mockConnection.getBlob(anyObject(URI.class), anyObject(Map.class))).andReturn(null);
        expect(mockFs.rename(anyObject(Path.class), anyObject(Path.class))).andReturn(true);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        URI newURI = URI.create("hdfs://localhost:9000/7f/kjahdsjahd/it-is-a-dir/moveTest");
        b.moveTo(newURI, null);
    }

    @Test(expected = DuplicateBlobException.class)
    public void testMoveToExists() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore);
        expect(mockStore.getId()).andReturn(blobStoreUri);
        expect(mockConnection.isClosed()).andReturn(false).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(9);
        expect(mockFs.mkdirs(anyObject(Path.class))).andReturn(true).times(3);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(true);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        URI newURI = URI.create("hdfs://localhost:9000/7f/kjahdsjahd/it-is-a-dir/moveTest");
        b.moveTo(newURI, null);
    }

    @Test
    public void testOpenInputStream() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockConnection.isClosed()).andReturn(false).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(2);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        byte[] buf = new byte[1024];
        new Random().nextBytes(buf);
        expect(mockFs.open((Path) anyObject())).andReturn(new FSDataInputStream(new SeekableInputStream(buf)));
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertNotNull(b.openInputStream());
    }

    @Test
    public void testOpenOutputStreamExisting() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockConnection.isClosed()).andReturn(false).times(2);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(2);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(true);
        expect(mockFs.create(anyObject(Path.class), anyBoolean())).andReturn(new FSDataOutputStream(new ByteArrayOutputStream(8)));
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertNotNull(b.openOutputStream(0, true));
    }

    @Test
    public void testOpenOutputStreamNew() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockConnection.isClosed()).andReturn(false).times(2);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(2);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(false);
        expect(mockFs.create(anyObject(Path.class))).andReturn(new FSDataOutputStream(new ByteArrayOutputStream(8)));
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertNotNull(b.openOutputStream(0, true));
    }

    @Test(expected = DuplicateBlobException.class)
    public void testOpenOutputStreamMissingBlob() throws Exception {
        expect(mockConnection.getBlobStore()).andReturn(mockStore).times(3);
        expect(mockConnection.isClosed()).andReturn(false).times(2);
        expect(mockStore.getId()).andReturn(blobStoreUri).times(2);
        expect(mockConnection.getFileSystem()).andReturn(mockFs).times(2);
        expect(mockFs.exists(anyObject(Path.class))).andReturn(true);
        replay(mockConnection, mockFs, mockStore);
        HDFSBlob b = new HDFSBlob(blobUri, mockConnection);
        assertNotNull(b.openOutputStream(0, false));
    }

    private FileStatus createTestFileStatus() {
        return new FileStatus(1024, false, 0, 0, 0, new Path(blobStoreUri + blobUri.toASCIIString().substring(5)));
    }

    public class SeekableInputStream extends ByteArrayInputStream implements PositionedReadable, Seekable {
        public SeekableInputStream(byte[] buf) {
            super(buf);
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length) throws IOException {
            return 0;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException {
        }

        @Override
        public void seek(long pos) throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public long getPos() throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            // TODO Auto-generated method stub
            return false;
        }

    }
}
