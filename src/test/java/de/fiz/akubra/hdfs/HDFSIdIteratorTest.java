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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HDFSIdIteratorTest {

    private FileSystem mockFs;
    private URI storeURI;

    private FileStatus[] createTestFiles(String prefix, boolean addDir) {
        List<FileStatus> files = new ArrayList<FileStatus>();
        files.add(new FileStatus(1402, false, 1, 67108864l, 0l, new Path(prefix + "/test1")));
        files.add(new FileStatus(1602, false, 1, 67108864l, 0l, new Path(prefix + "/testöÄüöl")));
        files.add(new FileStatus(102, false, 1, 67108864l, 0l, new Path(prefix + "/test&")));
        files.add(new FileStatus(12, false, 1, 67108864l, 0l, new Path(prefix + "/test with space")));
        files.add(new FileStatus(1102, false, 1, 67108864l, 0l, new Path(prefix + "/testËÚæêó")));
        files.add(new FileStatus(142, false, 1, 67108864l, 0l, new Path(prefix + "/test6")));
        if (addDir) {
            files.add(new FileStatus(142, true, 1, 67108864l, 0l, new Path(prefix + "/foo"))); // a
        }
        return (FileStatus[]) files.toArray(new FileStatus[6]);
    }

    private boolean find(URI needle, FileStatus[]... haystacks) {
        for (FileStatus[] haystack : haystacks) {
            for (FileStatus s : haystack) {
                if (s.getPath().toUri().equals(needle)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Before
    public void setUp() throws Exception {
        mockFs = createMock(FileSystem.class);
        storeURI = URI.create("hdfs://nohost:9000/");
    }

    @Test
    public void testIterator() throws Exception {
        FileStatus[] rootStats = createTestFiles(storeURI.toASCIIString(), true);
        FileStatus[] sub1Stats = createTestFiles(storeURI.toASCIIString() + "foo", false);
        expect(mockFs.listStatus(anyObject(Path.class))).andReturn(rootStats);
        expect(mockFs.listStatus(anyObject(Path.class))).andReturn(sub1Stats);
        replay(mockFs);
        HDFSIdIterator it = new HDFSIdIterator(mockFs, "test");
        assertNotNull(it);
        assertTrue(it.hasNext());
        int count = 0;
        while (it.hasNext()) {
            URI uri = (URI) it.next();
            assertTrue(find(uri, rootStats, sub1Stats));
            count++;
        }
        assertTrue(count == (rootStats.length - 1) * 2);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        HDFSIdIterator it = new HDFSIdIterator(mockFs, "test");
        it.remove();
    }
}
