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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import de.fiz.akubra.hdfs.HDFSIdIterator;

public class HDFSIdIteratorTest {
	@Test
	public void createIterator() {
		HDFSIdIterator it = new HDFSIdIterator(new ArrayList<FileStatus>());
		assertNotNull(it);
		assertTrue(it.elementCount() == 0);
		assertFalse(it.hasNext());
	}

	@Test
	public void testNext() {
		List<FileStatus> list = createTestList();
		HDFSIdIterator it = new HDFSIdIterator(list);
		assertTrue(it.elementCount() == list.size());
		int count = 0;
		while (it.hasNext()) {
			it.next();
			count++;
		}
		assertTrue(count == list.size());
	}

	@Test
	public void testGetElementCount() {
		List<FileStatus> list = createTestList();
		HDFSIdIterator it = new HDFSIdIterator(list);
		assertTrue(it.elementCount() == list.size());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRemove() {
		List<FileStatus> list = createTestList();
		HDFSIdIterator it = new HDFSIdIterator(list);
		it.remove();
	}

	private List<FileStatus> createTestList() {
		List<FileStatus> files = new ArrayList<FileStatus>();
		files.add(new FileStatus(1293, false, 0, 64, System.currentTimeMillis(), new Path("/test1")));
		files.add(new FileStatus(1213, false, 0, 64, System.currentTimeMillis(), new Path("/test2")));
		files.add(new FileStatus(12233, false, 0, 64, System.currentTimeMillis(), new Path("/test3")));
		files.add(new FileStatus(113, false, 0, 64, System.currentTimeMillis(), new Path("/test4")));
		files.add(new FileStatus(137, false, 0, 64, System.currentTimeMillis(), new Path("/test5")));
		files.add(new FileStatus(13, false, 0, 64, System.currentTimeMillis(), new Path("/test6")));
		files.add(new FileStatus(0, false, 0, 64, System.currentTimeMillis(), new Path("/test7")));
		files.add(new FileStatus(1, false, 0, 64, System.currentTimeMillis(), new Path("/test8")));
		files.add(new FileStatus(12, false, 0, 64, System.currentTimeMillis(), new Path("/test9")));
		files.add(new FileStatus(93, false, 0, 64, System.currentTimeMillis(), new Path("/test10")));
		return files;
	}
}
