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

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class HDFSIdIteratorTest {
	@Test
	public void createIterator() {
		HDFSIdIterator it = new HDFSIdIterator(new ArrayList<URI>());
		assertNotNull(it);
		assertTrue(it.elementCount() == 0);
		assertFalse(it.hasNext());
	}

	@Test
	public void testNext() {
		List<URI> list = createTestList();
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
		List<URI> list = createTestList();
		HDFSIdIterator it = new HDFSIdIterator(list);
		assertTrue(it.elementCount() == list.size());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRemove() {
		List<URI> list = createTestList();
		HDFSIdIterator it = new HDFSIdIterator(list);
		it.remove();
	}

	private List<URI> createTestList() {
		List<URI> files = new ArrayList<URI>();
		files.add(URI.create("hdfs:" + "/test1"));
		files.add(URI.create("hdfs:" + "/test2"));
		files.add(URI.create("hdfs:" + "/test3"));
		files.add(URI.create("hdfs:" + "/test4"));
		files.add(URI.create("hdfs:" + "/test5"));
		files.add(URI.create("hdfs:" + "/test6"));
		files.add(URI.create("hdfs:" + "/test7"));
		files.add(URI.create("hdfs:" + "/foo/test8"));
		files.add(URI.create("hdfs:" + "/foo/test9"));
		files.add(URI.create("hdfs:" + "/foo/test10"));
		files.add(URI.create("hdfs:" + "/foo/bar/test11"));
		files.add(URI.create("hdfs:" + "/foo/bar/test12"));
		files.add(URI.create("hdfs:" + "/foo/bar//test13"));
		return files;
	}
}
