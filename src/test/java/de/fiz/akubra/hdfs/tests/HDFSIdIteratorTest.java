package de.fiz.akubra.hdfs.tests;

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
