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

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;

/**
 * An {@link Iterator} implementation for the {@link HDFSBlobStoreConnection} implementation 
 * @author frank asseg
 *
 */
public class HDFSIdIterator implements Iterator<URI> {

	private final List<FileStatus> files;
	private final int len;
	private int currentIndex = 0;

	/**
	 * create a new {@link HDFSIdIterator} based on the supplied list
	 * @param list the {@link FileStatus} list to be iterated over
	 */
	public HDFSIdIterator(final List<FileStatus> list) {
		this.files = list;
		len = list.size();
	}

	@Override
	public boolean hasNext() {
		return currentIndex < len;
	}

	@Override
	public URI next() {
		return URI.create("hdfs:" + files.get(currentIndex++).getPath().getName());
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("optional remove is not implemented");
	}
	/**
	 * get the element count in the list
	 * @return the number of elements in the {@link Collection}
	 */
	public int elementCount() {
		return len;
	}
}
