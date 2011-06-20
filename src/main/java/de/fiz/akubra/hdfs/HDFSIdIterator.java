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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * An very simple {@link Iterator} implementation for the {@link HDFSBlobStoreConnection}
 * @author frank asseg
 *
 */
public class HDFSIdIterator implements Iterator<URI> {

	private final List<URI> uris;
	private final int len;
	private int currentIndex = 0;
	private static final Logger log=LoggerFactory.getLogger(HDFSIdIterator.class);
	/**
	 * create a new {@link HDFSIdIterator} based on the supplied list
	 * @param list the {@link FileStatus} list to be iterated over
	 */
	public HDFSIdIterator(final List<URI> list) {
		this.uris = list;
		len = list.size();
	}

	@Override
	public boolean hasNext() {
		return currentIndex < len;
	}

	@Override
	public URI next() {
		URI u=uris.get(currentIndex++);
		log.debug("iterating over " + u.toASCIIString());
		return u;
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
