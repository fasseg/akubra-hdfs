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

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An very simple {@link Iterator} implementation for the
 * {@link HDFSBlobStoreConnection}
 * 
 * @author frank asseg
 * 
 */
public class HDFSIdIterator implements Iterator<URI> {

    private static final Logger log = LoggerFactory.getLogger(HDFSIdIterator.class);
    private final FileSystem hdfs;
    private final String prefix;

    private final Queue<Path> dirQueue = new LinkedList<Path>();
    private final Queue<Path> fileQueue = new LinkedList<Path>();

    public HDFSIdIterator(final FileSystem hdfs, final String prefix) {
        this.hdfs = hdfs;
        if (prefix == null) {
            this.prefix = "";
        } else {
            this.prefix = prefix;
        }
        dirQueue.add(new Path("/"));
    }

    @Override
    public boolean hasNext() {
        while (fileQueue.isEmpty()) {
            if (!updateQueues()) {
                return false;
            }
        }
        return !fileQueue.isEmpty();
    }

    @Override
    public URI next() {
        while (fileQueue.isEmpty()) {
            if (!updateQueues()) {
                return null;
            }
        }
        return fileQueue.poll().toUri();
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("remove is not implemented");
    }

    private boolean updateQueues() {
        if (fileQueue.isEmpty()) {
            if (dirQueue.isEmpty()) {
                return false; // all queues are empty
            }
            Path dir = dirQueue.poll();
            try {
                for (FileStatus stat : hdfs.listStatus(dir)) {
                    if (stat.isDir()) {
                        dirQueue.add(stat.getPath());
                    } else if (stat.getPath().getName().startsWith(prefix)) {
                        fileQueue.add(stat.getPath());
                    }
                }
            } catch (IOException e) {
                log.error("Exception while updateing iterator queues", e);
                throw new RuntimeException(e);
            }
        }
        return true;
    }

}
