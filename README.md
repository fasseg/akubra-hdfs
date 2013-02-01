Akubra (https://wiki.duraspace.org/display/AKUBRA/Akubra+Project) is a file system abstraction layer
which is used by fedora-commons (http://fedora-commons.org/)

This implementation enables fedora-commons to use a Hadoop filesystem (http://hadoop.apache.org/)
as an underlying object and datastream storage.

akubra-hdfs is still in an early development state and in no way ready for production use!

Installation instructions (Fedora Commons 3.6.2, Hadoop 1.0.3):
===============================================================

Dependencies
------------

Copy the following dependencies to your fedora webapp's WEB-INF/lib directory:
* akubra-hdfs-0.0.1-SNAPSHOT.jar (can be found in target/ after building the project)
* hadoop-core-1.0.3.jar from $HADOOP_HOME/
* hadoop-client-1.0.3.jar from $HADOOP_HOME/
* commons-configuration-1.6.jar from $HADOOP_HOME/lib/
* commons-lang-2.4.jar from $HADOOP_HOME/lib/ 


Configuration
-------------

Open the file $FEDORA_HOME/server/config/spring/akubra-llstore.xml and edit the two beans 'fsObjectStore' 
and 'fsDataStreamStore' to use the class 'de.fiz.akubra.hdfs.HDFSBlobStore'and the two beans 
'fsObjectStoreMapper' and fsDatastreamStoreMapper' to be of class 'de.fiz.akubra.hdfs.HDFSIdMapper':


  <bean name="fsObjectStore"
        class="de.fiz.akubra.hdfs.HDFSBlobStore"
        singleton="true">
    <!-- the hdfs namenode URI goes here -->
    <constructor-arg value="hdfs://localhost:9000/fedora/objects"/>
  </bean>
  
    <bean name="fsObjectStoreMapper"
        class="de.fiz.akubra.hdfs.HDFSIdMapper"
        singleton="true">
    <constructor-arg ref="fsObjectStore"/>
  </bean>
  


  <bean name="fsDatastreamStore"
        class="de.fiz.akubra.hdfs.HDFSBlobStore"
        singleton="true">
    <!-- the hdfs namenode URI goes here -->
    <constructor-arg value="hdfs://localhost:9000/fedora/datastreams"/>
  </bean>

  <bean name="fsDatastreamStoreMapper"
        class="de.fiz.akubra.hdfs.HDFSIdMapper"
        singleton="true">
    <constructor-arg ref="fsDatastreamStore"/>
  </bean>




akubra-hdfs is licensed under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)
