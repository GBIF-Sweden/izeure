izeure
======

Purpose
-------

This program goes through the Bourgogne data warehouse and indexes it in Solr 
partly or totally according to the parameter provided at the command line.

Note that only the last version of a record is indexed.

Requirement
-----------
The Bourgogne database (<a href="http://github.com/GBIF-Sweden/bourgogne">http://github.com/GBIF-Sweden/bourgogne</a>), 
Java JDK 1.7 and a Solr installation. 

This program has been developed against Solr version 4.3.0. So it should work 
with upper versions.

Installation
------------
For creating the index you must copy the file schema.xml in the configuration
directory of your Solr collection.

Before running the program for the first time, you need to make a copy of the file <code>src/resources/META-INF/persistence_vanilla.xml</code>
in the same location and name it <code>persistence.xml</code>. In <code>persistence.xml</code>, change database, user and 
password accordingly to your installation. You also need to customize the <code>solrURL</code> variable in
<code>src/main/java/se/gbif/izeure/Izeure.java</code> in accordance to your Solr installation.

Run
---
Copy the file <code>sample.xml</code> located at the root of this directory and
name it according to the resource you want to use. Then open it and change the
content for your needs.

On the command line:
<code>
java -jar izeure.jar resource.xml
</code>