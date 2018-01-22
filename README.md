# Kafka Connect JDBC Connector
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-jdbc.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-jdbc?ref=badge_shield)

This is a fork of [Kafka-Connect-Jdbc](https://github.com/confluentinc/kafka-connect-jdbc)
I made to implement two features I needed in a project : 

- **Inline views (multiple Queries)** : to give the ability to define views in 'table.whitelist' configuration option (similar to QUERY mode with multiple queries).
Some schemas can't be used with Kafka Connect without defining specific views (multiple timestamps columns,
use of table inheritance, no uniform incrementing/timestamp column naming etc.).
Unfortunately, it is not always possible to add views to some systems in production, or time needed for installation prohibits agile developpement pace needed in BI projects.
Kakfa-Connect-Jdbc's QUERY mode can be used for such cases but you can only define one query by Connect process which is limiting when you have a large number of views/queries to define in your project.

Note about the 'inline view' term : there is no standard definition of such views defined in FROM clause of a SQL query (inline views, 
subselect, derived tables...). To highlight the difference with regular views, I picked the term 'inline view' (not to give a preference to Oracle (...) but I found that it has more meaning).

- **Timestamps without timezone** : to store to Kafka correct date/time when working with timestamps datatypes without timezone (Oracle's legacy DATE data type in my case). I had some serious issues with that, because data was incorrectly converted to UTC timezone and so time was shifted in Kafka topics.
I introduced a new parameter to specify the correct timezone.

Minor changes :
- A change in Sink Postgresql Dialect was made to be able to use BLOB types : Postgresql type is BYTEA.
- when using `schema.pattern` configuration option, the schema name is appended as a prefix to the queried table, in case the connected user is not the owner of the schema.

Existing unit tests still pass and new tests were added (see new tests in JdbcSourceConnectorTest.java, JdbcSourceTaskUpdateTest.java and TimestampIncrementingTableQuerierTest.java).

Original documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-jdbc/docs/index.html).

# Install
The fork is based on Kafka-Connect-Jdbc 4.0.0.

Sample instructions for a 4.0.0 Confluent Platform installed in /opt/kafka/confluent-4.0.0 (Ubuntu) :

    $ sudo mv kafka-connect-jdbc-4.1.0-SNAPSHOT.jar /opt/kafka/confluent-4.0.0/share/java/kafka-connect-jdbc/
    $ sudo chown [your_user]:[your_user] /opt/kafka/confluent-4.0.0/share/java/kafka-connect-jdbc/kafka-connect-jdbc-4.1.0-SNAPSHOT.jar
    $ cd /opt/kafka/confluent-4.0.0/share/java/kafka-connect-jdbc/
    $ mv kafka-connect-jdbc-4.0.0.jar kafka-connect-jdbc-4.0.0.jar.ori
    $ mv kafka-connect-jdbc-4.1.0-SNAPSHOT.jar kafka-connect-jdbc-4.0.0.jar
    
# Use

- **Inline views** :
To define views in table.whitelist configuration option, you have to add two options in configuration file :
  - `inline.view.tag` : the string tag to be used as an marker to identify an inline view in `table.whitelist` config
  Default is : `view_`
  So, if you already ingest tables "table1" and "table2" and you want to add two inline views "test1" and "test2", using the default tag, you must use following setting for table.whitelist configuration option, in any order :
	`table.whitelist=table1, view_test1, table2, view_test2`
  The tag becomes a part of the topic name, so the following topics will be created : [topic_prefix]view_test1, [topic_prefix]view_test2
	
  - `inline.views.definitions` : the list of inline views definitions (SQL enclosed in parentheses) to be matched with views declaration in `table.whitelist`, in the same order.
	Due to current limitations of Kafka's list configuration options escaping [(KAFKA-4524)](https://issues.apache.org/jira/browse/KAFKA-4524), commas in SQL must be escaped with `\\\\` (4 antislashes).
	Example :
	If you have as in the previous example :
	`table.whitelist=table1, view_test1, table2, view_test2`
	And want to use following SQL views definitions for `view_test1` and `view_test2` :
	
`view_test1` : 
   
```
(SELECT table3.id,
table3.timestamp_value AS ts,
table4.attribute_value AS val1
FROM table3 JOIN table4
ON table3.id = table4.id)
```
	
`view_test2` : 
   
``` 
(SELECT table5.id,
COALESCE(table5.modification_timestamp, creation_timestamp) AS ts,`
table6.attribute_value AS val2`
FROM table5, table6
WHERE table5.id = table6.id)
```
		
  You can use the following configurations options :
   
```
table.whitelist=table1,view_test1,table2,view_test2
inline.views.definitions=(SELECT table3.id\\\\,table3.timestamp_value AS ts\\\\, table4.attribute_value AS val1 FROM table3 JOIN table4 ON table3.id = table4.id), (SELECT table5.id\\\\, COALESCE(table5.modification_timestamp, creation_timestamp) AS ts\\\\, table6.attribute_value AS val2 FROM table5\\\\, table6 WHERE table5.id = table6.id)
mode=timestamp
timestamp.column.name=ts
```
	
   Note that the views definitions in `inline.views.definitions` appear in the same order than the views elements (those prefixed with `inline.view.tag`) in `table.whitelist`. Note also that to use the timestamp mode, all the SQL views definitions must return the same timestamp column name, the same than the one defined in `timestamp.column.name`.
	Here the default `inline.view.tag` was used (`view_`) so it was not specified in the configuration.
	
- **Timestamps without timezone** :
To define a specific timezone that will be used with timestamp, date and time datatypes when constructing PreparedStatements (source and sink) and converting to Kafka topic (source), use the following configuration option
`db.timezone=[java.util.Timezone display name]`
Default is UTC.
If you use the special value `jvm`, then the timezone of the virtual machine running the task will be used.
Examples :

`db.timezone=GMT+5`

`db.timezone=jvm`

