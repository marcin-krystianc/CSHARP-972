-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
CREATE TABLE IF NOT EXISTS my_keyspace.my_table (
partition_id int,
row_id int,
payload TEXT,
PRIMARY KEY (partition_id,row_id)
);
