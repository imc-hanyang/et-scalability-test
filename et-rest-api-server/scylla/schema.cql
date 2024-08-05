CREATE KEYSPACE et WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE et.data (user_id int PRIMARY KEY, value blob);