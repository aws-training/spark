CREATE DATABASE documents;

USE documents;

CREATE TABLE docs(text STRING);
    
LOAD DATA INPATH '/sp' INTO TABLE docs;

select * from docs LIMIT 100;

To display output on screen:
select word, COUNT(*) from docs LATERAL  VIEW explode(split(text,' ')) word_table as word GROUP BY word;

To create output in a seperate table:
create table word_count as select word, COUNT(*) as count from shakespeare LATERAL  VIEW explode(split(text,' ')) wcount as word GROUP BY word;

To Store output in a file:
INSERT OVERWRITE LOCAL DIRECTORY '/tmp/wcount' SELECT * FROM word_count;

SELECT * FROM word_count LIMIT 100;

DESCRIBE word_count;

DESCRIBE EXTENDED word_count;

