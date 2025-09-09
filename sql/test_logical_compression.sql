CREATE DATABASE src;
CREATE DATABASE dst;
\c src
CREATE TABLE test(id int not null primary key, name text);
INSERT INTO test SELECT a, repeat(a::text, a) from generate_series(1, 100) as a;
CREATE PUBLICATION test FOR ALL TABLES;
SELECT pg_create_logical_replication_slot('test', 'pgoutput') IS NOT NULL;
\c dst
CREATE TABLE test(id int not null primary key, name text);
CREATE SUBSCRIPTION test CONNECTION 'dbname=src' PUBLICATION test WITH (CONNECT = FALSE);
ALTER SUBSCRIPTION test ENABLE;
ALTER SUBSCRIPTION test REFRESH PUBLICATION;
CREATE FUNCTION wait_subscription(len bigint)
 RETURNS void AS $function$
DECLARE
    loop_cnt int DEFAULT 0;
BEGIN
	WHILE (SELECT SUM(length(name)) FROM test) != len LOOP
        loop_cnt := loop_cnt + 1;
        IF loop_cnt > 600 THEN
            RAISE EXCEPTION 'Subscription did not caught up after 1 minute';
        ELSE
            PERFORM pg_sleep(0.1);
        END IF;
    END LOOP;
    LOOP
        PERFORM 1 FROM pg_stat_subscription
        WHERE pg_current_wal_lsn() - received_lsn = 0
        AND pg_current_wal_lsn() - latest_end_lsn = 0;
        EXIT WHEN FOUND;
        loop_cnt = loop_cnt + 1;
        IF loop_cnt > 600 THEN
            RAISE EXCEPTION 'Subscription did not caught up after 1 minute';
        ELSE
            PERFORM pg_sleep(0.1);
        END IF;
    END LOOP;
END
$function$ LANGUAGE plpgsql;
SELECT wait_subscription(10155);
\c src
UPDATE test SET name = name || name;
\c dst
SELECT wait_subscription(20310);
DROP SUBSCRIPTION test;
\c postgres
DROP DATABASE dst;
DROP DATABASE src;
