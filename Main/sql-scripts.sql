-- create the DATABASE
create database system_Performance;

-- use the DATABASE as a DEFAULT DATABASE
use system_Performance;

-- create the TABLE
create table performance (
time datetime,
cpu_usage numeric(5,2),
memory_usage numeric(5,2),
cpu_interrupts  numeric(18,0),
cpu_calls numeric(18,0),
memory_used numeric(18,0),
memory_free numeric(18,0),
bytes_sent numeric(18,0),
bytes_received numeric(18,0),
disk_usage numeric(5,2)
);













select * from performance;

