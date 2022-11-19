-- DROP DATABASE IF EXISTS homework3;
-- CREATE DATABASE homework3;

--\c homework3;

CREATE TABLE rides_datamart
(
    id     INTEGER          NOT NULL,
    metric VARCHAR(16)      NOT NULL,
    value  DOUBLE PRECISION NOT null,
    primary key (id, metric)
);

