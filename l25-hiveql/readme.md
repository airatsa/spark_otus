## Решаем задачи из домашнего задания к уроку 12 RDD/Dataframe/Dataset

### Загружаем исходные данные

    curl -L https://github.com/airatsa/spark_otus/raw/main/l25-hiveql/data/taxi_zones.csv > taxi_zones.csv

    curl -L https://github.com/airatsa/spark_otus/raw/main/l25-hiveql/data/yellow_taxi_jan_25_2018/part-00000-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet > p-01.parquet

    curl -L https://github.com/airatsa/spark_otus/raw/main/l25-hiveql/data/yellow_taxi_jan_25_2018/part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet > p-02.parquet

### И копируем их в hdfs

    hdfs dfs -mkdir /user/hive/warehouse/taxizones
    hdfs dfs -put taxi_zones.csv /user/hive/warehouse/taxizones/

    hdfs dfs -mkdir /user/hive/warehouse/jan_25_2018
    hdfs dfs -put *.parquet /user/hive/warehouse/jan_25_2018/

### Проверяем наличине данных
    hdfs dfs -ls /user/hive/warehouse/taxizones
    hdfs dfs -ls /user/hive/warehouse/jan_25_2018


### Переходим в hive
    hive

    create database otus;

    CREATE EXTERNAL TABLE otus.taxi_facts (
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID INT,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE
    )
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/jan_25_2018/';

    CREATE EXTERNAL TABLE otus.taxi_facts (
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance DOUBLE,
        RatecodeID INT,
        store_and_fwd_flag STRING,
        PULocationID INT,
        DOLocationID INT,
        payment_type INT,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE
    )
    STORED AS PARQUET
    LOCATION '/user/hive/warehouse/jan_25_2018/';

    CREATE EXTERNAL TABLE otus.taxi_zones_raw (
        LocationID INT,
        Borough STRING,
        Zone STRING,
        service_zone STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/user/hive/warehouse/taxizones/'
    tblproperties("skip.header.line.count"="1");

    CREATE TABLE otus.taxi_zones (
        LocationID INT,
        Borough STRING,
        Zone STRING,
        service_zone STRING
    )
    stored as orc;

    INSERT INTO TABLE otus.taxi_zones
    SELECT LocationID, Borough, Zone, service_zone
    FROM otus.taxi_zones_raw;

### Чтобы select печатал схему данных
    set hive.cli.print.header=true;

### Проверяем данные

    SELECT * FROM otus.taxi_facts LIMIT 10;
    SELECT * FROM otus.taxi_zones LIMIT 10;

### Задание 1. Какие районы самые популярные для заказов

    CREATE VIEW otus.pickup_areas_by_popularity AS
    SELECT tz.LocationID AS loc_id, count(tf.PULocationID) AS num_pickups, min(tz.Borough) AS borough, min(tz.Zone) AS zone, min(tz.service_zone) AS service_zone
    FROM otus.taxi_facts tf JOIN otus.taxi_zones tz ON(tf.PULocationID = tz.LocationID)
    GROUP BY tz.LocationID
    ORDER BY num_pickups DESC;

    SELECT * FROM otus.pickup_areas_by_popularity;

### Задание 2. В какое время происходит больше всего вызовов
    SELECT pickup_hour, count(pickup_time) AS pickup_hour
    FROM (SELECT hour(tf.tpep_pickup_datetime) AS pickup_hour, tf.tpep_pickup_datetime AS pickup_time FROM otus.taxi_facts tf) tmp
    GROUP BY pickup_hour
    ORDER BY pickup_hour ASC;
   
### Задание 3. Витрина данных

    CREATE VIEW otus.metrics AS
    SELECT count(tf.trip_distance) AS total_rides, min(tf.trip_distance) AS dist_min, max(tf.trip_distance) AS dist_max, avg(tf.trip_distance) AS dist_average, stddev_pop(tf.trip_distance) AS dist_std_dev
    FROM otus.taxi_facts tf;

    SELECT * FROM otus.metrics;

