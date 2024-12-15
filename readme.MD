
copy jar docker cp "C:\Kafka-Team\flink_depo\flink-sql-connector-kafka-3.3.0-1.20.jar" jobmanager:/opt/flink/lib/
docker cp "C:\Kafka-Team\flink_depo\flink-sql-connector-kafka-3.3.0-1.20.jar" taskmanager:/opt/flink/lib/

docker restart id

docker restart 

docker cp "C:\Kafka-Team\flink_depo\flink-sql-connector-kafka-3.3.0-1.20.jar" 2da40033ce33:/opt/flink/lib/
docker restart c9c545d8ab4c
docker restart 5eecb9efc103a78861af80f460d74c68ac5808fbc432c0170995f359bea61b17


docker cp "C:\Kafka-Team\flink_depo\flink-sql-connector-kafka-3.3.0-1.20.jar" a45cd5868a0e6947ed33d2b1ff9f59dda93dd8c613cabfe87f96c65fc1dcb026:/opt/flink/lib/

create topic 
kafka-topics --create --topic SPY --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics --create --topic SPY_AVG  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

kafka-topics --list --bootstrap-server localhost:9092

docker exec -it 3d5aafc2431a bash
docker exec -it 3d5aafc2431a kafka-console-consumer --bootstrap-server localhost:9092 --topic SPY_AVG --from-beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic SPY --from-beginning


docker exec -it 3d5aafc2431a kafka-topics --create --topic SPY_AVG --bootstrap-server localhost:9092 --partitions 1 
kafka-console-consumer --bootstrap-server localhost:9092 --topic SPY_AVG --from-beginning--replication-factor 1

 Flink SQL Client Kullanarak SQL Sorguları Çalıştırmak


docker exec -it jobmanager /bin/bash
./bin/sql-client.sh


flink : DROP TABLE kafka_sink;

CREATE TABLE kafka_source (
    symbol STRING,
    price DOUBLE,
    proctime AS PROCTIME()  -- Verinin geldiği zamanı gösteren sütun
) WITH (
    'connector' = 'kafka',
    'topic' = 'SPY',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'consumer_of_stocks',  -- group.id ekleniyor
    'format' = 'json',
    'scan.startup.mode'= 'earliest-offset',
    'json.timestamp-format.standard' = 'ISO-8601'
);










Sending data for SPY: symbol='SPY' timestamp=datetime.datetime(2024, 12, 13, 17, 4, 7, 544554, tzinfo=datetime.timezone.utc) ask_exchange='V' ask_price=603.78 ask_size=1.0 bid_exchange='V' bid_price=603.52 bid_size=1.0 conditions=['R'] tape='B'
Message delivered to SPY [0]

streamlit run app.py

CREATE TABLE kafka_source (
>     symbol STRING,
>     price DOUBLE,
>     proctime AS PROCTIME()  -- Verinin geldiği zamanı gösteren sütun        
> ) WITH (
>     'connector' = 'kafka',
>     'topic' = 'SPY',
>     'properties.bootstrap.servers' = 'kafka:29092',
>     'properties.group.id' = 'consumer_of_stocks',  -- group.id ekleniyor    
>     'format' = 'json',
>     'scan.startup.mode'= 'earliest-offset',
>     'json.timestamp-format.standard' = 'ISO-8601'
> );


CREATE TABLE kafka_sink (
>     symbol STRING,
>     window_start TIMESTAMP(3),
>     window_end TIMESTAMP(3),
>     avg_price DOUBLE
> ) WITH (
>     'connector' = 'kafka',
>     'topic' = 'SPY_AVG',
>     'properties.bootstrap.servers' = 'kafka:29092',
>     'properties.group.id' = 'consumer_of_stocks',  -- group.id ekleniyor    
>     'format' = 'json',
>     'json.timestamp-format.standard' = 'ISO-8601'
> );
 INSERT INTO kafka_sink
> SELECT
>     symbol,
>     TUMBLE_START(proctime, INTERVAL '5' SECOND) AS window_start,
>     TUMBLE_END(proctime, INTERVAL '5' SECOND) AS window_end,
>     AVG(price) AS avg_price
> FROM kafka_source
> GROUP BY symbol, TUMBLE(proctime, INTERVAL '5' SECOND);