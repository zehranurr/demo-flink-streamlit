 ![alt text](images/image10.png)




![alt text](images\image10.png)



![alt text](images\image10.png)


![alt text]('C:\Kafka-Team\flink_depo\images\image10.png')




# To clone this repo 
```
git clone https://github.com/zehranurr/demo-flink-streamlit.git


```
# install library 
```
pip install requirements.txt

```
# Run docker-compose
```
docker-compose up --build

```

# To  copy jar
## Firstly copy to jobmanager
-  docker cp "flink-sql-connector-kafka-3.3.0-1.20.jar" jobmanager:/opt/flink/lib/
## Then restart jobmanager container 
- docker restart ID(jobmanager)
## Second copy to taskmanager
- docker cp "flink-sql-connector-kafka-3.3.0-1.20.jar" taskmanager:/opt/flink/lib/
## Then restart taskmanager container 
- docker restart id


![alt text](images/image-1.png)


# Create topic 
## Enter kafka container then create those 2 topics
- docker exec -it kafka /bin/bash 
- kafka-topics --create --topic SPY --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
- kafka-topics --create --topic SPY_AVG  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

## to be sure topics created run this 
- kafka-topics --list --bootstrap-server localhost:9092

## to listen topics
- docker exec -it id kafka-console-consumer --bootstrap-server localhost:9092 --topic SPY_AVG --from-beginning
- kafka-console-consumer --bootstrap-server localhost:9092 --topic SPY --from-beginning




#  Running SQL Queries Using Flink SQL Client
- docker exec -it jobmanager /bin/bash
- ./bin/sql-client.sh

# To create this table 

flink : DROP TABLE kafka_sink;

```

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

```
```
## First Table 
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

```
```
## Second Table 
CREATE TABLE kafka_sink (
     symbol STRING,
     window_start TIMESTAMP(3),
     window_end TIMESTAMP(3),
     avg_price DOUBLE
 ) WITH (
     'connector' = 'kafka',
     'topic' = 'SPY_AVG',
     'properties.bootstrap.servers' = 'kafka:29092',
     'properties.group.id' = 'consumer_of_stocks',  -- group.id ekleniyor    
     'format' = 'json',
     'json.timestamp-format.standard' = 'ISO-8601'
 );
```
```
## Connection
INSERT INTO kafka_sink
SELECT
        symbol,
        TUMBLE_START(proctime, INTERVAL '5' SECOND) AS window_start,
        TUMBLE_END(proctime, INTERVAL '5' SECOND) AS window_end,
        AVG(price) AS avg_price
    FROM kafka_source
    GROUP BY symbol, TUMBLE(proctime, INTERVAL '5' SECOND);

   
```
```
# To run Streamlit

 streamlit run app.py

```