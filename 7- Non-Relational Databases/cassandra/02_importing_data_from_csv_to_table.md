
## Connect to cqlsh

```
docker exec -it cas1 cqlsh --request-timeout=6000
Connected to MyCluster at 127.0.0.1:9042.
[cqlsh 5.0.1 | Cassandra 3.11.10 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
cqlsh>
```

```
[train@localhost cassandra]$ docker exec -it cas1 nodetool status
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens       Owns (effective)  Host ID                               Rack
UN  172.20.0.4  91.55 KiB  256          69.3%             4142a5b3-3ac6-4f76-9e58-368780c0915c  rack1
UN  172.20.0.2  95.03 KiB  256          61.8%             19d17b6e-42d5-482a-8cee-b974bd4ace31  rack1
UN  172.20.0.3  70.73 KiB  256          68.9%             ecba1792-22a8-436d-a3ed-4b46c740a7ec  rack1
```

```
[train@localhost cassandra]$ docker exec -it cas1 nodetool info
```

```
[train@localhost cassandra]$ docker-compose down
```

```
[train@localhost cassandra]$ ls -l node1
```

## Download csv file

` wget https://github.com/erkansirin78/datasets/raw/master/retail_db/order_items.csv `

## Start

` [train@localhost cassandra]$ docker-compose up -d `

```
[train@localhost cassandra]$ docker-compose ps
```

```
[train@localhost cassandra]$ docker exec -it cas1 nodetool status
```

```
[train@localhost cassandra]$ cat docker-compose.yaml
--- 
services: 
  cas1: 
    container_name: cas1
    environment: 
      - CASSANDRA_START_RPC=true
      - CASSANDRA_CLUSTER_NAME=MyCluster
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      - CASSANDRA_DC=datacenter1
      - HEAP_NEWSIZE=128M
      - MAX_HEAP_SIZE=1024M
    image: "cassandra:3.11"
    ports: 
      - "9042:9042"
    volumes: 
      - "./node1:/var/lib/cassandra/data"
      - "./cassandra_data:/cassandra_train_data"
  cas2: 
    command: "bash -c 'sleep 60;  /docker-entrypoint.sh cassandra -f'"
    container_name: cas2
    depends_on: 
      - cas1
    environment: 
      - CASSANDRA_START_RPC=true
      - CASSANDRA_CLUSTER_NAME=MyCluster
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      - CASSANDRA_DC=datacenter1
      - CASSANDRA_SEEDS=cas1
      - HEAP_NEWSIZE=128M
      - MAX_HEAP_SIZE=1024M
    image: "cassandra:3.11"
    ports: 
      - "9043:9042"
    volumes: 
      - "./node2:/var/lib/cassandra/data"
      - "./cassandra_data:/cassandra_train_data"
  cas3: 
    command: "bash -c 'sleep 120;  /docker-entrypoint.sh cassandra -f'"
    container_name: cas3
    depends_on: 
      - cas2
    environment: 
      - CASSANDRA_START_RPC=true
      - CASSANDRA_CLUSTER_NAME=MyCluster
      - CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch
      - CASSANDRA_DC=datacenter1
      - "CASSANDRA_SEEDS=cas1,cas2"
      - HEAP_NEWSIZE=128M
      - MAX_HEAP_SIZE=1024M
    image: "cassandra:3.11"
    ports: 
      - "9044:9042" 
    volumes: 
      - "./node3:/var/lib/cassandra/data"
      - "./cassandra_data:/cassandra_train_data"
version: "2"
```

## Copy file inside container

` sudo cp order_items.csv cassandra_data/ `

```
[train@localhost cassandra]$ docker exec -it cas1 bash
```

```
ls -l cassandra_data
```

## Create a keyspace

```
cqlsh> CREATE KEYSPACE retail WITH replication ={'class':'SimpleStrategy', 'replication_factor':3};
```

```
cqlsh> use retail ;
```

## create a table

```
CREATE TABLE if NOT EXISTS order_items(orderItemName int,orderItemOrderId int, orderItemProductId int, orderItemQuantity int, orderItemSubTotal float, orderItemProductPrice float, PRIMARY KEY((orderItemProductId), orderItemName));
```

## Insert csv file to table

```
COPY retail.order_items (orderItemName, orderItemOrderId, orderItemProductId, orderItemQuantity, orderItemSubTotal, orderItemProductPrice) FROM '/cassandra_train_data/order_items.csv' WITH HEADER = true; 
```

## Check data

```
cqlsh:retail> select count(*) from retail.order_items ;

 count
--------
 172198

(1 rows)

Warnings :
Aggregation query used without partition key
```

```
cqlsh:retail> select * from retail.order_items limit 10;
```
