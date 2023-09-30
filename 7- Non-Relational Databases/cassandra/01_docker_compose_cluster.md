## Start docker service

` sudo systemctl start docker `

## Create new directory and copy docker-compose.yml file into it

## docker-compose.yml

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

## Start

` [train@localhost cassandra]$ docker-compose up -d `

- It needs 2-3 minutes

## check cluster is up

```
[train@localhost cassandra]$ docker-compose ps
Name              Command               State                             Ports
----------------------------------------------------------------------------------------------------------
cas1   docker-entrypoint.sh cassa ...   Up      7000/tcp, 7001/tcp, 7199/tcp, 0.0.0.0:9042->9042/tcp,
                                                9160/tcp
cas2   docker-entrypoint.sh bash  ...   Up      7000/tcp, 7001/tcp, 7199/tcp, 0.0.0.0:9043->9042/tcp,
                                                9160/tcp
cas3   docker-entrypoint.sh bash  ...   Up      7000/tcp, 7001/tcp, 7199/tcp, 0.0.0.0:9044->9042/tcp,
                                                9160/tcp
```

- If you see `cas3   docker-entrypoint.sh bash  ...   Exit 3` re-run `docker-compose up -d`

-
