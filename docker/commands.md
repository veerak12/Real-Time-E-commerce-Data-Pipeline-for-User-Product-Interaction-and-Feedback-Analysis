for running the docker files 
-- first we have to create a common network for it

docker network create ekart_network

docker exec -it kafka kafka-topics.sh --create --topic ekart_topic --partitions 2 --replication-factor 1 --bootstrap-server kafka:9092


docker exec -it kafka kafka-topics.sh --list --bootstrap-server kafka:9092

"if datanode is not communicating with the namenode try to change the configurations on
hdfs-site.xml and core-site.xml so that we can give the configuration where the data can bestored in datanode...""

