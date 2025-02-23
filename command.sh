# Post connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @debezium-postgres-connector.json

# Delete connector
curl -X DELETE http://localhost:8083/connectors/postgres-connector

# List topic
docker compose exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# Sub broker topics
docker compose exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --from-beginning --property print.key=true --topic postgres.public.customers

# Submit spark job
spark-submit --master local --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0 stream_kafka_to_minio.py