
kafka-log-dirs --bootstrap-server $bs --describe --command-config ~/client.config > topics.json

kubectl cp monitoring/debug-tvtime:/root/topics.json ~/prj/pets/kafkatools/topics.json

mvn package

java -cp target/kafka-tools-1.0-SNAPSHOT-jar-with-dependencies.jar TopicsSize topics.json > data.sql

psql -X -1 -f data.sql 2>&1 | tee db.log

