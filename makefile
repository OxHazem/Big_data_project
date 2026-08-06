.PHONY: up down start stop hdfs spark export_hdfs hive pipeline

up:
	
	docker compose up -d
	sleep 20
	docker exec -it hadoop-namenode hdfs dfsadmin -safemode leave

down:
	docker compose down -v 

start:
	docker compose start

stop:
	docker compose stop

hdfs:
	bash src/HDFS/init_HDFS.sh

export_hdfs:
	bash src/HDFS/export_HDFS.sh

spark:
	bash src/spark/run_preprocessing.sh

hive:
	bash src/hive/run_schema.sh
	sleep 10
	bash src/hive/run_queries.sh

pipeline: hdfs  spark export_hdfs hive
	@echo "Pipeline completed successfully!"