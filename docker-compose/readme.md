# Docker compose for Airflow and Spark Cluster

## Important Check Points
### Airflow 
- Airflow and Python version match to Spark Python: **apache/airflow:2.10.5-python3.8**
- Spark and Airflow must share same network: **spark-network**
- All pods (postgres, redis, airflow-worker, ...) must be in **spark-network**
- besides that
```bash
docker volume ls
docker volume rm <airflow-postgres-1 volume>
docker network ls
docker network rm spark-network
```
In airflow folder (http://localhost:8080)
```bash
docker compose up airflow-init
docker compose up -d

docker exec -it airflow-airflow-webserver-1 bash -c "pip install apache-airflow-providers-apache-spark==4.4.0 pyspark==3.5.0"
docker exec -it airflow-airflow-scheduler-1 bash -c "pip install apache-airflow-providers-apache-spark==4.4.0 pyspark==3.5.0"
docker exec -it airflow-airflow-triggerer-1 bash -c "pip install apache-airflow-providers-apache-spark==4.4.0 pyspark==3.5.0"
docker exec -it airflow-airflow-worker-1 bash -c "pip install apache-airflow-providers-apache-spark==4.4.0 pyspark==3.5.0"

docker exec -it airflow-airflow-worker-1 bash -c 'pip install "apache-airflow-providers-amazon==8.29.0"'

docker exec -it airflow-airflow-webserver-1 bash -c 'airflow connections add "spark_default" --conn-type "spark" --conn-host "spark://spark-master:7077"'

docker exec -it airflow-airflow-webserver-1 bash -c 'airflow connections add "aws_default" --conn-type "aws" --conn-login "AWS_ACCESS_KEY_ID" --conn-password "AWS_SECRET_ACCESS_KEY"'

docker exec -it airflow-airflow-webserver-1 bash -c 'airflow connections add "my_postgres" --conn-type "postgres" --conn-host "airflow-postgres-1" --conn-database "airflow" --conn-login "airflow" --conn-password "airflow" --conn-port "5432"'

# Check <your_table_name>
docker exec -it airflow-postgres-1 psql -U airflow -d airflow -c "\dt"

airflow variables set target_table <your_table_name>
```

### Spark
 - Spark version: **apache/spark:3.5.0** (which has python version 3.8)
 - PySpark version: **PySpark==3.5.0** (which must match to airflow spark-provider 4.4.0 with PySpark==3.5.0)

In spark folder (http://localhost:8085)
 ```bash
docker compose up -d
 ```