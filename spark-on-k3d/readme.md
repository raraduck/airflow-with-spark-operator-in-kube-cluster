# Install Spark on k3d

## 1. official k3d guide: [official-site-k3d]
[official-site-k3d]:https://k3d.io/stable/#releases
### 1.1. Install current latest release
- wget: 
```bash
wget -q -O - https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
```

## 2. Create Cluster with Sharing Local Directory
```bash
k3d cluster create spark-cluster \
    --agents 3 \
    --volume <host-data-path>:/opt/spark-data@all 
```
- ex)
```bash
k3d cluster create cluster \
    --agents 3 \
    --port "30092:30092@server:*" \
    --port "30093:30093@server:*" \
    --port "30094:30094@server:*" \
    --port "30095:30095@server:*" \
    --port "30096:30096@server:*" \
    --port "30097:30097@server:*" \
    --volume /home2/dwnusa/c20/data:/opt/spark-data@all \
    --volume /home2/dwnusa/c20/dags:/opt/airflow/dags@all \
```
### 2.1. Check shared volume in node
```bash
k3d node list
# NAME                            ROLE          CLUSTER           STATUS
# k3d-airflow-cluster-server-0    server        airflow-cluster   running
# k3d-airflow-cluster-agent-0     agent         airflow-cluster   running
docker exec -it k3d-airflow-cluster-agent-0 sh
~ $ ls -l /opt/spark-data
# (for example)
# total 12
# -rw-r--r-- 1 2585 1999  47 Oct 14 09:20 data.csv
# drwxr-xr-x 2 2585 1999 512 Oct 14 09:18 logs
```

## 3. Add helm repo for spark-operator 
```bash
helm repo add --force-update spark-operator https://kubeflow.github.io/spark-operator
helm repo update
```

## 4. Install Spark-operator
(Updated 2025.10.14)  bitnami/spark is deprecated

include tags
- **--set sparkJobNamespace=default**
```bash
helm install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set sparkJobNamespace=default \
    --set rbac.create=true \
    --set webhook.enable=true \
    --set serviceAccounts.spark.name=spark \
    --wait
```
- Helm으로 spark-operator 설치할 때는 공유 설정 불필요.
- SparkApplication YAML 안에서 driver/executor 에만 volumeMount 지정하세요.
- (또는 k3d cluster 생성 시 --volume 으로 모든 노드에 미리 공유 경로 추가!)

## 4. Submit Testing Spark Job
```bash
kubectl apply -f https://raw.githubusercontent.com/kubeflow/spark-operator/refs/heads/master/examples/spark-pi.yaml
# (Optional. Download yaml)
# curl -L -o spark-pi.yaml https://raw.githubusercontent.com/kubeflow/spark-operator/refs/heads/master/examples/spark-pi.yaml

kubectl get sparkapp spark-pi
# NAME       STATUS    ATTEMPTS   START                  FINISH       AGE
# spark-pi   RUNNING   1          2025-10-14T12:38:15Z   <no value>   99s

kubectl get pod -n default -l spark-role=driver 
# NAME              READY   STATUS      RESTARTS   AGE
# spark-pi-driver   0/1     Completed   0          14s

kubectl logs pod/spark-pi-driver
```

## 5. Submit python script with data sample
- To create output folder in spark-job pod (spark user), permission 777 is required
```bash
sudo chmod -R 777 /home2/dwnusa/c20/data
```

- /home2/dwnusa/c20/data/analyze_csv.py
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, count
import os

# 1 Spark 세션 생성
spark = SparkSession.builder.appName("AnalyzeCSV").getOrCreate()

# 2 CSV 로드
csv_path = "/opt/spark-data/data.csv"  # ConfigMap이나 PVC 마운트 경로
df = spark.read.option("header", True).csv(csv_path, inferSchema=True)

print("\n[1] CSV Schema:")
df.printSchema()

print("\n[2] 데이터 샘플:")
df.show(10, truncate=False)

# 3 컬럼 분석 (예시: score 컬럼 기준)
target_col = "age"

if target_col not in df.columns:
    raise ValueError(f"'{target_col}' 컬럼이 CSV에 존재하지 않습니다. 현재 컬럼들: {df.columns}")

# 4 평균, 최대, 개수 계산
result_df = df.select(
    avg(col(target_col)).alias("avg_age"),
    max(col(target_col)).alias("max_age"),
    count(col(target_col)).alias("count_rows")
)

print("\n[3] 통계 결과:")
result_df.show()

# 5 결과 파일로 저장 (Spark Operator에서는 driver pod 내부 경로)
output_dir = "/opt/spark-data/output"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "result.csv")

result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print(f"\n[4] 결과 파일 저장 완료 → {output_path}")

# 6 Spark 종료
spark.stop()
```

- spark-csv.yaml
```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-csv-analysis
  namespace: default # spark-operator
spec:
  type: Python
  mode: cluster
  image: docker.io/library/spark:4.0.0
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///opt/spark-data/analyze_csv.py
  sparkVersion: 4.0.0
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: 512m
    serviceAccount: spark-operator-spark
    volumeMounts:
      - name: data
        mountPath: /opt/spark-data
  executor:
    cores: 1
    instances: 2
    memory: 512m
    volumeMounts:
      - name: data
        mountPath: /opt/spark-data
  volumes:
    - name: data
      hostPath:
        path: /opt/spark-data
```
- Spark-job 제출 및 실행
```bash
kubectl create -f spark-csv.yaml
```


## Tips
```bash
k3d cluster list
k3d cluster delete mycluster
k3d cluster delete --all
```