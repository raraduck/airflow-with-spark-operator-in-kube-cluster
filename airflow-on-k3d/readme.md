# Install Airflow on k3d

## 1. official k3d guide: [official-site-k3d]
[official-site-k3d]:https://k3d.io/stable/#releases
### 1.1. Install current latest release
- wget: 
```bash
wget -q -O - https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
```

## 2. Create Cluster with Sharing Local Directory
```bash
k3d cluster create airflow-cluster \
    --agents 3 \
    --volume <host-dags-path>:/opt/airflow/dags@all 
```
- ex)
```bash
k3d cluster create airflow-cluster \
    --agents 3 \
    --volume /home2/dwnusa/c20/dags:/opt/airflow/dags@all \
    --volume /home2/dwnusa/c20/data:/opt/airflow/data@all
```
### 2.1. Check shared volume in node
```bash
k3d node list
# NAME                            ROLE          CLUSTER           STATUS
# k3d-airflow-cluster-server-0    server        airflow-cluster   running
# k3d-airflow-cluster-agent-0     agent         airflow-cluster   running
docker exec -it k3d-airflow-cluster-agent-0 sh
~ $ ls -l /opt/airflow/data
# (for example)
# total 12
# -rw-r--r-- 1 2585 1999  47 Oct 14 09:20 data.csv
# drwxr-xr-x 2 2585 1999 512 Oct 14 09:18 logs
```

## 3. Create PV and PVC for empty volume mount
- pv-airflow-dag.yaml
```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pv-airflow-dags
spec:
  capacity:
    storage: 1Gi
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Retain
  storageClassName: local-path
  hostPath:
    path: /opt/airflow/dags
  claimRef:
    namespace: airflow
    name: airflow-dags
```
- pvc-airflow-dag.yaml
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-dags
  namespace: airflow
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
  storageClassName: local-path

```
Create PV and PVC
```bash
kubectl create ns airflow
# namespace/airflow created
kubectl create -f pv-airflow-dag.yaml # pv is global
# persistentvolume/pv-airflow-dags created
kubectl create -f pvc-airflow-dag.yaml -n airflow # pvc depends on namespace
# persistentvolumeclaim/airflow-dags created
kubectl get pv,pvc -n airflow # check STATUS Bound
```
## 4. Install Apache-airflow/airflow using helm with volume claim
(Updated 2025.10.14)  include tags
- **--set postgresql.image.tag=latest**
- **--set airflow.extraPipPackages="{apache-airflow-providers-apache-spark==4.1.1}"**

Airflow 2.8 → 3.0 전환에서 core에 있던 여러 provider packages가 완전히 분리되었습니다.

예전(2.x) : apache/airflow 이미지 자체에 SparkSubmitOperator, SparkSqlOperator 등이 포함되어 있었음

지금(3.x) : 이제 모두 apache-airflow-providers-apache-spark 패키지로 분리되어 있음

그래서 Airflow를 Helm으로 설치했을 때 기본 컨테이너엔 Spark 관련 Operator도 Connection Type도 없습니다.
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```
```bash
helm upgrade --install airflow apache-airflow/airflow  \
    --namespace airflow \
    --create-namespace \
    --set dags.persistence.enabled=true \
    --set dags.persistence.existingClaim=airflow-dags \
    --set airflow.extraPipPackages="{apache-airflow-providers-apache-spark,apache-airflow-providers-cncf-kubernetes}" \
    --set postgresql.image.tag=latest
    # --set persistence.enabled=true \
    # --set persistence.existingClaim=airflow-data \
```
or
```bash
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --create-namespace \
  -f airflow-values.yaml
```

## 5. Open api server 
- Option 1. Port-forwarding
```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```
- Option 2. NodePort
```bash
kubectl get svc -n airflow
# NAME                    TYPE        CLUSTER-IP    PORT(S)
# airflow-api-server      ClusterIP   10.43.11.75   8080/TCP
```
```bash
kubectl edit svc airflow-api-server -n airflow # type: ClusterIP >>> NodePort
# or 
kubectl patch svc airflow-api-server -n airflow -p '{"spec": {"type": "NodePort"}}'
```
```bash
kubectl get svc -n airflow
# NAME                    TYPE        CLUSTER-IP    PORT(S)
# airflow-api-server      NodePort    10.43.11.75   8080:32606/TCP
```
## 6. Access from Browser
- Option 1. port-forwarding
```bash
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```
http://127.0.0.1:8080/

- Option 2. edit loadbalancer to connect nodeport

k3d v5.6 이상 버전에서 지원
(이전 버전에서는 cluster recreate 필요)
```bash
k3d cluster edit airflow-cluster --port-add 8080:32606@loadbalancer
```
http://127.0.0.1:8080/

## 7. Tips
```bash
k3d cluster list
k3d cluster delete mycluster
k3d cluster delete --all
```