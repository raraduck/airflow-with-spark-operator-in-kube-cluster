# Install Kafka on k3d

## 1. official k3d guide: [official-site-k3d]
[official-site-k3d]:https://k3d.io/stable/#releases
### 1.1. Install current latest release
- wget: 
```bash
wget -q -O - https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
```

## 2. Create Cluster with Sharing Local Directory
```bash
k3d cluster create kafka-cluster \
    --agents 3 \
    --volume <host-data-path>:/opt/kafka-on-k3d@all 
```
- ex)
```bash
k3d cluster create kafka-cluster \
    --agents 3 \
    --volume /home2/dwnusa/c20/kafka-on-k3d:/opt/kafka-on-k3d@all 
```

## 2.1. NodePort setup
```bash
k3d cluster create kafka-cluster \
  --agents 3 \
    --port "30092:30092@server:*" \
    --port "30093:30093@server:*" \
    --port "30094:30094@server:*" \
    --port "30095:30095@server:*" \
    --port "30096:30096@server:*" \
    --port "30097:30097@server:*" \
  --volume /home2/dwnusa/c20/kafka-on-k3d:/opt/kafka-on-k3d@all \
  # http://localhost:30000
#   --volume /home2/dwnusa/c20/airflow-with-spark-operator-in-kube-cluster/kafka-on-k3d:/opt/kafka-on-k3d@all
```
or kubectl port-forward
```bash
kubectl port-forward -n kafka svc/kafka-ui 8080:8080
# http://localhost:8080
```

### 2.2. Check shared volume in node
```bash
k3d node list
docker exec -it k3d-kafka-cluster-agent-0 sh
~ $ ls -l /opt/kafka-on-k3d
```
## 3. Add helm repo for bitnami
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

## 4. install kafka helm
```bash
helm install kafka bitnami/kafka \
  --namespace kafka \
  --create-namespace \
  -f values.yaml \
  --set image.repository=bitnamilegacy/kafka \
  --set image.tag=latest
```
Or
```bash
helm install kafka bitnami/kafka \
  --namespace kafka \
  --create-namespace \
  -f values.yaml
```
```bash
kubectl get pods -n kafka
kubectl get svc -n kafka
kubectl get pods -l app.kubernetes.io/name=kafka -n kafka
```

## 4.1. manual installation tips
```bash
helm template kafka bitnami/kafka \
  --namespace kafka \
  -f values.yaml > kafka-cluster.yaml

kubectl create namespace kafka
kubectl apply -f kafka-cluster.yaml
```

## 4.2. delete or reinstall
```bash
helm list -n kafka 
helm uninstall kafka -n kafka # delete helm kafka
kubectl get all -n kafka # check
```



## 5. kafka-ui (manual install)
> kafka-ui.yaml
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-ui
  namespace: kafka
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-ui
  template:
    metadata:
      labels:
        app: kafka-ui
    spec:
      containers:
      - name: kafka-ui
        image: provectuslabs/kafka-ui:latest
        ports:
        - containerPort: 8080
        env:
        - name: KAFKA_CLUSTERS_0_NAME
          value: "local"
        - name: KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS
          value: "kafka.kafka.svc.cluster.local:9092"
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-ui
  namespace: kafka
spec:
  type: NodePort
  selector:
    app: kafka-ui
  ports:
  - name: http
    port: 8080
    targetPort: 8080
    nodePort: 30095
```
```bash
kubectl create namespace kafka
kubectl apply -f kafka-ui.yaml
```



## Create Topic 
```bash
kubectl exec -it kafka-controller-0 -n kafka -- bash

# Topic 생성
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic demo-topic \
  --replication-factor 1 \
  --partitions 3
```