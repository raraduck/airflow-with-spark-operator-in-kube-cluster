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
    --agents 3
```
- ex)
```bash
# k3d cluster create kafka-cluster \
#     --agents 3 \
#     --volume /home2/dwnusa/c20/dags:/opt/airflow/dags@all \
#     --volume /home2/dwnusa/c20/data:/opt/airflow/data@all
```

## 3. Add helm repo for bitnami
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```