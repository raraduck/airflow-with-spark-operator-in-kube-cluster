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

### 2.1. Check shared volume in node
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

kubectl get pods -n kafka
kubectl get svc -n kafka
```

## 4.1. delete or reinstall
```bash
helm list -n kafka 
helm uninstall kafka -n kafka # delete helm kafka
kubectl get all -n kafka # check
```