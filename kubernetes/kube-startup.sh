kubectl create -f flink-configuration-configmap.yaml
kubectl create -f jobmanager-service.yaml
kubectl create -f jobmanager-session-deployment.yaml
kubectl create -f taskmanager-session-deployment.yaml

PODNAME=$(kubectl get pods --selector=component=jobmanager -o name --no-headers=true)
while ! kubectl port-forward "$PODNAME" 8888:8081
do
    echo waiting for jobmanager container to start
    sleep 2
done




