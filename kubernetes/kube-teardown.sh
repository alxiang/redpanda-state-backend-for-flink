kubectl delete -f flink-configuration-configmap.yaml --force
kubectl delete -f jobmanager-service.yaml --force
kubectl delete -f jobmanager-session-deployment.yaml --force
kubectl delete -f taskmanager-session-deployment.yaml --force

