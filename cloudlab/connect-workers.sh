#!/bin/bash

# Set up ssh
cp /local/flink-1.13.2/redpanda-state-backend-for-flink/cloudlab/.ssh/id_rsa ~/.ssh
chmod 600 ~/.ssh/id_rsa

# Joins the workers to the master kubelet
# Generate and replace comand with: sudo kubeadm token create --print-join-command
JOIN_COMMAND="kubeadm join 128.110.219.127:6443 --token rocswq.4fk2bfctvg4cufb0 --discovery-token-ca-cert-hash sha256:5c492f79f1d4059df604e2ae3739f49b59ddbbaaa75cfb0caddf806ab409dbec"

# /etc/hosts has one line for each node plus one line for localhost, 
NUM_NODES=$(($(wc -l < /etc/hosts)-1))
for ((i = 1; i < NUM_NODES; i++)); do
  ssh -o StrictHostKeyChecking=no alxiang@node$i ./local/flink-1.13.2/redpanda-state-backend-for-flink/cloudlab/worker-start.sh
  ssh -o StrictHostKeyChecking=no alxiang@node$i -t "sudo $JOIN_COMMAND"
done 
