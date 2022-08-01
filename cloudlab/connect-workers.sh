#!/bin/bash

# Set up ssh
cp /local/flink-1.13.2/redpanda-state-backend-for-flink/cloudlab/.ssh/id_rsa ~/.ssh
chmod 600 ~/.ssh/id_rsa

# Joins the workers to the master kubelet
# Generate and replace comand with: sudo kubeadm token create --print-join-command
JOIN_COMMAND="kubeadm join 128.110.219.144:6443 --token cv4kp8.prto19199o8zivcg --discovery-token-ca-cert-hash sha256:d3e957627f1b580b28a923302ce17f3bead7a39e830cef5a482ca4d29e850812"

# /etc/hosts has one line for each node plus one line for localhost, 
NUM_NODES=$(($(wc -l < /etc/hosts)-1))
for ((i = 1; i < NUM_NODES; i++)); do
  ssh -o StrictHostKeyChecking=no alxiang@node$i ./local/flink-1.13.2/redpanda-state-backend-for-flink/cloudlab/worker-start.sh
  ssh -o StrictHostKeyChecking=no alxiang@node$i -t "sudo $JOIN_COMMAND"
done 
