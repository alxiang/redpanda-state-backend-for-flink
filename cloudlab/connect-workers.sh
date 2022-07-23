#!/bin/bash
# Joins the workers to the master kubelet

JOIN_COMMAND="$(kubeadm token create --print-join-command)"

# /etc/hosts has one line for each node plus one line for localhost, 
NUM_NODES=$(($(wc -l < /etc/hosts)-1))
for ((i = 1; i < NUM_NODES; i++)); do
  ssh -o StrictHostKeyChecking=no alxiang@node$i echo "hello world"
#   ssh -o StrictHostKeyChecking=no alxiang@node$i eval $JOIN_COMMAND
done 
