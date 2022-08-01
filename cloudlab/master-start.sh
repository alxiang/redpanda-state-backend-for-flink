#!/bin/bash

sudo swapoff -a
curl -sL https://gist.githubusercontent.com/alexellis/e8bbec45c75ea38da5547746c0ca4b0c/raw/23fc4cd13910eac646b13c4f8812bab3eeebab4c/configure.sh | sudo sh

# Master only
sudo kubeadm init

mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

kubectl apply -f "https://cloud.weave.works/k8s/net?k8s-version=$(kubectl version | base64 | tr -d '\n')"

sudo mkdir -p /etc/sysconfig
sudo touch /etc/sysconfig/kubelet
echo "KUBELET_EXTRA_ARGS=--root-dir=/dev/shm/kubelet" | sudo tee /etc/sysconfig/kubelet