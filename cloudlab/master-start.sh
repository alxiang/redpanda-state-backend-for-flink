#!/bin/bash

# kubeadm reset
# sudo apt-get purge kubeadm kubectl kubelet kubernetes-cni kube*   
# sudo apt-get autoremove  
# sudo rm -rf ~/.kube

sudo systemctl restart containerd

sudo swapoff -a
curl -sL https://gist.githubusercontent.com/alexellis/e8bbec45c75ea38da5547746c0ca4b0c/raw/23fc4cd13910eac646b13c4f8812bab3eeebab4c/configure.sh | sudo sh

# Master only
sudo kubeadm init

mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config

kubectl apply -f "https://cloud.weave.works/k8s/net?k8s-version=$(kubectl version | base64 | tr -d '\n')"

# for python imports
sudo apt-get install python3.8-dev
python -m pip install Cython
python -m pip install setuptools wheel
python -m pip install numpy kafka-python