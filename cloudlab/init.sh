sudo apt install python3.8 -y
sudo apt-get install python3-pip -y
python3.8 -m pip install kubernetes  
echo "alias k=kubectl" >> ~/.bashrc
echo "alias python=python3.8" >> ~/.bashrc

sudo apt install tmuxinator -y
