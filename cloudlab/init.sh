sudo apt install python3.8 -y
sudo apt-get install python3-pip -y
python3.8 -m pip install kubernetes  
echo "alias k=kubectl" >> ~/.bashrc
echo "alias python=python3.8" >> ~/.bashrc

sudo apt install tmuxinator -y

# Use kafka instead of redpanda
cd /local/flink-1.13.2/redpanda-state-backend-for-flink/kafka
wget https://dlcdn.apache.org/kafka/3.2.1/kafka_2.13-3.2.1.tgz
tar -xzf kafka_2.13-3.2.1.tgz
sudo systemctl stop redpanda
sudo rm -r /tmp/kafka-logs/lost+found/