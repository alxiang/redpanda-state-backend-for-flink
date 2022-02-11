FROM flink:1.13-scala_2.11-java11

# Install SSH
RUN apt-get -yq update && apt-get -yqq install ssh

# Set up SSH for accessing git repositories based on local private key
ARG SSH_KEY
ENV SSH_KEY=$SSH_KEY

RUN mkdir -p /root/.ssh
RUN echo "$SSH_KEY" > /root/.ssh/id_rsa
RUN chmod 600 /root/.ssh/id_rsa
RUN touch /root/.ssh/known_hosts
RUN ssh-keyscan github.com >> /root/.ssh/known_hosts

# Install Redpanda
RUN apt-get install -y curl
RUN curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | bash
RUN apt install -y redpanda
RUN rpk config bootstrap --id 0
RUN apt-get install -y systemd
# RUN systemctl start redpanda-tuner redpanda

# Clone Jiffy
RUN apt install -y git
RUN git clone git://github.com/alxiang/jiffy.git && cd jiffy && mkdir -p build && cd build

# Install Jiffy requirements
# RUN snap install cmake --classic
RUN apt-get install cmake -y
RUN echo cmake --version
RUN apt-get update
RUN apt-get install -y build-essential
RUN apt-get install -y libnuma-dev
RUN apt install -y libevent-dev
RUN apt install -y automake
RUN apt-get install -y libssl-dev
RUN apt install -y maven
RUN apt install -y python3-pip
RUN pip3 install setuptools

# Build Jiffy
RUN apt install -y openjdk-11-jdk
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME
RUN cd jiffy && git checkout cluster && cd build && cmake .. && make -j 4 && make install

#  Start a storage daemon
# RUN cd .. && storaged

