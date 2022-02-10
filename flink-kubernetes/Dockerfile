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
RUN curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh'
RUN su -E bash
RUN su apt install redpanda -y
RUN su rpk config bootstrap --id 0
RUN su systemctl start redpanda-tuner redpanda

# Clone Jiffy
RUN git clone git://github.com/alxiang/jiffy.git && cd jiffy && mkdir -p build && cd build

# Install Jiffy requirements
RUN su snap install cmake --classic
RUN su apt-get update
RUN su apt-get install -y build-essential
RUN su apt-get install libnuma-dev
RUN su apt install libevent-dev
RUN su apt install automake
RUN su apt-get install libssl-dev
RUN su apt install maven
RUN su apt install python-pip
RUN pip install setuptools

# Build Jiffy
RUN cmake ..
RUN make -j && su make install

#  Start a storage daemon
RUN cd .. && storaged

