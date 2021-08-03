FROM registry.scontain.com:5050/clenimar/test:pyspark311-amd-k8s

USER root

# FIXME: export appropriate env. vars instead of
# copying assembly jars over to $SPARK_HOME/jars.
RUN cp -r /spark/assembly/target/scala-2.12/jars /spark/jars

# Install kubectl. This is just for convenience reasons, since we use
# the container itself as a client.
RUN apk add --update curl \
    && curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" \
    && install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

WORKDIR /fspf

# Copy application code and libraries.
ADD . .

ENTRYPOINT [ "docker-entrypoint.sh" ]
