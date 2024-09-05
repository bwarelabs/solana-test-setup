FROM google/cloud-sdk:latest

WORKDIR /usr/workspace/

RUN apt-get update && \
    apt-get install -y netcat

RUN git clone "https://github.com/bwarelabs/agave.git"

RUN chmod +x /usr/workspace/agave/storage-bigtable/init-bigtable.sh

COPY bigtable-service-account-key.json /usr/local/bin/bigtable-service-account-key.json
COPY start-bigtable.sh /usr/local/bin/start-bigtable.sh

RUN chmod +x /usr/local/bin/start-bigtable.sh

CMD gcloud beta emulators bigtable start

ENTRYPOINT "start-bigtable.sh"
