FROM google/cloud-sdk:latest

WORKDIR /usr/workspace/

RUN apt-get update && \
    apt-get install -y netcat

RUN git clone "https://github.com/bwarelabs/agave.git"

RUN chmod +x /usr/workspace/agave/storage-bigtable/init-bigtable.sh

COPY solana-data-migration-1b9caa70b7cc.json /usr/local/bin/solana-data-migration-1b9caa70b7cc.json
COPY start-bigtable.sh /usr/local/bin/start-bigtable.sh

RUN chmod +x /usr/local/bin/start-bigtable.sh

CMD gcloud beta emulators bigtable start

ENTRYPOINT "start-bigtable.sh"
