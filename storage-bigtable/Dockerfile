FROM google/cloud-sdk:latest

WORKDIR /usr/workspace/

RUN git clone "https://github.com/bwarelabs/solana.git" -b "SOL-12-Implement-a-test-setup-for-the-project"

RUN chmod +x /usr/workspace/solana/storage-bigtable/init-bigtable.sh

COPY solana-data-migration-1b9caa70b7cc.json /usr/local/bin/solana-data-migration-1b9caa70b7cc.json
COPY start-bigtable.sh /usr/local/bin/start-bigtable.sh

RUN chmod +x /usr/local/bin/start-bigtable.sh

CMD gcloud beta emulators bigtable start

ENTRYPOINT "start-bigtable.sh"
