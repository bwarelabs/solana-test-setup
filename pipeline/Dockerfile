ARG hadoop_version=3.2.0
ARG python_version=3.6

FROM crs4/pydoop-base:${hadoop_version}-${python_version}

RUN apt-get update && apt-get install -y git python3 python3-pip

RUN ln -s /usr/bin/python3 /usr/bin/python || echo "Symlink already exists"

RUN git clone https://github.com/crs4/pydoop.git /build/pydoop

WORKDIR /build/pydoop

ENV PYTHON=python

RUN ${PYTHON} -m pip install --no-cache-dir --upgrade -r requirements.txt \
    && ${PYTHON} setup.py sdist \
    && ${PYTHON} -m pip install --pre dist/pydoop-$(cat VERSION).tar.gz

RUN ${PYTHON} -m pip install google-cloud-bigtable google-cloud 
# google-auth

COPY pipeline.py /app/pipeline.py
COPY solana-data-migration-1b9caa70b7cc.json /usr/local/bin/solana-data-migration-1b9caa70b7cc.json

WORKDIR /app
