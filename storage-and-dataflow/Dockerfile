FROM apache/beam_python3.8_sdk:latest

WORKDIR /usr/workspace

COPY . /usr/workspace

RUN pip install gcloud-storage-emulator

#RUN pip install -r requirements.txt

EXPOSE 9090

CMD gcloud-storage-emulator start --host=0.0.0.0 --port=9090 & \
    python your_pipeline_script.py
