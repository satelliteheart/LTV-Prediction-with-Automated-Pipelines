FROM python:3.8-buster

WORKDIR /home
COPY . . 

RUN apt-get update 

RUN pip3 install -r requirements.txt
ENTRYPOINT ["python", "main.py"]