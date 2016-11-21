FROM ubuntu

RUN apt-get update
RUN apt-get install -yq python python-pip
RUN pip install socketIO_client==0.5.6
RUN pip install grpcio-tools

ADD timberslide_pb2.py timberslide_pb2.py
ADD pytimberslide.py pytimberslide.py
ADD wikimedia.py wikimedia.py

CMD ["/usr/bin/python", "/wikimedia.py"]
