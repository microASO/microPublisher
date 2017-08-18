FROM tiangolo/uwsgi-nginx-flask:python3.6

COPY ./python /app

RUN mv /app/publisher.py /app/main.py

# docker build -t myimage .
# docker run -d --name mycontainer -p 80:80 myimage