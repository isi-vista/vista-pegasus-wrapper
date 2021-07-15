# Dockerfile, Image, Container
FROM python:3.6

LABEL name="Pegasus Wrapper Demo Container"
LABEL version=0
LABEL revision=1
LABEL maintainer="jacobl@isi.edu"

WORKDIR /home/app
COPY add_y.py /home/app/
COPY sort_nums_in_file.py /home/app/

CMD echo "Docker running"