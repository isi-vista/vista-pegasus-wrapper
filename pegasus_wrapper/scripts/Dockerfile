# Dockerfile, Image, Container
FROM python:3.6

LABEL name="Pegasus Wrapper Demo Container"
LABEL version=0
LABEL revision=1
LABEL maintainer="jacobl@isi.edu"

WORKDIR /home/app
COPY add_y.py /home/app/
RUN pip install git+https://github.com/isi-vista/saga-tools.git@master#egg=saga-tools
RUN pip install more_itertools
RUN pip install pegasus-wrapper

CMD echo "Docker running/completed"