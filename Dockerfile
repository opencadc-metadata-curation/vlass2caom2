FROM python:3.6-alpine

RUN apk --no-cache add \
        bash \
        gcc \
        git \
        g++ \
        libxml2-dev \
        libxslt-dev \
        libffi-dev \
        make \
        musl-dev \
        openssl-dev

RUN pip install astropy && \
        pip install aenum && \
	pip install cadcdata && \
	pip install caom2repo && \
        pip install funcsigs && \
 	pip install future && \
	pip install numpy && \
	pip install PyYAML && \
        pip install spherical-geometry && \
	pip install xml-compare

WORKDIR /usr/src/app

RUN pip install bs4

RUN git clone https://github.com/opencadc-metadata-curation/caom2tools.git && \
  cd caom2tools && git pull origin master && \
  pip install ./caom2utils && pip install ./caom2pipe && cd ..

RUN git clone https://github.com/opencadc-metadata-curation/vlass2caom2.git && \
  cp ./vlass2caom2/data/ArchiveQuery-2018-08-15.csv /usr/src/ && \
  cp ./vlass2caom2/data/rejected_file_names-2018-09-05.csv /usr/src/ && \
  pip install ./vlass2caom2

RUN apk --no-cache del git

COPY ./docker-entrypoint.sh /

ENTRYPOINT ["/docker-entrypoint.sh"]

