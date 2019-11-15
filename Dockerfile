FROM opencadc/matplotlib:3.6-alpine

RUN apk --no-cache add \
        bash \
        coreutils \
        git \
        g++ \
        libmagic \
        wget

RUN pip install cadcdata && \
    pip install cadctap && \
    pip install caom2repo && \
    pip install ftputil && \
    pip install PyYAML && \
    pip install spherical-geometry && \
    pip install vos

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.6/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.6/site-packages/footprintfinder.py && \
    cd $oldpath

WORKDIR /usr/src/app

RUN pip install bs4

RUN git clone https://github.com/opencadc-metadata-curation/caom2tools.git && \
  cd caom2tools && git pull origin master && \
  pip install ./caom2utils && pip install ./caom2pipe && cd ..

RUN git clone https://github.com/opencadc-metadata-curation/vlass2caom2.git && \
  cp ./vlass2caom2/data/ArchiveQuery-2018-08-15.csv /usr/src/ && \
  cp ./vlass2caom2/data/rejected_file_names-2018-09-05.csv /usr/src/ && \
  cp ./vlass2caom2/scripts/config.yml / && \
  cp ./vlass2caom2/scripts/docker-entrypoint.sh / && \
  pip install ./vlass2caom2

RUN apk --no-cache del git

ENTRYPOINT ["/docker-entrypoint.sh"]
