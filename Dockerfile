FROM opencadc/matplotlib

RUN apk add --no-cache \
        bash \
        coreutils \
        git \
        g++ \
        libmagic \
        wget

RUN pip install cadcdata \
    cadctap \
    caom2 \
    caom2repo \
    caom2utils \
    ftputil \
    importlib-metadata \
    pytz \
    PyYAML \
    spherical-geometry \
    vos

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.7/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.7/site-packages/footprintfinder.py && \
    cd $oldpath

WORKDIR /usr/src/app

RUN apk add --no-cache jpeg-dev

RUN pip install bs4 \
    pillow

ARG OMC_REPO=opencadc-metadata-curation

RUN git clone https://github.com/${OMC_REPO}/caom2pipe.git && \
  pip install ./caom2pipe

RUN git clone https://github.com/${OMC_REPO}/vlass2caom2.git && \
  cp ./vlass2caom2/data/ArchiveQuery-2018-08-15.csv /usr/src/ && \
  cp ./vlass2caom2/data/rejected_file_names-2018-09-05.csv /usr/src/ && \
  cp ./vlass2caom2/scripts/config.yml / && \
  cp ./vlass2caom2/scripts/docker-entrypoint.sh / && \
  pip install ./vlass2caom2

RUN apk --no-cache del git

ENTRYPOINT ["/docker-entrypoint.sh"]
