FROM opencadc/matplotlib:3.8-slim
  
RUN apt-get update -y && apt-get dist-upgrade -y

RUN apt-get install -y \
    git \
    python3-pip \
    python3-tz \
    python3-yaml \
    wget

RUN pip3 install cadcdata && \
    pip3 install cadctap && \
    pip3 install caom2 && \
    pip3 install caom2repo && \
    pip3 install caom2utils && \
    pip3 install deprecated && \
    pip3 install ftputil && \
    pip3 install importlib-metadata && \
    pip3 install pytz && \
    pip3 install PyYAML && \
    pip3 install spherical-geometry && \
    pip3 install vos

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    cd $oldpath

WORKDIR /usr/src/app

RUN pip3 install bs4

ARG OMC_REPO=opencadc-metadata-curation

RUN git clone https://github.com/${OMC_REPO}/caom2pipe.git && \
  pip install ./caom2pipe

RUN git clone https://github.com/${OMC_REPO}/vlass2caom2.git && \
  cp ./vlass2caom2/data/ArchiveQuery-2018-08-15.csv /usr/src/ && \
  cp ./vlass2caom2/data/rejected_file_names-2018-09-05.csv /usr/src/ && \
  cp ./vlass2caom2/scripts/config.yml / && \
  cp ./vlass2caom2/scripts/docker-entrypoint.sh / && \
  pip install ./vlass2caom2

RUN apt-get purge -y \
    git \
    wget

ENTRYPOINT ["/docker-entrypoint.sh"]
