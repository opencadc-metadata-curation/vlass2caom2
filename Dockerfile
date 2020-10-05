FROM opencadc/matplotlib:3.8-slim
  
RUN apt-get update -y && apt-get dist-upgrade -y

RUN apt-get install -y build-essential \
    git

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

RUN apt-get install -y wget

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    cd $oldpath

WORKDIR /usr/src/app

RUN pip install bs4 \
    pillow

ARG OPENCADC_REPO=opencadc

RUN git clone https://github.com/${OPENCADC_REPO}/caom2tools.git && \
  pip install ./caom2tools/caom2 && \
  pip install ./caom2tools/caom2utils

RUN git clone https://github.com/${OPENCADC_REPO}/caom2pipe.git && \
  pip install ./caom2pipe

RUN git clone https://github.com/${OPENCADC_REPO}/vlass2caom2.git && \
  cp ./vlass2caom2/data/ArchiveQuery-2018-08-15.csv /usr/src/ && \
  cp ./vlass2caom2/data/rejected_file_names-2018-09-05.csv /usr/src/ && \
  cp ./vlass2caom2/scripts/config.yml / && \
  cp ./vlass2caom2/scripts/docker-entrypoint.sh / && \
  pip install ./vlass2caom2

ENTRYPOINT ["/docker-entrypoint.sh"]
