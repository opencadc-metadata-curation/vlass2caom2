FROM opencadc/matplotlib

RUN apt-get update -y && apt-get dist-upgrade -y && \
    apt-get install -y build-essential git wget && \
    rm -rf /var/lib/apt/lists/ /tmp/* /var/tmp/*

RUN oldpath=`pwd` && cd /tmp && \
    wget http://www.eso.org/~fstoehr/footprintfinder.py && \
    cp footprintfinder.py /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    chmod 755 /usr/local/lib/python3.8/site-packages/footprintfinder.py && \
    cd $oldpath

RUN pip install bs4 \
    cadcdata \
    cadctap \
    caom2 \
    caom2repo \
    caom2utils \
    ftputil \
    importlib-metadata \
    pillow \
    python-dateutil \
    PyYAML \
    spherical-geometry \
    vos

WORKDIR /usr/src/app

ARG OPENCADC_BRANCH=master
ARG OPENCADC_REPO=opencadc
ARG PIPE_BRANCH=master
ARG PIPE_REPO=opencadc

RUN pip install git+https://github.com/${PIPE_REPO}/caom2pipe@${PIPE_BRANCH}#egg=caom2pipe

RUN git clone https://github.com/${OPENCADC_REPO}/vlass2caom2.git --branch=${OPENCADC_BRANCH} && \
  cp ./vlass2caom2/data/ArchiveQuery-2018-08-15.csv /usr/src/ && \
  cp ./vlass2caom2/data/rejected_file_names-2018-09-05.csv /usr/src/ && \
  cp ./vlass2caom2/scripts/config.yml / && \
  cp ./vlass2caom2/scripts/docker-entrypoint.sh / && \
  pip install ./vlass2caom2

ENTRYPOINT ["/docker-entrypoint.sh"]
