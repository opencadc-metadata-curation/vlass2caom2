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

RUN pip install git+https://github.com/${OPENCADC_REPO}/caom2pipe@${OPENCADC_BRANCH}#egg=caom2pipe

RUN pip install git+https://github.com/${PIPE_REPO}/vlass2caom2@${PIPE_BRANCH}#egg=vlass2caom2

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
