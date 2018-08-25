FROM python:3.6

RUN pip install astropy && pip install numpy && \
        pip install spherical-geometry
RUN pip install cadcdata

RUN pip install pytest && pip install mock && pip install flake8 && \
        pip install funcsigs && pip install xml-compare && \
        pip install pytest-cov && pip install aenum && pip install future
RUN pip install caom2repo && pip install PyYAML

WORKDIR /usr/src/app
RUN git clone https://github.com/SharonGoliath/caom2tools.git && \
  cd caom2tools && git checkout s2303 && git pull origin s2303 && \
  pip install ./caom2utils && pip install ./caom2pipe

#RUN git clone -b master https://github.com/opencadc-metadata-curation/vlass2caom2.git && \
#  cd vlass2caom2 && pip install ./

COPY dist/vlass2caom2-0.2.tar.gz /tmp/
RUN pip install /tmp/vlass2caom2-0.2.tar.gz

COPY ./docker-entrypoint.sh /
COPY vlass_run_batch.sh /usr/local/bin/

ENTRYPOINT ["/docker-entrypoint.sh"]

