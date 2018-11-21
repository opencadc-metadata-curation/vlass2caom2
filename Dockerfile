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
  cd caom2tools && git pull origin master && \
  pip install ./caom2utils && pip install ./caom2pipe && cd ..

RUN git clone https://github.com/opencadc-metadata-curation/vlass2caom2.git && \
  cp ./vlass2caom2/data/ArchiveQuery-2018-08-15.csv /usr/src/ && \
  cp ./vlass2caom2/data/rejected_file_names-2018-09-05.csv /usr/src/ && \
  pip install ./vlass2caom2

COPY ./docker-entrypoint.sh ./

ENTRYPOINT ["./docker-entrypoint.sh"]

