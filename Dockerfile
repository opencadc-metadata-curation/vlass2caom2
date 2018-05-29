FROM python:3.6-jessie


RUN pip install astropy && pip install numpy && \
        pip install spherical-geometry
RUN pip install cadcdata

RUN pip install pytest && pip install mock && pip install flake8 && \
        pip install funcsigs && pip install xml-compare && \
        pip install pytest-cov && pip install aenum && pip install future
RUN pip install caom2repo && pip install PyYAML

WORKDIR /usr/src/app

COPY ./docker-entrypoint.sh ./

ENTRYPOINT ["./docker-entrypoint.sh"]

