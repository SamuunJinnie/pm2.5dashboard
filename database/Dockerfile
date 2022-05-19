FROM postgres:13

ADD /setup.sql /docker-entrypoint-initdb.d/

RUN service postgresql start