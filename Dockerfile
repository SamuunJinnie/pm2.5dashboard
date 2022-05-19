FROM apache/airflow:2.3.0

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV PYTHONPATH=$PYTHONPATH:${AIRFLOW_USER_HOME}

# Python packages required for th Selenium Plugin
USER root

# RUN pip install docker && \
#     pip install selenium

# RUN groupadd --gid 999 docker \
#    && usermod -aG docker airflow 
RUN apt-get update && apt install -y python3-dev
RUN apt-get install -y postgresql-server-dev-10 gcc python3-dev musl-dev
RUN apt-get -y upgrade
RUN apt --fix-broken install
RUN apt-get install wget
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb
# RUN dpkg -i ./google-chrome-stable_current_amd64.deb



USER airflow

RUN pip3 install docker && \
    pip3 install selenium && \
    pip3 install webdriver_manager && \
    pip3 install chromedriver-autoinstaller && \
    pip3 install psycopg2
    
USER root

# RUN service postgresql start
RUN ln -s /tmp/.s.PGSQL.5432 /var/run/postgresql/.s.PGSQL.5432


USER airflow

# mkdir downloads