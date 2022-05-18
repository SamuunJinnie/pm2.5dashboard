FROM apache/airflow:2.3.0

ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV PYTHONPATH=$PYTHONPATH:${AIRFLOW_USER_HOME}

# Python packages required for th Selenium Plugin
USER root

# RUN pip install docker && \
#     pip install selenium

# RUN groupadd --gid 999 docker \
#    && usermod -aG docker airflow 
RUN apt-get update
RUN apt-get -y upgrade
RUN apt --fix-broken install
RUN apt-get install wget
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb
# RUN dpkg -i ./google-chrome-stable_current_amd64.deb



USER airflow

RUN pip install docker && \
    pip install selenium && \
    pip install webdriver_manager && \
    pip install chromedriver-autoinstaller && \
    pip install psycopg2-binary


# mkdir downloads