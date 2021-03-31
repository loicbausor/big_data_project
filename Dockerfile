#Download base image ubuntu 18.04
FROM ubuntu:18.04

ENV NB_USER loic
ENV NB_UID 1000
ENV HOME /home/${NB_USER}

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN apt-get update && apt-get install -y \
    tar \
    wget \
    bash \
    rsync \
    gcc \
    libfreetype6-dev \
    libhdf5-serial-dev \
    libpng-dev \
    libzmq3-dev \
    python3 \ 
    python3-dev \
    python3-pip \
    unzip \
    pkg-config \
    software-properties-common \
    graphviz \
	unrar

RUN adduser --disabled-password \
    --gecos "Default user" \
    --uid ${NB_UID} \
    ${NB_USER}

# Install OpenJDK-8
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Fix certificate issues
RUN apt-get update && \
    apt-get install ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f;
# Setup JAVA_HOME -- useful for docker commandline
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

RUN echo "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> ~/.bashrc

RUN apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir notebook==5.* numpy pyspark==3.1.1 spark-nlp==3.0.0 pandas mlflow Keras scikit-spark scikit-learn scipy matplotlib pydot tensorflow graphviz geopy pycountry-convert patool gdown geopandas bokeh wordcloud seaborn  

USER root
RUN chown -R ${NB_UID} ${HOME}
USER ${NB_USER}

WORKDIR ${HOME}

# Specify the default command to run
CMD ["jupyter", "notebook", "--ip", "0.0.0.0"]