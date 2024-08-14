FROM ghcr.io/osgeo/gdal:ubuntu-full-3.8.4

RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    git \
    libpq-dev \
    ca-certificates \
    build-essential \
    && apt-get autoclean \
    && apt-get autoremove \
    && rm -rf /var/lib/{apt,dpkg,cache,log}


ADD . /tmp/dep-coastlines
WORKDIR /tmp
RUN pip install --no-cache-dir --upgrade pip && pip install ./dep-wofs

ADD . /code

WORKDIR /code

# Don't use old pygeos
ENV USE_PYGEOS=0
