FROM python:3-alpine

# Add Edge and bleeding repos
RUN echo -e '@edge http://dl-cdn.alpinelinux.org/alpine/edge/main' >> /etc/apk/repositories \
    && echo -e '@testing http://dl-cdn.alpinelinux.org/alpine/edge/testing' >> /etc/apk/repositories

RUN apk --update --no-cache add \
    # GDAL
    gdal-dev@testing \
    # geos
    geos-dev@testing \
    # Standard C/C++ tools/libs
    libc-dev@testing gcc g++

COPY . /src

RUN pip install -e /src/ && rm -fr /root/.cache

RUN mkdir /geozones

VOLUME /geozones
WORKDIR /geozones
ENV GEOZONES_HOME /geozones

ENTRYPOINT [ "/src/entrypoint.sh" ]

CMD = [ "--help" ]

EXPOSE 5000
