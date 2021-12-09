FROM hseeberger/scala-sbt:8u265_1.3.13_2.11.12

WORKDIR /opt/plttools-spark

COPY . /opt/plttools-spark

RUN sbt assembly

ENTRYPOINT ["/usr/bin/sbt"]
