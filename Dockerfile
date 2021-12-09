FROM hseeberger/scala-sbt:8u265_1.3.13_2.11.12

WORKDIR /opt/rms-plt-analytics

COPY . /opt/rms-plt-analytics 

RUN sbt assembly

ENTRYPOINT ["/usr/bin/sbt"]
