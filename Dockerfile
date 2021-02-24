FROM p7hb/docker-spark
WORKDIR /project/code_jar
COPY target/scala-2.12/Secureworks_scala_coding_challenge-assembly-0.1.jar .
COPY spark-submit.sh .
COPY NASA_access_log_Jul95.gz /project/NASA_access_log_Jul95.gz
RUN ["chmod", "+x", "./spark-submit.sh"]
ENTRYPOINT ["./spark-submit.sh"]