########################################
# Stage 1: build fat JAR with sbt-assembly
########################################
FROM openjdk:11-slim AS builder

# Install curl and gnupg (no recommends), add sbt repo, install sbt
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99E82A75642AC823" | apt-key add - && \
    apt-get update && \
    apt-get install -y sbt

WORKDIR /app

# Pre-fetch dependencies (leverages Docker layer cache)
COPY build.sbt .
COPY project ./project
RUN sbt update

# Copy sources & assemble fat jar
COPY src ./src
RUN sbt clean assembly

########################################
# Stage 2: slim runtime image
########################################
FROM openjdk:11-jre-slim

WORKDIR /app


COPY --from=builder /app/target/scala-2.12/streams_intermediate-assembly-0.1.jar app.jar
ENTRYPOINT ["java","-jar","/app/app.jar"]\
