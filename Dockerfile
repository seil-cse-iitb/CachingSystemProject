# Start from Apache Ignite image.',
FROM apacheignite/ignite:2.5.0

# Set config uri for node.
ENV CONFIG_URI BitmapCluster-server.xml

# Copy optional libs.
ENV OPTION_LIBS ignite-rest-http

# Update packages and install maven.
RUN set -x \
    && apk add --no-cache \
        openjdk8

RUN apk --update add \
    maven \
    && rm -rfv /var/cache/apk/*

# Append project to container.
ADD . BitmapCluster

# Build project in container.
RUN mvn -f BitmapCluster/pom.xml clean package -DskipTests

# Copy project jars to node classpath.
RUN mkdir $IGNITE_HOME/libs/BitmapCluster && \
   find BitmapCluster/target -name "*.jar" -type f -exec cp {} $IGNITE_HOME/libs/BitmapCluster \;