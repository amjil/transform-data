FROM java:8-alpine
MAINTAINER Your Name <you@example.com>

ADD target/uberjar/transform-data.jar /transform-data/app.jar

EXPOSE 3000

CMD ["java", "-jar", "/transform-data/app.jar"]
