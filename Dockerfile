FROM openjdk:11-jre-slim
ADD target/ObservationReplayer-1.0-SNAPSHOT-jar-with-dependencies.jar ObservationReplayer.jar
ENTRYPOINT ["java","-jar", "ObservationReplayer.jar"]