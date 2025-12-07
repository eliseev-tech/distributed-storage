FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY proto/pom.xml proto/pom.xml
COPY coordinator-service/pom.xml coordinator-service/pom.xml
COPY datanode-service/pom.xml datanode-service/pom.xml
COPY client-cli/pom.xml client-cli/pom.xml
COPY proto/src proto/src
COPY datanode-service/src datanode-service/src
RUN mvn -pl datanode-service -am clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/datanode-service/target/*.jar app.jar
VOLUME ["/app/data"]
EXPOSE 50051
ENV GRPC_SERVER_PORT=50051
ENTRYPOINT ["java", "-jar", "app.jar"]