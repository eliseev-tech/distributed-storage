FROM maven:3.9.8-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY proto/pom.xml proto/pom.xml
COPY coordinator-service/pom.xml coordinator-service/pom.xml
COPY datanode-service/pom.xml datanode-service/pom.xml
COPY client-cli/pom.xml client-cli/pom.xml
COPY proto/src proto/src
COPY client-cli/src client-cli/src
RUN mvn -pl client-cli -am clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/client-cli/target/*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]