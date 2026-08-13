FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 --home /opt/ai-canal canal
WORKDIR /opt/ai-canal
COPY canal-server/target/canal-server-1.0.0-SNAPSHOT-all.jar app.jar
COPY canal-distribution/config/application.yaml config/application.yaml
RUN mkdir -p data && chown -R canal:canal /opt/ai-canal
USER 10001
EXPOSE 11111 11112
VOLUME ["/opt/ai-canal/data"]
ENTRYPOINT ["java","-XX:MaxRAMPercentage=75","-jar","app.jar","--config","config/application.yaml"]
