# Distributed key value database

Este é um banco de dados chave valor distribuído.
O objetivo deste projeto é implementar "Distributed Patterns" conforme apresentado no seguinte vídeo: https://www.youtube.com/watch?v=Adu_dbcnUHA

### Como compilar e rodar
Esse é uma aplicação feita em groovy com Spring Boot.

####Pre-requisitos:
  - JDK 8 (ou maior) instalada.
  - Docker
  
1. Rodar o redis no docker
 - docker run --name some-redis -d redis                           

2. Na basta raiz do projeto executar:
 - ./gradlew clean build

3. Depois disso subir o nodo master:
 - java -jar build/libs/node-0.0.1-SNAPSHOT.jar

4. E então os demais nodos:
 - java -jar build/libs/node-0.0.1-SNAPSHOT.jar --spring.profiles.active=node02
 - java -jar build/libs/node-0.0.1-SNAPSHOT.jar --spring.profiles.active=node03
 - java -jar build/libs/node-0.0.1-SNAPSHOT.jar --spring.profiles.active=node04

### Como usar
Tanto a leitura quando a escrita podem ser feitas através de qualquer um dos nodos.

####Escrevendo dados:
 - curl -v -d 'Value' -H "Content-Type: application/json" -X POST http://localhost:8080/db/{key}
 - curl -v -d 'dado teste 001' -H "Content-Type: application/json" -X POST http://localhost:8080/db/key001

####Lendo dados:
 - curl -v -X GET http://localhost:9090/db/{key}   

### TODO
[X] V1 DONE
  [X] Partitioned Distributed Hash DONE
  [X] Save and Get Operations DONE
  [X] Static Cluster DONE

[] V2
  [] Replication
    [X] Dados replicados DONE
    [X] Master node distribuindo partições DONE
    [] Retornar dados quando leader cair. DOING
  [] Dynamic Cluster

[] V3
  [] Vector clocks
  [] Read Repair

