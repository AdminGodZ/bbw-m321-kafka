# M321 Kafka Message Broker

Spring-Boot-Anwendung zur Übung "Kafka Message Broker" (Version 261017-ee74f281).
Sie sendet und empfängt Nachrichten über drei Topics:

| Kapitel | Endpunkt | Topic | Producer / Consumer |
|---|---|---|---|
| 2 – Strings | `POST /send?message=…` | `testTopic` | `KafkaProducer` / `KafkaConsumer` |
| 3 – JSON | `POST /sendStructured?message=…` | `structuredTopic` | `KafkaProducerStructured` / `KafkaConsumerStructured` |
| 5 – Transaktionen | `POST /sendTransactional?first=…&second=…&fail=…` | `transactionTopic` | `KafkaTransactionalProducer` / `KafkaTransactionalConsumer` |

`KafkaTopicConfig` legt die Topics beim Start an, damit die Listener sie vor der
ersten Nachricht abonnieren können.

## Starten

Voraussetzungen: Java 25 und Docker Desktop.

```sh
docker compose up -d
./gradlew bootRun
```

Die Requests aus `requests.http` lassen sich direkt in IntelliJ ausführen, oder mit curl:

```sh
curl -X POST "http://localhost:8080/send?message=Hello"
curl -X POST "http://localhost:8080/sendStructured?message=Hello%20Again"
curl -X POST "http://localhost:8080/sendTransactional?first=one&second=two"
curl -X POST "http://localhost:8080/sendTransactional?first=lost-one&second=lost-two&fail=true"
```

Die Consumer loggen jede empfangene Nachricht, z.B.
`YAY Received message in group myGroup: Hello`. Im Fehlerfall antwortet der
letzte Request mit HTTP 500, und es wird keine der beiden Nachrichten geloggt.

## Tests

```sh
./gradlew test
```

`KafkasampleApplicationTests` startet einen eingebetteten Kafka-Broker
(`@EmbeddedKafka`), Docker ist dafür nicht nötig. Die Tests prüfen alle drei
Flows, auch dass eine abgebrochene Transaktion keine Nachricht ausliefert.

## Aufgabe 1 – Verständnisfragen

**Was sind Streaming-Daten? Nennen Sie zwei Beispiele für Datenquellen.**
Streaming-Daten werden kontinuierlich und meist gleichzeitig von vielen Quellen
erzeugt. Es gibt kein definiertes Ende; die Daten werden fortlaufend und
inkrementell verarbeitet. Beispiele: IoT-Sensoren (Temperatur, GPS), Klickstreams
einer Webseite, Logfiles von Servern, Zahlungs- oder Börsentransaktionen.

**Welche Rolle haben Producer, Consumer und Broker?**
Der Producer schreibt Nachrichten (Records) in ein Topic. Der Consumer liest
Nachrichten aus einem Topic. Der Broker ist der Kafka-Server: Er speichert die
Partitionen auf Disk und beantwortet die Schreib- und Leseanfragen. Mehrere
Broker bilden einen Cluster.

**Was ist ein Topic, und was ist eine Partition? Wofür werden Partitionen gebraucht?**
Ein Topic ist ein benannter Kanal für eine Art von Nachrichten (z.B. `orders`).
Es ist in eine oder mehrere Partitionen aufgeteilt. Eine Partition ist ein
geordnetes Log, an das nur angehängt wird (append-only). Partitionen erlauben
Skalierung und Parallelität: Sie werden auf mehrere Broker verteilt und können
von mehreren Consumern einer Gruppe gleichzeitig gelesen werden. Ausserdem sind
sie die Einheit der Replikation.

**Wie unterscheidet sich Kafka von einer klassischen Message Queue?**
Eine klassische Queue löscht eine Nachricht, sobald ein Consumer sie bestätigt
hat, und jede Nachricht geht an genau einen Empfänger. Kafka speichert
Nachrichten persistent für eine konfigurierbare Zeit (Retention), unabhängig
davon, ob sie gelesen wurden. Beliebig viele Consumer Groups können dieselbe
Nachricht unabhängig voneinander lesen, auch mehrfach und von vorne (Replay).

**Was ist ein Offset, und wer merkt sich ihn?**
Der Offset ist die fortlaufende Position eines Records innerhalb einer
Partition. Der Consumer bzw. die Consumer Group merkt sich, bis wohin gelesen
wurde, und committet diesen Offset an den Broker. Der Broker speichert ihn im
internen Topic `__consumer_offsets`.

**Wofür ist eine Consumer Group da, und wie werden Partitionen verteilt?**
Eine Consumer Group teilt sich die Arbeit an einem Topic. Jede Partition wird
genau einem Consumer der Gruppe zugeteilt; ein Consumer kann mehrere Partitionen
bekommen. Gibt es mehr Consumer als Partitionen, bleiben die überzähligen untätig.
Tritt ein Consumer der Gruppe bei oder verlässt sie, verteilt Kafka die
Partitionen neu (Rebalancing). Verschiedene Groups erhalten jeweils alle Nachrichten.

**Welche Reihenfolge-Garantie bietet Kafka, und wo gilt sie nicht?**
Innerhalb einer Partition ist die Reihenfolge garantiert. Über mehrere
Partitionen eines Topics hinweg gibt es keine globale Reihenfolge. Wer
Reihenfolge braucht, gibt zusammengehörigen Nachrichten denselben Key, denn
gleiche Keys landen in derselben Partition. Auch innerhalb einer Partition
können Retries die Reihenfolge vertauschen, wenn der Producer nicht idempotent
ist (`enable.idempotence`, seit Kafka 3.0 Default) und mehrere Requests
gleichzeitig unterwegs sind.

**Warum spricht man von "entkoppelten" Systemen?**
Producer und Consumer kennen sich nicht, sie kennen nur das Topic. Sie müssen
nicht gleichzeitig laufen: Ein Consumer kann offline sein und später nachlesen.
Sie lassen sich unabhängig skalieren, deployen und in verschiedenen
Technologien umsetzen, und neue Consumer kommen hinzu, ohne den Producer zu ändern.

## Aufgabe 2 – Fragen zur Docker-Compose-Datei

**Welche Imageversion wird tatsächlich verwendet?**
`apache/kafka:latest`, also die neueste beim Pull verfügbare Version. Beim Test
war das Kafka **4.3.1**, festgestellt mit:
`docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --version`.
`latest` ändert sich mit der Zeit; für reproduzierbare Builds sollte man eine
feste Version angeben (z.B. `apache/kafka:4.3.1`).

**Auf welchem Port ist Kafka verfügbar, und wofür sind die anderen Ports?**
Auf dem Computer ist nur Port **9092** (Listener `EXTERNAL`, advertised als
`localhost:9092`) veröffentlicht. Port 19092 (`INTERNAL`) dient der
Kommunikation innerhalb des Docker-Netzwerks, also zwischen Brokern und für
andere Container unter `kafka:19092`. Port 9093 (`CONTROLLER`) nutzen die
KRaft-Controller für den Metadaten-Konsens.

**Was könnte "Replication Factor" sein?**
Die Anzahl Kopien jeder Partition auf verschiedenen Brokern. Fällt ein Broker
aus, ist die Partition auf einem anderen noch vorhanden. Weil hier nur ein
Broker läuft, ist der Faktor für die internen Topics (`__consumer_offsets`,
`__transaction_state`) auf 1 gesetzt; ein höherer Wert ginge gar nicht.
`MIN_ISR` (`min.insync.replicas`) legt bei `acks=all` fest, wie viele
In-Sync-Replikas mindestens verfügbar sein müssen, damit ein Schreibvorgang
bestätigt wird.

**Was bedeutet "Quorum" in diesem Kontext?**
Kafka verwaltet seine Cluster-Metadaten im KRaft-Modus mit dem Raft-Konsens
statt mit ZooKeeper. Das Quorum ist die Menge der Controller-Knoten (Voters),
die über diese Metadaten abstimmen; eine Mehrheit muss zustimmen.
`KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093` definiert ein Quorum aus genau
einem Voter: Node 1, erreichbar unter `kafka:9093`. Dieser Node ist dank
`PROCESS_ROLES: broker,controller` gleichzeitig Broker und Controller.

## Repetitionsfragen

**Wie funktioniert die Konfiguration über Spring Properties?**
Spring Boot liest `application.yaml` bzw. `application.properties` und bindet
die Werte unter `spring.kafka.*` an die Klasse `KafkaProperties`
(`@ConfigurationProperties`). Die Auto-Configuration erzeugt daraus
`ProducerFactory`, `ConsumerFactory`, `KafkaTemplate`, `KafkaAdmin` und
bei gesetztem `transaction-id-prefix` den `KafkaTransactionManager`. Die Werte
lassen sich überschreiben, etwa mit Umgebungsvariablen
(`SPRING_KAFKA_BOOTSTRAP_SERVERS`), Kommandozeilenargumenten oder Profilen.

**Lombok: Wie funktioniert `private final` ohne Konstruktor?**
`@RequiredArgsConstructor` generiert beim Kompilieren einen Konstruktor mit
allen nicht initialisierten `final`-Instanzfeldern (und nicht initialisierten
`@NonNull`-Feldern) als Parametern. Hat eine Klasse genau einen Konstruktor,
nutzt Spring ihn automatisch für die Constructor Injection, `@Autowired` braucht
es nicht.

## Aufgabe 3 – Verständnisfragen

**Was ist der Unterschied zwischen einem Topic und einer Consumer Group?**
Das Topic bestimmt, *wo* die Nachrichten liegen. Die Consumer Group bestimmt,
*wer* sie liest: Die Consumer einer Gruppe teilen sich die Partitionen und
haben gemeinsame Offsets. Jede Gruppe liest alle Nachrichten eines Topics.

**Welche Klasse sendet Nachrichten an ein Topic?**
`KafkaTemplate<K, V>`, z.B. mit `kafkaTemplate.send("testTopic", message)`.

**Wie wird eine Methode markiert, die Nachrichten empfangen soll?**
Mit `@KafkaListener(topics = "testTopic", groupId = "myGroup")`.

**Wie wird die Adresse des Kafka-Servers konfiguriert?**
Über die Property `spring.kafka.bootstrap-servers`, hier `localhost:9092`.

**Welche Art von Daten wird ausgetauscht?**
Einfache Strings (`String message`). Key und Value werden serialisiert und als
Bytes übertragen. Ab Kapitel 3 (JSON-Serializer) werden auch die Strings als
JSON-String (`"Hello"`) übertragen.

## Kapitel 4 – Zusätzlicher Consumer (CLI)

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic structuredTopic --from-beginning
```

Beobachtungen:

Die ersten beiden Punkte gelten für einen Consumer ohne gültigen committeten
Offset (ohne `--group` erzeugt das CLI jedes Mal eine neue Gruppe).

- **Ohne `--from-beginning`** zeigt der CLI-Consumer nur Nachrichten, die nach
  seinem Start eintreffen (`auto.offset.reset=latest`).
- **Mit `--from-beginning`** erscheinen alle Nachrichten ab dem frühesten noch
  vorhandenen Offset, auch jene, welche die Spring-App (Gruppe `myGroup`) längst
  konsumiert hat. Kafka löscht gelesene Nachrichten nicht; entfernt werden sie
  erst durch Retention oder Compaction.
- **Mit `--group cliGroup --from-beginning`** liefert der erste Aufruf alle
  Nachrichten, ein zweiter Aufruf keine mehr. Die Gruppe hat ihren Offset
  committet und macht dort weiter; `--from-beginning` (bzw. `auto.offset.reset`)
  greift nur, wenn die Gruppe keinen gültigen Offset hat.
- **Mit `--group myGroup`** wird der CLI-Consumer Teil der Gruppe der
  Spring-App, und die Partitionen werden zwischen beiden aufgeteilt; jede
  Nachricht kommt nur bei einem der beiden an. Beobachtet: Der CLI-Consumer
  bekam `structuredTopic-0`, und der Spring-Listener hatte für diese Zeit keine
  Partition dieses Topics.
- Die Nachrichten erscheinen als JSON. Mit `--property print.headers=true`
  sieht man den Header `__TypeId__:ch.bbw.m321.kafkasample.dto.StructuredMessage`,
  über den der `JacksonJsonDeserializer` den Zieltyp bestimmt.

## Aufgabe 5 – Transaktionen

Die Konfiguration steht in `application.yaml` (`transaction-id-prefix: tx-`,
`isolation-level: read_committed`). `KafkaTransactionConfig` erlaubt den
Kapiteln 2 und 3 weiterhin Sends ohne Transaktion.

Im Fehlerfall ruft `KafkaTransactionalProducer` vor der Exception
`kafkaTemplate.flush()` auf. `send()` arbeitet asynchron. Ohne `flush()` kann die
erste Nachricht beim Abbruch noch im Puffer des Producers liegen und wird dann
einfach verworfen (in unseren Versuchen ohne `flush()` war das jedes Mal so).
Mit `flush()` steht sie zuverlässig im Log, und der Unterschied zwischen den
Isolation Levels wird sichtbar. Für die Atomarität der Transaktion ist
`flush()` nicht nötig.

**Was passiert im Fehlerfall ohne `@Transactional`?**
Die erste Nachricht wird sofort ohne Transaktion gesendet (wegen
`setAllowNonTransactional(true)`), die zweite nie. Der Consumer erhält also
`first`, aber nicht `second`: ein halber, inkonsistenter Zustand. Der Test
`abortedTransactionDeliversNothing` schlägt genau dann fehl. Ohne
`allowNonTransactional` würde der Send selbst mit einer `IllegalStateException`
(keine laufende Transaktion) scheitern.

**Was passiert ohne `isolation-level: read_committed`?**
Der Default ist `read_uncommitted`. Der Consumer sieht dann auch Nachrichten
aus abgebrochenen Transaktionen. Mit dem CLI getestet:

```sh
# zeigt auch "lost-one" aus der abgebrochenen Transaktion
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactionTopic --from-beginning \
  --isolation-level read_uncommitted

# zeigt nur Nachrichten aus committeten Transaktionen
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic transactionTopic --from-beginning \
  --isolation-level read_committed
```

**Warum kann Kafka eine gesendete Message nicht zurückholen? Was geschieht stattdessen?**
Eine Partition ist ein Append-only-Log. Eine geschriebene Nachricht steht
bereits im Log, ist eventuell repliziert und wurde vielleicht schon von einem
`read_uncommitted`-Consumer gelesen. Es gibt keine Operation, die einen
einzelnen Record wieder aus dem Log entfernt; Records verschwinden nur später
durch Retention oder Compaction. Ein Abbruch entfernt also nichts.
Stattdessen schreibt der Transaction Coordinator am Ende der Transaktion einen **Transaction Marker** (Control
Record `COMMIT` oder `ABORT`) in jede beteiligte Partition. Ein
`read_committed`-Consumer liest nur bis zum Last Stable Offset und filtert die
Records abgebrochener Transaktionen heraus. Die Marker sind für die Anwendung
unsichtbar, belegen aber Offsets: Im manuellen Versuch mit Docker enthielt
`transactionTopic` 9 Records (davon `lost-one` aus der abgebrochenen
Transaktion), das End-Offset war aber 14 (4 `COMMIT`- und 1 `ABORT`-Marker,
bestätigt mit `kafka-dump-log.sh`).
