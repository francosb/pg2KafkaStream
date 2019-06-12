## pg4KafkaStream

### Postgresql Stream to Kafka

Send events made on a Postgresql database to Kafka. 
pg4KafkaStream may be run as a stand-alone application from the command line.
    
### Getting Started

First, setup your Postgres database to support [logical replication](https://www.postgresql.org/docs/10/static/logical-replication.html),
and setup Kafka brokers with a topic.

#### Run pg2kafKaStream as a Stand-alone Application

```
java -jar pg2KafkaStream.jar --pgport <your_postgres_post> 
--pghost <your_postgres_host> --pguser <your_postgres_user> 
--pgpassword <your_postgres_password> --pgdatabase <your_postgres_database> 
--slotname <your_postgres_replication_slot_name> 
--kind <postgres_event_kind_to_publish> --tables <postgres_tables_to_publish> 
--brokers <kafka_brokers> --topic <kafka_topic>
``` 


