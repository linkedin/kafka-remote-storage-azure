# kafka-remote-storage-azure
This project is a [Kafka Tiered Storage KIP-405](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405%3A+Kafka+Tiered+Storage)
-compatible implementation of Remote Storage Manager (RSM) based on Azure Blob Storage. This implementation includes a set of
minimally viable features such as copying, fetching, and deleting log segments and index files to/from Azure Blob Storage. It
also includes an in-memory implementation of a blob cache and storage-key-based authentication for blobs I/O.

This implementation is released in open-source to gather early feedback from the community. There are known
[issues](/linkedin/kafka-remote-storage-azure/issues) with this plugin.

The RSM plugin is compatible with [linkedin/kafka 3.0-li branch](https://github.com/linkedin/kafka/tree/3.0-li) and
has been minimally tested on a small Linkedin Kafka 3.0 cluster using synthetic traffic. The scenarios tested
successfully include produce/consume, leadership change, migration of a non-tiered topic to a tiered configuration
(but not the other way around), various combinations of `local.retention.ms` and `retentino.ms`, and tiered topic deletion.

## Build
The Remote Storage Manager implementation for Azure Blob is published as a shadowjar.
To build the shadow jar run the following.
```bash
./gradlew clean shadowJar
```

## Run unit tests
The unit tests rely on a local installation of [Azurite Blob Service](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=npm)
emulator called `azurite-blob`. The executable should be accessible from `$PATH`. A new instance of Azurite Blob Service
is started before every batch of unit tests if it's not already listening to the default endpoint `http://127.0.0.1:10000`.

To bypass the unit tests run `./gradlew build -x test`.

## Using the Remote Storage Manager plugin
Include the plugin jar file in the classpath before running kafka-server.
```bash
CLASSPATH=/path/to/kafka-azure-rsm-<version>-shadow.jar bin/kafka-server-start.sh kafka-server.ssl.azurite.properties
```

Sample kafka-server configurations are provided below for local azurite connectivity.
```properties
# Remote Storage Manager Config
remote.log.storage.system.enable=true
remote.log.storage.manager.class.name=com.linkedin.kafka.tiered.storage.AzureBlobRemoteStorageManager

remote.log.storage.manager.impl.prefix=remote.log.storage.
remote.log.storage.azure.authentication.strategy=storage-key
remote.log.storage.azure.blob.storage.account=devstoreaccount1
# account key and name copied from public documentation:
# https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=npm#well-known-storage-account-and-key
remote.log.storage.azure.blob.storage.account.key=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==

# Azurite default endpoint
remote.log.storage.azure.blob.storage.endpoint=http://127.0.0.1:10000

# Remote Log Metadata Manager Config
remote.log.metadata.manager.class.name=org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager
remote.log.metadata.manager.listener.name=PLAINTEXT
remote.log.metadata.manager.impl.prefix=rlmm.config.
rlmm.config.remote.log.metadata.topic.num.partitions=5
rlmm.config.remote.log.metadata.topic.replication.factor=1
rlmm.config.remote.log.metadata.topic.retention.ms=-1
auto.create.topics.enable=false

# SSL Configs
listeners=PLAINTEXT://:9090,SSL://actualHostname:9190
ssl.client.auth=required
ssl.protocol=TLS
ssl.trustmanager.algorithm=SunX509
ssl.keymanager.algorithm=SunX509
ssl.keystore.type=pkcs12
ssl.keystore.location=/path/to/broker-identity.p12
ssl.keystore.password=hidden
ssl.key.password=hidden
ssl.truststore.type=JKS
ssl.truststore.location=/path/to/truststore
ssl.truststore.password=hidden
ssl.secure.random.implementation=SHA1PRNG
```
