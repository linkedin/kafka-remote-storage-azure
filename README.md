# kafka-remote-storage-azure
KIP-405 Remote Storage Manager Plugin for Azure Blob Storage

# Build
The Remote Storage Manager implementation for Azure Blob is published as a shadowjar.
To build the shadow jar run the following.
```bash
./gradlew clean shadowJar
```

# Using the Remote Storage Manager plugin
Include the plugin jar file in the classpath before running kafka-server.
```bash
CLASSPATH=/path/to/azure-kafka-3.0.0-rsm-shadow.jar bin/kafka-server-start.sh kafka-server.ssl.azurite.properties
```

Sample kafka-server configurations are provided below for local azurite connectivity
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
ssl.keystore.location=/path/to//broker-identity.p12
ssl.keystore.password=hidden
ssl.key.password=hidden
ssl.truststore.type=JKS
ssl.truststore.location=/path/to/truststore
ssl.truststore.password=hidden
ssl.secure.random.implementation=SHA1PRNG
```