package pubsub

import com.google.cloud.pubsub.v1.AckReplyConsumer
import com.google.cloud.pubsub.v1.MessageReceiver
import com.google.cloud.pubsub.v1.SchemaServiceClient
import com.google.cloud.pubsub.v1.Subscriber
import com.google.pubsub.v1.GetSchemaRequest
import com.google.pubsub.v1.ProjectSubscriptionName
import com.google.pubsub.v1.PubsubMessage
import com.google.pubsub.v1.SchemaName
import com.google.pubsub.v1.SchemaView
import org.apache.avro.Conversions
import org.apache.avro.Schema
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumReader
import java.io.InputStream

fun main() {
    val projectId = System.getenv("GOOGLE_CLOUD_PROJECT")
        ?: throw IllegalArgumentException("GOOGLE_CLOUD_PROJECT environment variable must be set");
    val subscriptionId = "staging.fullpayments.payment-completed-debug"
    val subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId)

    val receiver = MessageReceiver { message: PubsubMessage, consumer: AckReplyConsumer ->
        var messageContentResolver = resolverFor(projectId, message.attributesMap)

        println("Message Id: ${message.messageId}")
        val data = messageContentResolver(message.data.newInput())
        println("Data: $data")
//        consumer.nack()
//        consumer.ack()
    }

    val subscriber = Subscriber.newBuilder(subscriptionName, receiver).build()
    subscriber.startAsync().awaitRunning()
    println("Listening for messages on $subscriptionId...")

    // Keep the main thread alive to listen for messages
    Thread.sleep(60000)
    subscriber.stopAsync()
}

fun resolverFor(projectId: String, headers: Map<String, String>): (content: InputStream) -> Any {
    val contentType = headers["contentType"]
    val encoding = headers["googclient_schemaencoding"] ?: "BINARY"
    val schemaName = headers["googclient_schemaname"]
    val schemaVersion = headers["googclient_schemarevisionid"]

    return when (contentType?.lowercase()) {
        "application/vnd.apache.avro",
        "application/vnd.apache.avro+json" -> {
            avroResolver(projectId, encoding, schemaName, schemaVersion)
        }
        else -> throw IllegalArgumentException("Unsupported content type: $contentType")
    }
}

fun avroResolver(
    projectId: String,
    encoding: String,
    schemaNameFromatted: String?,
    schemaVersion: String?
): (content: InputStream) -> Any {
    SchemaServiceClient.create().use { schemaServiceClient ->
        val versionSuffix = if (schemaVersion != null) "@$schemaVersion" else ""
        val schema = schemaServiceClient.getSchema(
            GetSchemaRequest.newBuilder()
                .setName("${SchemaName.parse(schemaNameFromatted)}$versionSuffix")
                .setView(SchemaView.FULL)
                .build()
        )

        val genericData = GenericData().apply {
            isFastReaderEnabled = true
            addLogicalTypeConversion(Conversions.UUIDConversion())
            addLogicalTypeConversion(Conversions.DurationConversion())
            addLogicalTypeConversion(Conversions.DecimalConversion())
            addLogicalTypeConversion(Conversions.BigDecimalConversion())

            addLogicalTypeConversion(TimeConversions.TimeMicrosConversion())
            addLogicalTypeConversion(TimeConversions.TimeMillisConversion())

            addLogicalTypeConversion(TimeConversions.TimestampNanosConversion())
            addLogicalTypeConversion(TimeConversions.TimestampMicrosConversion())
            addLogicalTypeConversion(TimeConversions.TimestampMillisConversion())

            addLogicalTypeConversion(TimeConversions.LocalTimestampNanosConversion())
            addLogicalTypeConversion(TimeConversions.LocalTimestampMicrosConversion())
            addLogicalTypeConversion(TimeConversions.LocalTimestampMillisConversion())
        }

        return when (encoding) {
            "BINARY" -> {
                // Create a parser for binary encoding

                { input: InputStream ->
                    val avroSchema = Schema.Parser().parse(schema.definition)
                    val datumReader = GenericDatumReader<GenericRecord>(avroSchema, avroSchema, genericData)
//                val datumReader = SpecificDatumReader<SpecificData>(avroSchema)
                    val binaryDecoder = DecoderFactory.get().binaryDecoder(input, null);
                    datumReader.read(null, binaryDecoder)
                        ?: throw IllegalArgumentException("Failed to parse Avro message")
                }
            }

            "JSON" -> {
                // Create a parser for JSON encoding
                { input: InputStream ->
                    val avroSchema = Schema.Parser().parse(schema.definition)
                    val datumReader = GenericDatumReader<GenericRecord>(avroSchema)
                    val binaryDecoder = DecoderFactory.get().jsonDecoder(avroSchema, input);
                    datumReader.read(null, binaryDecoder)
                        ?: throw IllegalArgumentException("Failed to parse Avro message")
                }
            }

            else -> throw IllegalArgumentException("Unsupported encoding: $encoding")
        }
    }
}
