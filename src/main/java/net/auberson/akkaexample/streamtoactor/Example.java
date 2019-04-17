package net.auberson.akkaexample.streamtoactor;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.apache.qpid.server.SystemLauncher;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.amqp.AmqpCachedConnectionProvider;
import akka.stream.alpakka.amqp.AmqpConnectionProvider;
import akka.stream.alpakka.amqp.AmqpDetailsConnectionProvider;
import akka.stream.alpakka.amqp.AmqpWriteSettings;
import akka.stream.alpakka.amqp.NamedQueueSourceSettings;
import akka.stream.alpakka.amqp.QueueDeclaration;
import akka.stream.alpakka.amqp.ReadResult;
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
import akka.stream.alpakka.amqp.javadsl.AmqpSource;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

public class Example implements Runnable {

	private final Config config;
	private final ActorSystem system;
	final Materializer materializer;
	private final LoggingAdapter log;
	private final AmqpConnectionProvider connectionProvider;

	private static final String qname = "TestQueue#" + Long.toHexString(System.currentTimeMillis()).toUpperCase();
	private final QueueDeclaration queueDeclaration;

	private Example() throws Exception {
		config = ConfigFactory.load();
		system = ActorSystem.create("StreamToActorExample", config);
		log = Logging.getLogger(system, this);
		materializer = ActorMaterializer.create(system);

		log.info("Actor system started");

		// Start AMQP Broker
		startBroker();

		// Initialize Alpakka connector
		AmqpDetailsConnectionProvider amqpConnection = AmqpDetailsConnectionProvider.create("localhost", 5672);
		connectionProvider = AmqpCachedConnectionProvider.create(amqpConnection);
		queueDeclaration = QueueDeclaration.create(qname);

		// Fill AMQP Broker with test data
		populateBroker(qname);
	}

	/**
	 * Starts an in-memory AMQP broker
	 */
	private SystemLauncher startBroker() throws Exception {
		SystemLauncher broker = new SystemLauncher();
		URL qpidConfigFile = this.getClass().getClassLoader().getResource("qpid-config.json");
		log.info("Starting AMQP Broker with configuration at " + qpidConfigFile.toString());
		Map<String, Object> attributes = new HashMap<>();
		attributes.put("type", "Memory");
		attributes.put("initialConfigurationLocation", qpidConfigFile.toExternalForm());
		broker.startup(attributes);
		log.info("AMQP Broker started.");

		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				broker.shutdown();
			}
		}));

		return broker;
	}

	private void populateBroker(String queueName) {
		final Sink<ByteString, CompletionStage<Done>> amqpSink = AmqpSink.createSimple(AmqpWriteSettings
				.create(connectionProvider).withRoutingKey(queueName).withDeclaration(queueDeclaration));

		CompletionStage<Done> writing = Source.range(0, 999).map(i -> ByteString.fromString(Integer.toString(i)))
				.runWith(amqpSink, materializer);

		log.info("Wrote 1000 messages to " + queueName);
	}

	public void run() {

		// A Regular Actor:
		//ActorRef bookingActor = system.actorOf(BookingActor.props(), "bookingActor");

		// Our stream, starting with a custom source that counts from 0 to 999...
		final Integer bufferSize = 1000;
		final Source<ReadResult, NotUsed> source = AmqpSource.atMostOnceSource(NamedQueueSourceSettings
				.create(connectionProvider, qname).withDeclaration(queueDeclaration).withAckRequired(false),
				bufferSize);

		// ...and finishing with a Sink that acknowledges Alpakka messages:
		ActorRef mediatorActor = system.actorOf(AMQPMediatorActor.props(), "mediatorActor");
		final Sink<ReadResult, NotUsed> sink = AMQPMediatorActor.getSink(mediatorActor) ;
		
		// Run the stream with our actor as the sink
		source.log("emitted").runWith(sink, materializer);
	}

	public static void main(String[] args) throws Exception {
		new Thread(new Example()).start();
	}

}
