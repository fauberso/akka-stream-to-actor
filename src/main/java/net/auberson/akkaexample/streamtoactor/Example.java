package net.auberson.akkaexample.streamtoactor;

import java.net.URL;
import java.time.Duration;
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
import akka.stream.alpakka.amqp.javadsl.AmqpSink;
import akka.stream.alpakka.amqp.javadsl.AmqpSource;
import akka.stream.alpakka.amqp.javadsl.CommittableReadResult;
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

	/**
	 * Set-up the example. This creates the actor system, materializer, and alpakka
	 * components. Also, it starts an AMQP broker and fills it with example data.
	 */
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

		system.registerOnTermination(new Runnable() {
			@Override
			public void run() {
				log.info("Shutting down AMQP Broker.");
				broker.shutdown();
			}
		});

		return broker;
	}

	/**
	 * Populates the AMQP broker with 1000 test messages (containing numbers from 0
	 * to 999)
	 */
	private void populateBroker(String queueName) {
		final Sink<ByteString, CompletionStage<Done>> amqpSink = AmqpSink.createSimple(AmqpWriteSettings
				.create(connectionProvider).withRoutingKey(queueName).withDeclaration(queueDeclaration));

		final CompletionStage<Done> writing = Source.range(0, 999).map(i -> ByteString.fromString(Integer.toString(i)))
				.runWith(amqpSink, materializer);
		writing.toCompletableFuture().join();

		log.info("Wrote 1000 messages to " + queueName);
	}

	/**
	 * Run the example: Create a flow consuming messages from AMQP. The consumer,
	 * bookingActor, may fail, in which case messages from AMQP will be reprocessed.
	 */
	public void run() {
		// A Regular Actor:
		ActorRef bookingActor = system.actorOf(BookingActor.props());

		// Our stream, starting with an AMQP messaging source with messages containing
		// the numbers from 0 to 999...
		final Integer bufferSize = 1000;
		final Source<CommittableReadResult, NotUsed> source = AmqpSource.committableSource(NamedQueueSourceSettings
				.create(connectionProvider, qname).withDeclaration(queueDeclaration).withAckRequired(true), bufferSize);

		// ...and finishing with a Sink that acknowledges Alpakka messages:
		ActorRef mediatorActor = system.actorOf(AMQPMediatorActor.props(bookingActor, Duration.ofSeconds(5)));
		final Sink<CommittableReadResult, NotUsed> sink = AMQPMediatorActor.getAtLeastOnceSink(mediatorActor);

		// Run the stream with our actor as the sink
		source.log("emitted").runWith(sink, materializer);

		// Have the bookingActor regularly check whether it's idle, then terminate.
		system.scheduler().schedule(Duration.ofSeconds(1), Duration.ofMillis(250), bookingActor,
				BookingActor.MSG_IDLE_CHECK, system.dispatcher(), null);
	}

	public static void main(String[] args) throws Exception {
		new Thread(new Example()).start();
	}

}
