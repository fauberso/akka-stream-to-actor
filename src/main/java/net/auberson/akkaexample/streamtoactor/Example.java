package net.auberson.akkaexample.streamtoactor;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import akka.stream.scaladsl.Sink;

public class Example implements Runnable {

	private final Config config;
	private final ActorSystem system;
	private final LoggingAdapter log;

	private Example() {
		config = ConfigFactory.load();
		system = ActorSystem.create("StreamToActorExample", config);
		log = Logging.getLogger(system, this);
		log.info("Actor system started");
	}

	public void run() {
		// A Regular Actor:
		ActorRef bookingActor = system.actorOf(BookingActor.props(), "bookingActor");

		// Our stream:
		final Source<Integer, NotUsed> source = Source.range(0, 999);

		final Sink<BookingMessage, NotUsed> sink = Sink.actorRefWithAck(bookingActor, EventMessages.streamInit(), EventMessages.messageProcessed(), EventMessages.streamFinished(), ex -> ex);

		final Materializer materializer = ActorMaterializer.create(system);

		//Run the stream with our actor as the sink
		source.map(id -> new BookingMessage(id)).log("emitted").runWith(sink, materializer);
	}

	public static void main(String[] args) {
		new Thread(new Example()).start();
	}

}
