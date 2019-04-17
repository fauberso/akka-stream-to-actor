package net.auberson.akkaexample.streamtoactor;

import java.util.Optional;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.alpakka.amqp.ReadResult;
import net.auberson.akkaexample.streamtoactor.EventMessages.StreamFinishedMessage;
import net.auberson.akkaexample.streamtoactor.EventMessages.StreamInitMessage;

public class BookingActor extends AbstractActor {
	// Simulate a failure in this percentage of incoming messages:
	private static final int FAIL_PERCENT = 0;

	Random rnd = new Random();
	int messageCount = 0;
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	static Props props() {
		return Props.create(BookingActor.class);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder() //
				.match(BookingMessage.class, this::onMessage) //
				.match(ReadResult.class, this::onMessage) //
				.match(StreamInitMessage.class, this::onMessage) //
				.match(StreamFinishedMessage.class, this::onMessage) //
				.matchAny(this::onMessageAny).build();
	}

	@Override
	public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
		super.preRestart(reason, message);
		log.warning("Actor restarting");
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessage(StreamInitMessage message) {
		log.info("Stream Initialized");
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessage(StreamFinishedMessage message) {
		log.info("Stream Finished");
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessage(BookingMessage message) {
		if (messageCount++ >= 10 && rnd.nextInt(100) >= 100 - FAIL_PERCENT) {
			// Introduce 5% chance of failure after the 10th message:
			log.warning("Simulating an exception, this will happen randomly in roughly 5% of all cases");
			throw new RuntimeException("A mysterious and unexprected error has happened, unable to process " + message);
		}

		log.info("Booking Successful: " + message);
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessage(ReadResult message) {
		//TODO: Temporary...

		log.info("Booking Successful: " + message);
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessageAny(Object o) {
		log.error("onMessage unknown message: " + o.toString());
	}
}