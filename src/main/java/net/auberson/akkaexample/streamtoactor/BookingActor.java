package net.auberson.akkaexample.streamtoactor;

import java.util.Optional;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.actor.Status;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import net.auberson.akkaexample.streamtoactor.EventMessages.StreamFinishedMessage;
import net.auberson.akkaexample.streamtoactor.EventMessages.StreamInitMessage;

public class BookingActor extends AbstractActor {

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
				.match(StreamInitMessage.class, this::onMessage) //
				.match(StreamFinishedMessage.class, this::onMessage) //
				.matchAny(this::onMessageAny).build();
	}

	@Override
	public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
		super.preRestart(reason, message);
		getSender().tell(new Status.Failure(reason), self());
	}

	public void onMessage(StreamInitMessage message) {
		log.info("Stream Initialized");
		getSender().tell(new Status.Success(message), self());
	}

	public void onMessage(StreamFinishedMessage message) {
		log.info("Stream Finished");
		getSender().tell(new Status.Success(message), self());
	}

	public void onMessage(BookingMessage message) {
		if (messageCount++ >= 10 && rnd.nextInt(100) >= 95) {
			// Introduce 5% chance of failure after the 10th message:
			log.warning("Simulating an exception, this will happen randomly in roughly 5% of all cases");
			throw new RuntimeException("A mysterious and unexprected error has happened, unable to process " + message);
		}

		log.info("Booking Successful: " + message);
		getSender().tell(new Status.Success(""), self());
	}

	public void onMessageAny(Object o) {
		log.error("onMessage unknown message: " + o.toString());
	}
}