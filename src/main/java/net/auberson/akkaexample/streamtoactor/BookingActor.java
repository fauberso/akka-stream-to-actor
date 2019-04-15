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
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessage(StreamFinishedMessage message) {
		log.info("Stream Finished");
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessage(BookingMessage message) {
		log.info("Booking Successful: " + message);
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessageAny(Object o) {
		log.error("onMessage unknown message: " + o.toString());
	}
}