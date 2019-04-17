package net.auberson.akkaexample.streamtoactor;

import java.util.Optional;
import java.util.Random;

import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.alpakka.amqp.ReadResult;
import akka.stream.javadsl.Sink;
import net.auberson.akkaexample.streamtoactor.EventMessages.StreamFinishedMessage;
import net.auberson.akkaexample.streamtoactor.EventMessages.StreamInitMessage;

public class AMQPMediatorActor extends AbstractActor {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	static Props props() {
		return Props.create(AMQPMediatorActor.class);
	}
	
	static Sink<ReadResult, NotUsed> getSink(ActorRef mediatorActor) {
		return Sink.actorRefWithAck(mediatorActor, EventMessages.streamInit(), EventMessages.messageProcessed(),
				EventMessages.streamFinished(), ex -> ex);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder() //
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

	public void onMessage(ReadResult message) {
		//TODO: Temporary...

		log.info("Booking Successful: " + message);
		getSender().tell(EventMessages.messageProcessed(), self());
	}

	public void onMessageAny(Object o) {
		log.error("onMessage unknown message: " + o.toString());
	}
}