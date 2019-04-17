package net.auberson.akkaexample.streamtoactor;

import java.util.Optional;

import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.alpakka.amqp.ReadResult;
import akka.stream.javadsl.Sink;

public class AMQPMediatorActor extends AbstractActor {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	private static final class MessageProcessed {
		static final MessageProcessed INST = new MessageProcessed();
	};

	private static final class StreamInit {
		static final StreamInit INST = new StreamInit();
	};

	private static final class StreamFinished {
		static final StreamFinished INST = new StreamFinished();
	};

	static Props props() {
		return Props.create(AMQPMediatorActor.class);
	}

	static Sink<ReadResult, NotUsed> getSink(ActorRef mediatorActor) {
		return Sink.actorRefWithAck(mediatorActor, StreamInit.INST, MessageProcessed.INST, StreamFinished.INST,
				ex -> ex);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder() //
				.match(ReadResult.class, this::onMessage) //
				.match(StreamInit.class, this::onMessage) //
				.match(StreamFinished.class, this::onMessage) //
				.matchAny(this::onMessageAny).build();
	}

	@Override
	public void preRestart(Throwable reason, Optional<Object> message) throws Exception {
		super.preRestart(reason, message);
		log.warning("Actor restarting");
		getSender().tell(MessageProcessed.INST, self());
	}

	public void onMessage(StreamInit message) {
		log.info("Stream Initialized");
		getSender().tell(MessageProcessed.INST, self());
	}

	public void onMessage(StreamFinished message) {
		log.info("Stream Finished");
		getSender().tell(MessageProcessed.INST, self());
	}

	public void onMessage(ReadResult message) {
		// TODO: Temporary...

		log.info("Booking Successful: " + message);
		getSender().tell(MessageProcessed.INST, self());
	}

	public void onMessageAny(Object o) {
		log.error("onMessage unknown message: " + o.toString());
	}
}