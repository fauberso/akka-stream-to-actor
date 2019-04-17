package net.auberson.akkaexample.streamtoactor;

import java.time.Duration;
import java.util.Optional;

import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.stream.alpakka.amqp.ReadResult;
import akka.stream.alpakka.amqp.javadsl.CommittableReadResult;
import akka.stream.javadsl.Sink;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;

public class AMQPMediatorActor extends AbstractActor {
	final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	final ActorRef consumer;
	final Timeout timeout;

	private static final class MessageProcessed {
		static final MessageProcessed INST = new MessageProcessed();
	};

	private static final class StreamInit {
		static final StreamInit INST = new StreamInit();
	};

	private static final class StreamFinished {
		static final StreamFinished INST = new StreamFinished();
	};

	public AMQPMediatorActor(ActorRef consumer, Duration timeout) {
		this.consumer = consumer;
		this.timeout = Timeout.create(timeout);
	}

	static Props props(ActorRef consumer, Duration timout) {
		return Props.create(AMQPMediatorActor.class, consumer, timout);
	}

	static Sink<ReadResult, NotUsed> getAtMostOnceSink(ActorRef mediatorActor) {
		return Sink.actorRefWithAck(mediatorActor, StreamInit.INST, MessageProcessed.INST, StreamFinished.INST,
				ex -> ex);
	}

	static Sink<CommittableReadResult, NotUsed> getAtLeastOnceSink(ActorRef mediatorActor) {
		return Sink.actorRefWithAck(mediatorActor, StreamInit.INST, MessageProcessed.INST, StreamFinished.INST,
				ex -> ex);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder() //
				.match(CommittableReadResult.class, this::onMessage) //
				.match(ReadResult.class, this::onMessage) //
				.match(StreamInit.class, this::onMessage) //
				.match(ReadResult.class, this::onMessage) //
				.match(StreamFinished.class, this::onMessage) //
				.match(Throwable.class, this::onError) //
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

	public void onMessage(CommittableReadResult message) throws Exception {
		Future<Object> future = Patterns.ask(consumer, message.message(), timeout);
		AMQPConsumerStatus status = (AMQPConsumerStatus) Await.result(future, timeout.duration());

		if (status.equals(AMQPConsumerStatus.ACK)) {
			message.ack();
		} else if (status.equals(AMQPConsumerStatus.NACK)) {
			message.nack();
		} else {
			log.error("Unknown status (reverting to NACK): " + status);
			message.nack();
		}

		getSender().tell(MessageProcessed.INST, self());
	}

	public void onMessage(ReadResult message) throws Exception {
		Future<Object> future = Patterns.ask(consumer, message, timeout);
		AMQPConsumerStatus status = (AMQPConsumerStatus) Await.result(future, timeout.duration());
		if (status.equals(AMQPConsumerStatus.NACK)) {
			log.warning("Received NACK, but using auto-acknowledge: NACK ignored, and treated as ACK.");
		}
		getSender().tell(MessageProcessed.INST, self());
	}

	public void onError(Throwable t) {
		log.error(t, "Error in Stream processing");
	}

	public void onMessageAny(Object o) {
		log.error("onMessage unknown message: " + o.toString());
	}
}