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

/**
 * AMQP Mediator: Consumes messages from an AMQP alpakka component, namely
 * either a ReadResult, or a CommittableReadResult. This actor contains
 * convenience methods to create a sink to an instance of this actor, with Back
 * Pressure and Acknowledgements.
 */
public class AMQPMediatorActor extends AbstractActor {
	private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	private final ActorRef consumer;
	private final Timeout timeout;
	private final Object endMarker;

	/**
	 * This message is sent from this actor to the Sink when a message has been
	 * processed, regardless of whether it was successful or not. This is used for
	 * Back Pressure.
	 */
	private static final class MessageProcessed {
		static final MessageProcessed INST = new MessageProcessed();
	};

	/**
	 * Sent by the Sink to this actor when the Stream is initialized.
	 */
	private static final class StreamInit {
		static final StreamInit INST = new StreamInit();
	};

	/**
	 * Sent by the Sink to this actor when the Stream is finished.
	 */
	private static final class StreamFinished {
		static final StreamFinished INST = new StreamFinished();
	};

	public AMQPMediatorActor(ActorRef consumer, Duration timeout) {
		this.consumer = consumer;
		this.timeout = Timeout.create(timeout);
		this.endMarker = null;
	}

	/**
	 * Creates an AMQP Mediator Actor
	 * 
	 * @param consumer the Actor to forward any incoming message to. This actor
	 *                 should be able to receive ReadResult messages, and is
	 *                 expected to reply with AMQPConsumerStatus.ACK or
	 *                 AMQPConsumerStatus.NACK
	 * @param timout   how long to wait for a reply before the call is viewed as
	 *                 failed.
	 */
	static Props props(ActorRef consumer, Duration timout) {
		return Props.create(AMQPMediatorActor.class, consumer, timout);
	}

	/**
	 * Creates an AMQP Mediator Actor
	 * 
	 * @param consumer  the Actor to forward any incoming message to. This actor
	 *                  should be able to receive ReadResult messages, and is
	 *                  expected to reply with AMQPConsumerStatus.ACK or
	 *                  AMQPConsumerStatus.NACK
	 * @param timout    how long to wait for a reply before the call is viewed as
	 *                  failed.
	 * @param endMarker the message to send to the consumer actor once the stream
	 *                  terminates.
	 */
	public AMQPMediatorActor(ActorRef consumer, Duration timeout, Object endMarker) {
		this.consumer = consumer;
		this.timeout = Timeout.create(timeout);
		this.endMarker = endMarker;

	}

	static Props props(ActorRef consumer, Duration timout, Object endMarker) {
		return Props.create(AMQPMediatorActor.class, consumer, timout, endMarker);
	}

	/**
	 * @return a Sink that forwards all elements to a Mediator actor, with
	 *         Backpressure working.
	 */
	static Sink<ReadResult, NotUsed> getAtMostOnceSink(ActorRef mediatorActor) {
		return Sink.actorRefWithAck(mediatorActor, StreamInit.INST, MessageProcessed.INST, StreamFinished.INST,
				ex -> ex);
	}

	/**
	 * @return a Sink that forwards all elements to a Mediator actor, with
	 *         Backpressure working, as well as Acknowledgements for the Messages in
	 *         the AMQP source.
	 */
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
		log.warning("AMQP Mediator restarting");
		getSender().tell(MessageProcessed.INST, self());
	}

	public void onMessage(StreamInit message) {
		log.info("Stream Initialized");
		getSender().tell(MessageProcessed.INST, self());
	}

	public void onMessage(StreamFinished message) {
		log.info("Stream Finished");
		if (endMarker != null) {
			consumer.tell(endMarker, self());
		}
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