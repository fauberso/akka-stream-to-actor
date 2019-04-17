package net.auberson.akkaexample.streamtoactor;

import java.nio.charset.Charset;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.util.ByteString;

public class BookingActor extends AbstractActor {
	// Simulate a failure in this percentage of incoming messages:
	private static final int FAIL_PERCENT = 5;

	Random rnd = new Random();
	int messageCount = 0;
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	static Props props() {
		return Props.create(BookingActor.class);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder() //
				.match(ByteString.class, this::onMessage) //
				.match(BookingMessage.class, this::onMessage) //
				.matchAny(this::onMessageAny).build();
	}

	/**
	 * AMQP will deliver bytes. Here's an example of how to decode an AMQP payload
	 * into a proper message, then forward it.
	 * 
	 * @param payload
	 */
	public void onMessage(ByteString payload) {
		self().forward(new BookingMessage(Integer.parseInt(payload.decodeString(Charset.defaultCharset()))), context());
	}

	/**
	 * This processes the actual decoded message
	 * 
	 * @param message
	 */
	public void onMessage(BookingMessage message) {
		try {
			if (messageCount++ >= 10 && rnd.nextInt(100) >= 100 - FAIL_PERCENT) {
				// Introduce 5% chance of failure after the 10th message:
				log.warning("Simulating an exception, this will happen randomly in roughly 5% of all cases");
				throw new RuntimeException(
						"A mysterious and unexprected error has happened, unable to process " + message);
			}

			log.info("Booking Successful: " + message);
			sender().tell(AMQPConsumerStatus.ACK, self());
		} catch (Throwable t) {
			log.info("Booking failed: " + message);
			sender().tell(AMQPConsumerStatus.NACK, self());
		}
	}

	public void onMessageAny(Object o) {
		log.error("onMessage unknown message: " + o.toString());
	}
}