package net.auberson.akkaexample.streamtoactor;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.stream.alpakka.amqp.ReadResult;

/**
 * This actor simulates the actual processing of the data from AMQP.
 * Additionally, it will check whether all messages (numbered from 0 to 999)
 * have been processed exactly once: It counts the number of times each id is
 * "booked".
 *
 */
public class BookingActor extends AbstractActor {
	// Simulate a failure in this percentage of incoming messages:
	private static final int FAIL_PERCENT = 5;

	final Random rnd = new Random();
	final int[] bookings = new int[1000];
	int messageCount = 0;

	// TimerTask that detects that the BookingActor is now idle.
	final Timer idleTimer = new Timer("IdleTimer");
	final TimerTask idleTask = createIdleTimerTask();

	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

	static Props props() {
		return Props.create(BookingActor.class);
	}

	public BookingActor() {
		idleTimer.schedule(idleTask, 1000, 250);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder() //
				.match(ReadResult.class, this::onMessage) //
				.match(BookingMessage.class, this::onMessage) //
				.matchAny(this::onMessageAny).build();
	}

	/**
	 * AMQP will deliver a ReadResult with a message as bytes. Here's an example of
	 * how to decode an AMQP payload into a proper message, then forward it.
	 * 
	 * @param payload
	 */
	public void onMessage(ReadResult message) {
		self().forward(new BookingMessage(Integer.parseInt(message.bytes().decodeString(StandardCharsets.UTF_8))),
				context());
	}

	/**
	 * This processes the actual decoded message
	 * 
	 * @param message
	 */
	public void onMessage(BookingMessage message) {
		try {
			// Introduce 5% chance of failure after the 10th message:
			if (messageCount++ >= 10 && rnd.nextInt(100) >= 100 - FAIL_PERCENT) {
				log.warning("Simulating an exception, this will happen randomly in roughly 5% of all cases");
				throw new RuntimeException(
						"A mysterious and unexprected error has happened, unable to process " + message);
			}

			// "Book" a reservation (simulated)
			bookings[message.id % bookings.length]++;

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

	/**
	 * Custom Timer tasks that check whether this actor is idle. Once it becomes
	 * idle (i.e. it hasn't received messages for 250 millis), statistics are
	 * printed out, and the ActorSystem is terminated.
	 */
	private TimerTask createIdleTimerTask() {
		return new TimerTask() {
			int lastMessageCount = 0;

			@Override
			public void run() {
				// Check whether number of processed messages has increased:
				if (lastMessageCount != messageCount) {
					lastMessageCount = messageCount;
					return;
				}

				// We're idle:
				int warnings = 0;
				log.info("BookingActor idle.");

				for (int i = 0; i < bookings.length; i++) {
					if (bookings[i] < 1) {
						log.error("{}: Booking {} has no bookings", ++warnings, i);
					}
					if (bookings[i] > 1) {
						log.error("{}: Booking {} has multiple bookings", ++warnings, i);
					}
				}

				if (warnings == 0) {
					log.info("{} bookings in total, all processed correctly (exactly 1 booking per id).", bookings.length);
				}

				// Terminate the Actor System and the JVM.
				akka.actor.ActorContext context = context();
				if (context != null) {
					idleTimer.cancel();
					context.system().terminate().value();
					System.exit(0);
				}
			}
		};
	}
}