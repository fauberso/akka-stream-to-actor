package net.auberson.akkaexample.streamtoactor;

/**
 * Example message for a "Booking". This contains only an id.
 */
public class BookingMessage {

	public final int id;

	public BookingMessage(int id) {
		super();
		this.id = id;
	}

	@Override
	public String toString() {
		return String.format("Booking[%03d]", id);
	}

}
