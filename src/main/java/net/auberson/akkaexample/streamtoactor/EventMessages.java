package net.auberson.akkaexample.streamtoactor;

public class EventMessages {

	// Stream Init

	public static final class StreamInitMessage {
	};

	private static final StreamInitMessage STREAM_INIT_INSTANCE = new StreamInitMessage();

	public static final StreamInitMessage streamInit() {
		return STREAM_INIT_INSTANCE;
	}

	// Stream Finished

	public static final class StreamFinishedMessage {
	};

	private static final StreamFinishedMessage STREAM_FINISHED_INSTANCE = new StreamFinishedMessage();

	public static final StreamFinishedMessage streamFinished() {
		return STREAM_FINISHED_INSTANCE;
	}

	// MessageProcessed

	public static final class MessageProcessedMessage {
	};

	private static final MessageProcessedMessage MESSAGE_PROCESSED_INSTANCE = new MessageProcessedMessage();

	public static final MessageProcessedMessage messageProcessed() {
		return MESSAGE_PROCESSED_INSTANCE;
	}

}
