# Akka Stream-to-Actor Example

In this example, messages are taken from an AMQP message broker using Akka Streaming and the Alpakka AMQP component. They are then forwarded to a regular Akka actor where they are consumed.

The problem: Akka Streaming takes care of backpressure, and the Alpakka messages allow the consumer to commit messages after they are processed,thereby preventing loss of messages on failures.
But using a regular Actor as a Sink breaks the backpressure mechanism. Furthermore, this actor could forward the message to an actor on another VM, which then would be unable to commit the Alpakka message (this other VM may not even have Alpakka installed, or access to the AMQP broker). 

In this example, this is solved by using a special actor, the AMQP Mediator, which serves as a Sink for the Akka Streams flow, and forwards the contents of the Alpakka messages to an actor using the Ask pattern. This actor may then process and forward the message as needed, but is expected to ultimately reply to the message by an ACK or NACK message. This reply is used by the AMQP Mediator for two things: First, to commit the Alpakka message (or not, in the case of a NACK). Second, to pull the next message, thus enabling back pressure accross actor boundaries.

The code in this example purposefully throws exceptions every now and then in order to showcase the retry mechanism of the AMQP broker: Messages that are consumed but NACKed are eventually resent.

The demo detects when no messages are processed anymore, and checks that all messages have been processed successfully exactly once.

It's important to note that the Akka Streams flow, as configured in this example, serialises processing of all messages. Use mapAsyncUnordered or similar to paralellise processing as needed.
