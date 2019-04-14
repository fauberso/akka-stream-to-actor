# Akka Stateful Microservices Example

Thill _will_ be an example of a Stateful, distributed, scalable microservice that is accessible through a REST JSON interface.


## Operations

__Create a booking__

Post the following to `http://localhost:8080/bookings`:

```
{
    "flight": "LX123",
    "seat" : "24F",
    "name": "aubersonfmr"
}
```

For example using curl:

```
curl -H "Content-Type: application/json" -X POST -d '{"flight":"LX223", "seat":"24F", "name":"aubersonfmr"}' http://localhost:8080/bookings
```

The microservice will respond with HTTP 200 and the payload "`{"description":"Booking LX223 created."}`"

A special feature of the booking class is that it will accept arbitrary additional properties, and return them when queried. For example:

```
{
    "flight": "LX123",
    "seat" : "24F",
    "name": "aubersonfmr",
    "meal": "low-carb"
}
```

Will add a "meal" field to that particular booking.

__Query all bookings__

Execute a Get operation (e.g. using a browser) on `http://localhost:8080/bookings`. A JSON collections of all bookings will be returned.

__Query a specific booking__

Execute a Get operation on a specific booking, e.g. `http://localhost:8080/bookings/LX123`. A JSON representation of the booking will be returned.

__Delete a booking__

Execute a Delete operation on a booking, e.g. `http://localhost:8080/bookings/LX123`. 
The microservice will respond with HTTP 200 and the payload "`{"description":"Booking LX123 deleted."}`"


