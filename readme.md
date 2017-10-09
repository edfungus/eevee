## Eevee

#### What?

Eevee is a bridge between Kafka and MQTT. This great if you want you IOT devices to also be connected to your firehose of application logs and communication. This is especially awesome now that your IOT sensors can directly interact with your applications. 

#### How does it work?

The payload is pass from one service (Kafka, MQTT, etc) to another. Eevee does not care what format your message is in. Because Eevee is subscribing and publishing to the same topics, it needs the `MessageID` to identify whether or not the message has been seen. Therefore, you need to implement the `Translator` so the `MessageID` can be pull out from your message. The `Translator` also serves an important purpose which is to convert the payload from one service to another. It is suggested that the payload are the same between serves to reduce complexity.

#### Running Eevee 

A simple example has been written under the examples directory. To use, Kafka and MQTT implements for `Connection` is given. You will need to write your own `Translator` base on the payload you are transferring and a `IDStore`. The `MessageID` is an `interface{}` is up to you to keep track of its type throughout your implementation. Note that the `MessageID` implementation should be shared between the `Translator` and the `IDStore`

#### Todo
* Tests plz
* A better key store?
* Worker?
* New source/sink?

#### Notes 
* The simple example is extremely naive. It cannot be used in a distributed or a multi-threaded fashion
* This is called Eevee because I needed a cool name for now and Eevee can evolve into different Pokemon. And its pretty cute.