## Eevee

#### What?

Eevee is a bridge between Kafka and MQTT. This great if you want you IOT devices to also be connected to your firehose of application logs and communication. This is especially awesome now that your IOT sensors can directly interact with your applications. 

The POC is just finished routing between two topics `kafka-in-out` on Kafka and `mqtt-in-out` on MQTT. Sending to one topic will appear in the other. 

I will be rewriting the code so it is actually maintainable with tests, etc. The Kafka topics will map directly to MQTT topics under the `/kafka/` address.  

#### Running Eevee 

Right now it only routes between `kafka-in-out` and `mqtt-in-out` topics on Kafka and MQTT respectively so don't know why you would want to use it now. But if you do, I suggest running Kafka locally and Mosquitto as the MQTT broker

#### Notes 
* Currently hijacking the message id / key header. If your application uses this key, this may pose a problem
* With this becomes distributed, can't store local hashmaps... will fix this later by using Kafka topics to keep track
* This is called Eevee because I needed a cool name for now and Eevee can evolve into different Pokemon. And its pretty cute.