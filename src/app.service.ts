import { HttpService, Scope, Injectable } from '@nestjs/common';
import { Kafka, logLevel  } from 'kafkajs';

const topic = 'new.user-admin-events'

const kafka = new Kafka({
  //logLevel: logLevel.DEBUG,
  brokers: [`localhost:9092`],
  clientId: 'example-producer',
})

@Injectable({ scope: Scope.DEFAULT })
export class AppService {

    constructor(
        private httpService: HttpService,
    ) {
        const consumer = kafka.consumer({ groupId: 'consumerId' })

        const consume = async () => {

            console.log('Consumer has started');
        	// first, we wait for the client to connect and subscribe to the given topic
        	await consumer.connect()
        	await consumer.subscribe({ topic })
        	await consumer.run({

        		// this function is called every time the consumer gets a new message
        		eachMessage: async ({ topic, partition, message }) => {
                  console.log(`MESSAGE - ${message}`)
                },
        	})
        }
        // start the consumer, and log any errors
        consume().catch((err) => {
        	console.error("Error in consumer: ", err)
        })
    }

  async getHello(): Promise<string> {
    const admin = kafka.admin()
    const createTopic = topic => admin.createTopics({ topics: [{ topic }] })

    const producer = kafka.producer()
    const message = {
         "@type": "Platform-User-Added",
         "properties": {
             "user": {
                 "userID": "test_user_hermitage2",
                 "email": "test_user_hermitage2@idbs.com",
                 "externalCollaborator": true,
                 "identityProvider": 'IDBS',
             }
         }
     };

    const run = async () => {
      await producer.connect()
      await producer.send({
      				topic,
      				messages: [
      					{
      						key: String(2),
      						value: JSON.stringify(message),
      					},
      				],
      			});
      console.log('message sent')
    }

    run().catch((err) => {
    	console.error("error in producer: ", err)
    })
    return 'Hello World!';
  }
}
