const amqp = require("amqplib");
const webPush = require("web-push");
const { Client } = require("pg");
require("dotenv").config();

webPush.setVapidDetails(
  "mailto:example@yourdomain.com",
  process.env.VAPID_PUBLIC_KEY,
  process.env.VAPID_PRIVATE_KEY
);

const queue = "webpush_queue";
const amqpUrl = process.env.RABBITMQ_URL || "amqp://mq:5672";

const client = new Client({
  user: process.env.POSTGRES_USER,
  host: process.env.POSTGRES_HOST,
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: process.env.POSTGRES_PORT || 5432,
});

client.connect().catch((error) => {
  console.error("Failed to connect to PostgreSQL:", error.message);
  process.exit(1); // Exit the process if the database connection fails
});

async function fetchUserSubscription(userId) {
  try {
    const res = await client.query(
      "SELECT subscription FROM webpush_notifications_subscriptions WHERE user_id = $1",
      [userId]
    );
    if (res.rows.length > 0) {
      return res.rows[0].subscription;
    } else {
      return null;
    }
  } catch (error) {
    console.error("Error fetching subscription from database:", error);
    return null;
  }
}

async function sendWebPush(subscription, message) {
  try {
    // Handle the notification payload based on the message type and payload
    const notificationPayload = {
      notification: {
        title: message.type || "Notification",
        body:
          typeof message.payload === "string"
            ? message.payload
            : message.payload.message,
        data: message,
      },
    };

    await webPush.sendNotification(
      subscription,
      JSON.stringify(notificationPayload)
    );
    console.log("Notification sent successfully:", message);
  } catch (error) {
    console.error("Error sending notification:", error);
  }
}

async function consumeMessages() {
  try {
    const connection = await amqp.connect(amqpUrl);
    const channel = await connection.createChannel();
    await channel.assertQueue(queue, {
      durable: true,
      arguments: {
        "x-max-priority": 10,
        "x-message-ttl": 24 * 60 * 60 * 1000,
      },
    });

    console.log(`Waiting for messages in ${queue}. To exit press CTRL+C`);

    channel.consume(queue, async (msg) => {
      if (msg !== null) {
        const message = JSON.parse(msg.content.toString());
        console.log("Received message:", message);

        const { recipient, type, payload } = message;

        const subscription = await fetchUserSubscription(recipient);

        if (subscription) {
          await sendWebPush(subscription, message);
        } else {
          console.log(`No subscription found for userId: ${recipient}`);
        }

        channel.ack(msg);
      }
    });
  } catch (error) {
    console.error("Error in RabbitMQ consumer:", error);
  }
}

consumeMessages();
