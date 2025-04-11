const amqp = require("amqplib");
const webPush = require("web-push");
const { Client } = require("pg");
require("dotenv").config();

const VAPID_PUBLIC_KEY = process.env.VAPID_PUBLIC_KEY;
const VAPID_PRIVATE_KEY = process.env.VAPID_PRIVATE_KEY;

if (!VAPID_PUBLIC_KEY || !VAPID_PRIVATE_KEY) {
  console.error("VAPID keys are not set in environment variables");
  process.exit(1);
}

console.log("VAPID Public Key:", VAPID_PUBLIC_KEY);

webPush.setVapidDetails(
  "mailto:example@yourdomain.com",
  VAPID_PUBLIC_KEY,
  VAPID_PRIVATE_KEY
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
  process.exit(1);
});

function isValidSubscription(subscription) {
  return (
    subscription &&
    typeof subscription === "object" &&
    typeof subscription.endpoint === "string" &&
    subscription.endpoint.length > 0 &&
    subscription.keys &&
    typeof subscription.keys === "object" &&
    typeof subscription.keys.p256dh === "string" &&
    typeof subscription.keys.auth === "string"
  );
}

async function fetchUserSubscription(userId) {
  try {
    const res = await client.query(
      "SELECT id, subscription FROM webpush_notifications_subscriptions WHERE user_id = $1",
      [userId]
    );
    if (res.rows.length > 0 && isValidSubscription(res.rows[0].subscription)) {
      return res.rows[0];
    }
    return null;
  } catch (error) {
    console.error("Error fetching subscription from database:", error);
    return null;
  }
}

async function fetchAllSubscriptions() {
  try {
    const res = await client.query(
      "SELECT id, subscription FROM webpush_notifications_subscriptions"
    );
    // Filtrar apenas subscrições válidas
    return res.rows.filter((row) => isValidSubscription(row.subscription));
  } catch (error) {
    console.error("Error fetching all subscriptions from database:", error);
    return [];
  }
}

async function deleteSubscription(subscriptionId) {
  try {
    await client.query(
      "DELETE FROM webpush_notifications_subscriptions WHERE id = $1",
      [subscriptionId]
    );
    console.log(`Subscription ${subscriptionId} deleted successfully`);
    return true;
  } catch (error) {
    console.error(`Error deleting subscription ${subscriptionId}:`, error);
    return false;
  }
}

async function sendWebPush(subscriptionData, message) {
  if (!isValidSubscription(subscriptionData.subscription)) {
    console.log(
      `Invalid subscription format for ID ${subscriptionData.id}, deleting...`
    );
    await deleteSubscription(subscriptionData.id);
    return false;
  }

  try {
    await webPush.sendNotification(
      subscriptionData.subscription,
      JSON.stringify(message)
    );
    console.log(
      `Notification sent successfully to subscription ${subscriptionData.id}`
    );
    return true;
  } catch (error) {
    if (error.statusCode === 410 || error.statusCode === 404) {
      console.log(
        `Subscription ${subscriptionData.id} has expired or been unsubscribed`
      );
      await deleteSubscription(subscriptionData.id);
    } else if (error.statusCode === 401) {
      console.error(
        `VAPID authentication error for subscription ${subscriptionData.id}:`,
        error.body
      );
    } else {
      console.error(
        `Error sending notification to subscription ${subscriptionData.id}:`,
        error
      );
    }
    return false;
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

        const { recipient, payload, type } = message;

        if (recipient === null) {
          // Handle BROADCAST messages
          console.log("Processing BROADCAST message");
          const subscriptions = await fetchAllSubscriptions();
          console.log(`Sending to ${subscriptions.length} subscribers`);

          const results = await Promise.allSettled(
            subscriptions.map((subscription) =>
              sendWebPush(subscription, payload)
            )
          );

          const successCount = results.filter(
            (result) => result.status === "fulfilled" && result.value === true
          ).length;

          console.log(
            `Successfully sent to ${successCount}/${subscriptions.length} subscribers`
          );
        } else {
          // Handle UNICAST messages
          console.log(`Processing UNICAST message for recipient: ${recipient}`);
          const subscription = await fetchUserSubscription(recipient);

          if (subscription) {
            const success = await sendWebPush(subscription, payload);
            if (!success) {
              console.log(
                `Failed to send notification to recipient: ${recipient}`
              );
            }
          } else {
            console.log(`No valid subscription found for userId: ${recipient}`);
          }
        }

        channel.ack(msg);
      }
    });
  } catch (error) {
    console.error("Error in RabbitMQ consumer:", error);
  }
}

consumeMessages();
