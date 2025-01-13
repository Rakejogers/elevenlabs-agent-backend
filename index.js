import Fastify from "fastify";
import WebSocket from "ws";
import dotenv from "dotenv";
import fastifyFormBody from "@fastify/formbody";
import fastifyWs from "@fastify/websocket";
import twilio from "twilio";
import { createClient } from '@supabase/supabase-js';
import crypto from 'crypto';

// Load environment variables from .env file
dotenv.config();

const {
  TWILIO_ACCOUNT_SID,
  TWILIO_AUTH_TOKEN,
  TWILIO_PHONE_NUMBER,
  MEDIA_PUBLIC_URL,
  NEXT_PUBLIC_SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  WORKER_API_KEY,
  INDEX_PUBLIC_URL
} = process.env;

if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN || !TWILIO_PHONE_NUMBER || !NEXT_PUBLIC_SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !WORKER_API_KEY) {
  console.error("Missing required environment variables");
  process.exit(1);
}

const supabase = createClient(NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
const fastify = Fastify();

fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

const PORT = 3000;

// Authorization Middleware
fastify.addHook("preHandler", async (request, reply) => {
  // Skip authorization for status-callback route
  if (request.url === '/status-callback') {
    return;
  }
  
  try {
    const authToken = request.headers['authorization'];
    
    // Check for valid auth token
    if (!authToken || authToken !== `Bearer ${WORKER_API_KEY}`) {
      console.error(`Unauthorized access attempt: Invalid or missing auth token`);
      return reply.code(401).send({ error: "Unauthorized" });
    }
  } catch (error) {
    console.error("Error in authorization:", error);
    return reply.code(500).send({ error: "Internal server error during authorization" });
  }
});

// Root route for health check
fastify.get("/", async (_, reply) => {
  reply.code(200).send({ message: "Server is running" });
});

const callQueue = [];
let isProcessingQueue = false;

async function processQueue() {
  if (isProcessingQueue || callQueue.length === 0) return;

  isProcessingQueue = true;
  const { request, reply } = callQueue.shift();

  try {
    const { phoneNumber } = request.body;
    const scheduledCallId = request.headers['scheduled-call-id'] || null;
    const userId = request.headers['user-id'] || null;
    const reminders = request.headers['reminders'] || "No specific reminders.";
    const other = request.headers['other'] || "";

    console.log(`[Server] Processing call for user ID: ${userId}, scheduled call ID: ${scheduledCallId}, phone number: ${phoneNumber}`);

    if (!phoneNumber) {
      reply.code(400).send({ error: "Phone number is required" });
      isProcessingQueue = false;
      processQueue();
      return;
    }

    // Check if the scheduled call exists and userId matches
    const { data: scheduledCall, error: fetchError } = await supabase
      .from('scheduled_calls')
      .select('*')
      .eq('id', scheduledCallId)
      .eq('user_id', userId);

    if (fetchError || !scheduledCall) {
      reply.code(404).send({ error: "Scheduled call not found or user ID mismatch" });
      isProcessingQueue = false;
      processQueue();
      return;
    }

    const sessionId = crypto.randomUUID();

    // Use a publicly accessible URL for the WebSocket
    const publicUrl = `wss://${PUBLIC_URL}/media-stream/${sessionId}`;

    // Generate TwiML for the outbound call
    const twiml = new twilio.twiml.VoiceResponse();
    twiml.connect().stream({ url: publicUrl });
    console.log(`[Server] TwiML: ${twiml.toString()}`);

    // Initiate the call
    const call = await twilioClient.calls.create({
      twiml: twiml.toString(),
      to: phoneNumber,
      from: TWILIO_PHONE_NUMBER,
      statusCallback: `https://${PUBLIC_URL}/status-callback`, // Add your status callback URL
      statusCallbackEvent: ['completed'] // Specify the events you want to receive
    });

    // Send session data to media stream server
    const mediaStreamUrl = `http://localhost:3001/add-session/${sessionId}`;
    await fetch(mediaStreamUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        scheduledCallId,
        userId,
        reminders,
        other,
        phoneNumber
      })
    });

    reply.send({
      success: true,
      callSid: call.sid,
      status: call.status,
      sessionId
    });

  } catch (error) {
    console.error("Error making call:", error);
    reply.code(500).send({ error: error.message });
  }
}

fastify.post("/make-call", async (request, reply) => {
  try {
    const { phoneNumber } = request.body;
    const scheduledCallId = request.headers['scheduled-call-id'] || null;
    const userId = request.headers['user-id'] || null;
    const reminders = request.headers['reminders'] || "No specific reminders.";
    const other = request.headers['other'] || "";

    console.log(`[Server] Processing call for user ID: ${userId}, scheduled call ID: ${scheduledCallId}, phone number: ${phoneNumber}`);

    if (!phoneNumber) {
      return reply.code(400).send({ error: "Phone number is required" });
    }

    // Check if the scheduled call exists and userId matches
    const { data: scheduledCall, error: fetchError } = await supabase
      .from('scheduled_calls')
      .select('*')
      .eq('id', scheduledCallId)
      .eq('user_id', userId);

    if (fetchError || !scheduledCall || scheduledCall.length === 0) {
      console.error("Scheduled call error:", fetchError || "Call not found");
      return reply.code(404).send({ error: "Scheduled call not found or user ID mismatch" });
    }

    const sessionId = crypto.randomUUID();
    const publicUrl = `wss://${MEDIA_PUBLIC_URL}/media-stream/${sessionId}`;

    // Generate TwiML for the outbound call
    const twiml = new twilio.twiml.VoiceResponse();
    twiml.connect().stream({ url: publicUrl });
    console.log(`[Server] TwiML: ${twiml.toString()}`);

    // Initiate the call
    const call = await twilioClient.calls.create({
      twiml: twiml.toString(),
      to: phoneNumber,
      from: TWILIO_PHONE_NUMBER,
      statusCallback: `https://${INDEX_PUBLIC_URL}/status-callback`,
      statusCallbackEvent: ['completed']
    });

    console.log(`[Server] Call initiated with SID: ${call.sid}`);

    // Send session data to media stream server
    const mediaStreamUrl = `http://localhost:3001/add-session/${sessionId}`;
    await fetch(mediaStreamUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        scheduledCallId,
        userId,
        reminders,
        other,
        phoneNumber
      })
    });

    return reply.send({
      success: true,
      callSid: call.sid,
      status: call.status,
      sessionId
    });

  } catch (error) {
    console.error("[Server] Error making call:", error);
    return reply.code(500).send({ error: error.message });
  }
});

// Create a WebSocket server
const wss = new WebSocket.Server({ 
  port: 8080,
  verifyClient: (info, callback) => {
    const apiKey = info.req.headers['x-api-key'];
    if (!apiKey || apiKey !== WORKER_API_KEY) {
      callback(false, 401, 'Unauthorized');
      return;
    }
    callback(true);
  }
});

wss.on('connection', (ws) => {
  console.log('Worker connected to WebSocket server');
  
  ws.on('message', (message) => {
    console.log('Received message from worker:', message);
  });
});

// Notify worker when call is completed
fastify.post("/status-callback", async (request, reply) => {
  const accountsid = request.body.AccountSid;
  const callStatus = request.body.CallStatus;
  const callSid = request.body.CallSid;

  console.log(`Call SID: ${accountsid}, Status: ${callStatus}`);

  if(!accountsid || !callStatus) {
    console.error("Invalid status callback request");
    return reply.code(400).send({ error: "Invalid status callback request" });
  }

  if(accountsid != TWILIO_ACCOUNT_SID) {
    console.error("Invalid Account SID in status callback request");
    return reply.code(400).send({ error: "Invalid Account SID in status callback request" });
  }

  if (callStatus === 'completed') {
    // Notify all connected workers
    wss.clients.forEach(client => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify({ callSid, status: 'completed' }));
      }
    });

    console.log("[Server] Call completed. Processing next. Notifying workers...");
    isProcessingQueue = false;
    processQueue();
  }

  reply.code(200).send();
});

// Start the Fastify server
fastify.listen({ port: PORT, host: "0.0.0.0" }, (err) => {
  if (err) {
    console.error("Error starting server:", err);
    process.exit(1);
  }
  console.log(`[Server] Listening on port ${PORT}`);
});
