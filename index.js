import Fastify from "fastify";
import WebSocket from "ws";
import dotenv from "dotenv";
import fastifyFormBody from "@fastify/formbody";
import fastifyWs from "@fastify/websocket";
import twilio from "twilio";

// Load environment variables from .env file
dotenv.config();

const { 
  ELEVENLABS_AGENT_ID, 
  TWILIO_ACCOUNT_SID, 
  TWILIO_AUTH_TOKEN,
  TWILIO_PHONE_NUMBER,
  PUBLIC_URL
} = process.env;

// Check for required environment variables
if (!ELEVENLABS_AGENT_ID || !TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN || !TWILIO_PHONE_NUMBER) {
  console.error("Missing required environment variables");
  process.exit(1);
}

// Initialize Twilio client
const twilioClient = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

// Initialize Fastify server
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

const PORT = process.env.PORT || 8000;

// Root route for health check
fastify.get("/", async (_, reply) => {
  reply.send({ message: "Server is running" });
});

const sessionData = new Map(); // Store reminders/other by a unique key

// Route to initiate outbound calls
fastify.post("/make-call", async (request, reply) => {
    try {
      const { phoneNumber } = request.body;
      const reminders = request.headers['reminders'] || "No specific reminders.";
      const other = request.headers['other'] || "";
      
      if (!phoneNumber) {
        return reply.code(400).send({ error: "Phone number is required" });
      }

      const sessionId = crypto.randomUUID();
      sessionData.set(sessionId, { reminders, other });
  
      // Use a publicly accessible URL for the WebSocket
      const publicUrl = `wss://${PUBLIC_URL}/media-stream/${sessionId}`; // Replace with your URL
  
      // Generate TwiML for the outbound call
      const twiml = new twilio.twiml.VoiceResponse();
      twiml.connect().stream({ url: publicUrl });
      console.log(`[Server] TwiML: ${twiml.toString()}`);
  
      // Initiate the call
      const call = await twilioClient.calls.create({
        twiml: twiml.toString(),
        to: phoneNumber,
        from: TWILIO_PHONE_NUMBER
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
  });
  
  fastify.register(async (fastifyInstance) => {
    fastifyInstance.get("/media-stream/:sessionId", { websocket: true }, (connection, req) => {
        console.info("[Server] Twilio connected to media stream.");
        const { sessionId } = req.params;

  
        let streamSid = null;

        const { reminders, other } = sessionData.get(sessionId) || { reminders: "No specific reminders.", other: "" };
        console.log(`[Server] Session ID: ${sessionId}, Reminders: ${reminders}, Other: ${other}`);
  
      // Connect to ElevenLabs Conversational AI WebSocket
      const elevenLabsWs = new WebSocket(
        `wss://api.elevenlabs.io/v1/convai/conversation?agent_id=${ELEVENLABS_AGENT_ID}`
      );
  
      // Handle open event for ElevenLabs WebSocket
      elevenLabsWs.on("open", () => {
        console.log("[II] Connected to Conversational AI.");
  
        // Construct the prompt with variables from headers
        const promptText = `You are a friendly and empathetic AI assistant designed to help users by providing reminders and assisting with tasks. Each call will include reminders and, when applicable, additional tasks to complete. Your goal is to ensure the user fully understands the reminders, addresses any questions they may have, and confirms their understanding. If extra tasks are provided, guide the user through completing them with patience and clarity. Maintain a warm, approachable tone, and always prioritize being helpful and reassuring. Reminders: ${reminders} Other: ${other ? other : ''}`;
        console.log(`[II] Prompt: ${promptText}`);

        // Send conversation initiation client data
        const initiationData = {
          type: "conversation_initiation_client_data",
          conversation_config_override: {
            agent: {
              prompt: {
                prompt: promptText
              },
              first_message: "Hi! How are you today?",
            }
          }
        };
  
        elevenLabsWs.send(JSON.stringify(initiationData));
      });

    // Handle messages from ElevenLabs
    elevenLabsWs.on("message", (data) => {
      try {
        const message = JSON.parse(data);
        handleElevenLabsMessage(message, connection);
      } catch (error) {
        console.error("[II] Error parsing message:", error);
      }
    });

    // Handle errors from ElevenLabs WebSocket
    elevenLabsWs.on("error", (error) => {
      console.error("[II] WebSocket error:", error);
    });

    // Handle close event for ElevenLabs WebSocket
    elevenLabsWs.on("close", () => {
      console.log("[II] Disconnected.");
    });

    // Function to handle messages from ElevenLabs
    const handleElevenLabsMessage = (message, connection) => {
      switch (message.type) {
        case "conversation_initiation_metadata":
          console.info("[II] Received conversation initiation metadata.");
          break;
        case "audio":
          if (message.audio_event?.audio_base_64) {
            // Send audio data to Twilio
            const audioData = {
              event: "media",
              streamSid,
              media: {
                payload: message.audio_event.audio_base_64,
              },
            };
            connection.send(JSON.stringify(audioData));
          }
          break;
        case "interruption":
          // Clear Twilio's audio queue
          connection.send(JSON.stringify({ event: "clear", streamSid }));
          break;
        case "ping":
          // Respond to ping events from ElevenLabs
          if (message.ping_event?.event_id) {
            const pongResponse = {
              type: "pong",
              event_id: message.ping_event.event_id,
            };
            elevenLabsWs.send(JSON.stringify(pongResponse));
          }
          break;
      }
    };

    // Handle messages from Twilio
    connection.on("message", async (message) => {
      try {
        const data = JSON.parse(message);
        switch (data.event) {
          case "start":
            // Store Stream SID when stream starts
            streamSid = data.start.streamSid;
            console.log(`[Twilio] Stream started with ID: ${streamSid}`);
            break;
          case "media":
            // Route audio from Twilio to ElevenLabs
            if (elevenLabsWs.readyState === WebSocket.OPEN) {
              // data.media.payload is base64 encoded
              const audioMessage = {
                user_audio_chunk: Buffer.from(
                  data.media.payload,
                  "base64"
                ).toString("base64"),
              };
              elevenLabsWs.send(JSON.stringify(audioMessage));
            }
            break;
          case "stop":
            // Close ElevenLabs WebSocket when Twilio stream stops
            elevenLabsWs.close();
            break;
          default:
            console.log(`[Twilio] Received unhandled event: ${data.event}`);
        }
      } catch (error) {
        console.error("[Twilio] Error processing message:", error);
      }
    });

    // Handle close event from Twilio
    connection.on("close", () => {
      elevenLabsWs.close();
      console.log("[Twilio] Client disconnected");
    });

    // Handle errors from Twilio WebSocket
    connection.on("error", (error) => {
      console.error("[Twilio] WebSocket error:", error);
      elevenLabsWs.close();
    });
  });
});

// Start the Fastify server
fastify.listen({ port: PORT }, (err) => {
  if (err) {
    console.error("Error starting server:", err);
    process.exit(1);
  }
  console.log(`[Server] Listening on port ${PORT}`);
});
