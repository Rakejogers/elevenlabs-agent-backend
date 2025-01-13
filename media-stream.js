import Fastify from "fastify";
import WebSocket from "ws";
import dotenv from "dotenv";
import fastifyFormBody from "@fastify/formbody";
import fastifyWs from "@fastify/websocket";
import { createClient } from '@supabase/supabase-js';
import fetch from 'node-fetch';

// Load environment variables from .env file
dotenv.config();

const {
  ELEVENLABS_AGENT_ID,
  NEXT_PUBLIC_SUPABASE_URL,
  SUPABASE_SERVICE_ROLE_KEY,
  ELEVENLABS_API_KEY,
} = process.env;

if (!ELEVENLABS_AGENT_ID || !NEXT_PUBLIC_SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !ELEVENLABS_API_KEY) {
  console.error("Missing required environment variables");
  process.exit(1);
}

const supabase = createClient(NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const fastify = Fastify();

const SYSTEM_MESSAGE = `Your knowledge cutoff is 2023-10. You are a friendly, empathetic, and patient AI assistant designed to help users by providing reminders and assisting with tasks over a phone call.

Call Structure

Greeting and Offer: Begin each call by greeting the user warmly. Politely give their reminders after they have responded.
Reminders: Present each reminder clearly, one by one. If there are multiple reminders, confirm each separately. Do not repeat the same reminder unless the user specifically requests it or indicates they haven't acknowledged.
Tasks: If extra tasks are provided, guide the user through each step calmly and with patience. Always pause and wait for the user to finish speaking before responding.
Tone: Maintain a warm, approachable, and reassuring manner throughout the call. Answer questions thoroughly, ensuring the user fully understands.
Closing: If the user indicates they want to end the call, do not extend the conversation. Thank them if appropriate, and end the call politely and promptly.
Goal: Confirm that the user understands all reminders or tasks, address any questions, and provide reassurance as needed. Prioritize clarity, empathy, and respect for the user's time.`;

fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

const PORT = 3001;
const sessionData = new Map();

// Root route for health check
fastify.get("/", async (_, reply) => {
  reply.code(200).send({ message: "Media Stream Server is running" });
});

fastify.register(async (fastifyInstance) => {
  fastifyInstance.get("/media-stream/:sessionId", { websocket: true }, (connection, req) => {
    console.info("[Server] Twilio connected to media stream.");
    const { sessionId } = req.params;

    let currentConversationId = null;
    let streamSid = null;

    const { reminders, other, scheduledCallId, userId, phoneNumber } = sessionData.get(sessionId) || { reminders: "No specific reminders today. Ask how they are.", other: "" };

    // Connect to ElevenLabs Conversational AI WebSocket
    const elevenLabsWs = new WebSocket(
      `wss://api.elevenlabs.io/v1/convai/conversation?agent_id=${ELEVENLABS_AGENT_ID}`
    );

    // Handle open event for ElevenLabs WebSocket
    elevenLabsWs.on("open", () => {
      console.log("[II] Connected to Conversational AI.");

      const promptText = `${SYSTEM_MESSAGE} Reminders: ${reminders} Other: ${other ? other : ''}`;

      // Send conversation initiation client data
      const initiationData = {
        type: "conversation_initiation_client_data",
        conversation_config_override: {
          agent: {
            prompt: {
              prompt: promptText
            },
            first_message: "Hi! How are you today? I've got some reminders for you.",
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
          currentConversationId = message.conversation_initiation_metadata_event.conversation_id;
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

    // Function to poll conversation data
    async function pollConversationData(conversationId) {
      const url = `https://api.elevenlabs.io/v1/convai/conversations/${conversationId}`;
      const options = { method: 'GET', headers: { 'xi-api-key': ELEVENLABS_API_KEY } };
    
      while (true) {
        try {
          const response = await fetch(url, options);
          const data = await response.json();
        
          // Check if the call is no longer processing
          if (data.analysis && data.status !== "processing") {
            return data;
          }
        
          // Wait for a specified interval before the next fetch
          await new Promise(resolve => setTimeout(resolve, 5000)); // 5 seconds
        } catch (error) {
          console.error("Error fetching conversation data:", error);
          break;
        }
      }
    }

    // Handle close event from Twilio
    connection.on("close", async () => {
      elevenLabsWs.close();
      console.log("[Twilio] Client disconnected");
    
      try {
        const data = await pollConversationData(currentConversationId);
      
        // Record call history in Supabase
        const { error: insertError } = await supabase
          .from('call_history')
          .insert({
            scheduled_call_id: scheduledCallId,
            user_id: userId,
            transcript: data.transcript,
            call_status: data.analysis.call_successful,
            phone_number: phoneNumber,
            call_start_time: new Date(data.metadata.start_time_unix_secs * 1000),
            call_end_time: new Date(),
            call_duration: data.metadata.call_duration_secs,
            summary: data.analysis.transcript_summary,
          });
        
        if (insertError) {
          console.error('Error inserting call history:', insertError);
        } else {
          console.log('Call history recorded successfully.');
        }
      } catch (error) {
        console.error("Error during polling:", error);
      }
    });

    // Handle errors from Twilio WebSocket
    connection.on("error", (error) => {
      console.error("[Twilio] WebSocket error:", error);
      elevenLabsWs.close();
    });
  });
});

// Add session data endpoint
fastify.post("/add-session/:sessionId", async (request, reply) => {
  const { sessionId } = request.params;
  const sessionInfo = request.body;

  try {
    sessionData.set(sessionId, sessionInfo);
    reply.code(200).send({ message: "Session data added successfully" });
  } catch (error) {
    console.error("Error adding session data:", error);
    reply.code(500).send({ error: "Failed to add session data" });
  }
});

// Export the sessionData map and addSession function for external use
export const addSession = (sessionId, data) => {
  sessionData.set(sessionId, data);
};

// Start the Fastify server
fastify.listen({ port: PORT, host: "0.0.0.0" }, (err) => {
  if (err) {
    console.error("Error starting server:", err);
    process.exit(1);
  }
  console.log(`[Server] Media Stream Server listening on port ${PORT}`);
});