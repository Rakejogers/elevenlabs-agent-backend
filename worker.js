import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import WebSocket from 'ws';

dotenv.config();

const { NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, WORKER_API_KEY } = process.env;

if (!NEXT_PUBLIC_SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !WORKER_API_KEY) {
  console.error("Missing required environment variables");
  process.exit(1);
}

const supabase = createClient(NEXT_PUBLIC_SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

let isProcessingTask = false;

// Fix the WebSocket reconnection logic
let ws;

function connectWebSocket() {
  ws = new WebSocket('ws://localhost:8080', {
    headers: {
      'x-api-key': process.env.WORKER_API_KEY
    }
  });

  ws.on('open', () => {
    console.log('Connected to WebSocket server');
    // Start processing the queue when the worker starts
    if (!isProcessingTask) {
      processQueue();
    }
  });

  ws.on('message', (data) => {
    const message = JSON.parse(data);
    if (message.status === 'completed') {
      console.log(`Received completion notification for call SID: ${message.callSid}`);
      
      // Reset processing flag
      isProcessingTask = false;
      // Process next task with a small delay to ensure proper state reset
      setTimeout(processQueue, 1000);
    }
  });

  ws.on('close', () => {
    console.log('WebSocket connection closed. Attempting to reconnect...');
    setTimeout(connectWebSocket, 5000);
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
}

// Initial connection
connectWebSocket();

async function processQueue() {
  if (isProcessingTask) {
    return;
  }

  try {
    // Fetch the next item from the queue
    const { data: task, error: queueError } = await supabase.schema('pgmq_public').rpc('pop', { queue_name: 'call_queue' });

    if (queueError) {
      console.error('Error fetching task:', queueError);
      // Try again after 5 seconds if there's an error
      setTimeout(processQueue, 5000);
      return;
    }

    if (task.length === 0) {
      console.log('Queue is empty, checking again in 5 seconds...');
      setTimeout(processQueue, 5000);
      return;
    }

    console.log('Processing task:', task);
    isProcessingTask = true;

    // Process the task
    await handleTask(task);

    console.log('Task initiated, waiting for completion...');
  } catch (err) {
    console.error('Error in processing task:', err);
    isProcessingTask = false;
  }
}

async function handleTask(task) {
  console.log('Handling task:', task);

  const { message } = task[0];
  const { phoneNumber, scheduledCallId, userId, reminders, other } = message;

  try {
    const response = await fetch('http://localhost:3000/make-call', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'scheduled-call-id': scheduledCallId,
        'user-id': userId,
        'reminders': reminders,
        'other': other,
        'authorization': `Bearer ${WORKER_API_KEY}`,
      },
      body: JSON.stringify({ phoneNumber })
    });

    if (!response.ok) {
      throw new Error(`Failed to initiate call: ${response.statusText}`);
    }

    const data = await response.json();
    console.log('Call initiated successfully:', data);
  } catch (error) {
    console.error('Error initiating call:', error);
    isProcessingTask = false;
    setTimeout(processQueue, 5000);
  }
}
