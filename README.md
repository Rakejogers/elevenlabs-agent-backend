# ElevenLabs Agent Calling Platform

A Node.js backend service that enables making AI-powered phone calls using ElevenLabs for voice generation and Twilio for call handling.

## Setup

1. Install dependencies:
   ```bash
   npm install
   ```

2. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Fill in your Twilio and ElevenLabs credentials:
     - `TWILIO_ACCOUNT_SID`: Your Twilio Account SID
     - `TWILIO_AUTH_TOKEN`: Your Twilio Auth Token
     - `TWILIO_PHONE_NUMBER`: Your Twilio Phone Number
     - `ELEVENLABS_API_KEY`: Your ElevenLabs API Key

3. Start the server:
   ```bash
   node index.js
   ```

## API Endpoints

### Make a call
`POST /api/make-call`

Request body:
```json
{
  "phoneNumber": "+1234567890",
  reminders: "",
  other: ""
}
```

Response:
```json
{
  "success": true,
  "callSid": "CAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
  "status": "queued",
  "sessionId": "CAXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
}
```

## Notes
- The service uses ElevenLabs' Conversational Agent API to generate natural-sounding speech-to-speech interaction
- All phone numbers should be in E.164 format (e.g., +1234567890) 