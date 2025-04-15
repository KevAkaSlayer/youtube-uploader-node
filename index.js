require('dotenv').config();
const express = require('express');
const { google } = require('googleapis');
const { OAuth2Client } = require('google-auth-library');
const { MongoClient } = require('mongodb');
const { S3Client, PutObjectCommand, DeleteObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');
const os = require('os');
const axios = require('axios');
const stream = require('stream');
const util = require('util');

const pipeline = util.promisify(stream.pipeline);

const app = express();
const port = process.env.PORT || 8000;

app.use(express.json());

// ----- MongoDB Setup -----
let db;
MongoClient.connect(process.env.MONGO_URI)
  .then(client => {
    db = client.db('youtube_uploader');
    console.log('Connected to MongoDB');
  })
  .catch(err => {
    console.error('MongoDB connection error:', err);
  });

// ----- AWS S3 Setup -----
const s3 = new S3Client({
  region: 'auto',
  endpoint: process.env.R2_ENDPOINT_URL,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

// ----- Google OAuth2 Client Setup -----
const SCOPES = [
  'openid',
  'https://www.googleapis.com/auth/userinfo.email',
  'https://www.googleapis.com/auth/userinfo.profile',
  'https://www.googleapis.com/auth/youtube.upload',
];

const oauth2Client = new OAuth2Client(
  process.env.GOOGLE_CLIENT_ID,
  process.env.GOOGLE_CLIENT_SECRET,
  process.env.GOOGLE_REDIRECT_URI
);

// ----- Routes -----
app.get('/auth/login', (req, res) => {
  const state = uuidv4();
  const authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
    prompt: 'consent',
    state: state,
  });
  res.redirect(authUrl);
});

app.get('/auth/callback', async (req, res) => {
  try {
    const { code } = req.query;
    const { tokens } = await oauth2Client.getToken(code);
    oauth2Client.setCredentials(tokens);

    const ticket = await oauth2Client.verifyIdToken({
      idToken: tokens.id_token,
      audience: process.env.GOOGLE_CLIENT_ID,
    });
    const payload = ticket.getPayload();
    const googleSub = payload.sub;
    const googleEmail = payload.email;

    const users = db.collection('tokens');
    await users.updateOne(
      { sub: googleSub },
      {
        $set: {
          email: googleEmail,
          access_token: tokens.access_token,
          refresh_token: tokens.refresh_token,
          token_expiry: tokens.expiry_date,
        },
      },
      { upsert: true }
    );
    res.json({ message: 'Authentication successful', userId: googleSub });
  } catch (err) {
    console.error('Auth callback error:', err);
    res.status(500).json({ error: 'Authentication failed' });
  }
});

const downloadFromR2ToTemp = async (objectKey) => {
  const params = {
    Bucket: process.env.R2_BUCKET_NAME,
    Key: objectKey,
  };

  const tempFilePath = path.join(os.tmpdir(), objectKey);
  try {
    const { Body } = await s3.send(new GetObjectCommand(params));
    await pipeline(
      Body,
      fs.createWriteStream(tempFilePath)
    );
    return tempFilePath;
  } catch (err) {
    fs.unlink(tempFilePath, () => {});
    throw err;
  }
};

app.post('/upload', async (req, res) => {
  try {
    const { video_url, title, description, tags, category_id, privacy_status, publish_at } = req.body;
    const userId = req.query.userId;
    
    if (!userId) return res.status(401).json({ error: 'User not authenticated' });
    if (!video_url) return res.status(400).json({ error: 'Video URL is required' });

    // Fetch user credentials
    const users = db.collection('tokens');
    const userData = await users.findOne({ sub: userId });
    if (!userData) return res.status(401).json({ error: 'User not found' });

    // Setup YouTube API client
    const userAuth = new OAuth2Client(
      process.env.GOOGLE_CLIENT_ID,
      process.env.GOOGLE_CLIENT_SECRET,
      process.env.GOOGLE_REDIRECT_URI
    );
    userAuth.setCredentials({
      access_token: userData.access_token,
      refresh_token: userData.refresh_token,
      expiry_date: userData.token_expiry,
    });
    const youtube = google.youtube({ version: 'v3', auth: userAuth });

    // Download video from URL
    const response = await axios.get(video_url, { responseType: 'stream' });
    const contentLength = response.headers['content-length'];
    if (!contentLength) throw new Error('Content-Length header missing from video source');

    // Upload to R2
    const objectName = `video_${Date.now()}_${uuidv4().slice(0, 8)}.mp4`;
    await s3.send(new PutObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME,
      Key: objectName,
      Body: response.data,
      ContentLength: parseInt(contentLength, 10),
    }));

    // Process YouTube upload
    const tempFilePath = await downloadFromR2ToTemp(objectName);
    const media = { body: fs.createReadStream(tempFilePath) };

    const youtubeResponse = await youtube.videos.insert({
      part: 'snippet,status',
      requestBody: {
        snippet: {
          title,
          description,
          tags,
          categoryId: category_id,
        },
        status: {
          privacyStatus: privacy_status,
          ...(publish_at && { publishAt: publish_at }),
        },
      },
      media: media,
    });

    // Cleanup
    await s3.send(new DeleteObjectCommand({
      Bucket: process.env.R2_BUCKET_NAME,
      Key: objectName,
    }));
    fs.unlink(tempFilePath, (err) => {
      if (err) console.error('Error deleting temp file:', err);
    });

    res.json({ 
      videoId: youtubeResponse.data.id, 
      message: 'Video uploaded successfully',
      url: `https://youtu.be/${youtubeResponse.data.id}`
    });

  } catch (err) {
    console.error('Upload error:', err);
    res.status(500).json({ 
      error: err.message,
      ...(err.response?.data && { details: err.response.data })
    });
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});