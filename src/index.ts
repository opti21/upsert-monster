require('dotenv').config()
import express from 'express';
import axios from 'axios';
import { Job, Queue, QueueEvents, Worker } from 'bullmq';
import { PrismaClient } from '@prisma/client'

const prisma = new PrismaClient()

const app = express();

const redisConnection = {
    host: process.env.REDISHOST,
    port: Number(process.env.REDISPORT),
    password: process.env.REDISPASSWORD
};

const upsertQueue = new Queue('UpsertVideos', {
    connection: redisConnection
});

app.use(express.json({ limit: '500mb' }));

app.get('/', (req, res) => {
    res.send('UpsertMonster is running!');
});

app.post('/createJob', async (req, res) => {
    console.log("CREATE JOB")
    if (!req.body.jobId) {
      console.log("INVALID REQUEST", req)
      return res.status(400).end()
    }

    await upsertQueue.add('upsertVideos-' + req.body.jobId, {
        videos: req.body.videos
    })

    return res.status(200).end()
});

app.get('/getProgress', async (req, res) => {
    console.log("GET PROGRESS")
    if (!req.query.jobId) {
      console.log("INVALID REQUEST", req)
      return res.status(400).end()
    }

    const job = await upsertQueue.getJob(req.query.jobId as string) 

    if (!job) {
      return res.status(200).json({progress: 100})
    }

    console.log("return progress", job.progress)

    return res.status(200).json({progress: job.progress})
});

const queueEvents = new QueueEvents('UpsertVideos', {
    connection: redisConnection
});

new Worker('UpsertVideos', async job => {
  if (job.name.startsWith('upsertVideos-')) {
    const videos = job.data.videos

    for(let i = 0; i < videos.length; i++) {
     const v = videos[i];

     await prisma.video.upsert({
         where: {id: v.id as string},
         update: {
             channelId: v.channelId as string,
             snippet: v.snippet as any,
             status: v.status as any,
         },
         create: {
             id: v.id as string,
             channelId: v.channelId as string,
             snippet: v.snippet as any,
             status: v.status as any,
         }
     }).catch((e: Error) => {
         console.log(`Error upserting video ${v.id}`, e)
     })

     // calculate progress in percent
      const progress = Math.round((i / videos.length) * 100);
      job.updateProgress(progress);
    }
    job.updateProgress(100);

  }
}, {
    connection: redisConnection
});


// Report Queue Event Listeners
upsertQueue.on('waiting', (jobID) => {
  console.info(`[ADDED] Job added with job ID ${jobID}`);
});
queueEvents.on('active', (job) => {
  console.info(`[STARTED] Job ID ${job.jobId} has been started`);
});
queueEvents.on('completed', (job) => {
  console.info(`[COMPLETED] Job ID ${job.jobId} has been completed`);
});
queueEvents.on('failed', (job) => {
  console.error(`[FAILED] Job ID ${job.jobId} has been failed`);
});
queueEvents.on('error', (job) => {
  console.error(`[ERROR] An error occurred by the queue, got ${job}`);
});
queueEvents.on('cleaned', function () {
  console.info(`[CLEANED] Report queue has been cleaned`);
});
queueEvents.on('drained', function () {
  console.info(`[WAITING] Waiting for jobs...`);
});
queueEvents.on('progress', ({jobId, data}) => {
  console.info(`[PROGRESS] Job ID ${jobId} is ${data}% complete`);
});

app.listen(process.env.PORT || 3001, () => {
    console.log('UpsertMonster is running on port: ' + (process.env.PORT || 3001));
})

   
   
   
   
   
   
   
