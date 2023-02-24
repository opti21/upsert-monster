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

app.use(express.json({ limit: '50mb' }));

app.get('/', (req, res) => {
    res.send('UpsertMonster is running!');
});

app.post('/createJob', async (req, res) => {
    console.log("CREATE JOB")
    console.log(req.body)
    if (!req.body.channelId || !req.body.videos || !req.body.date) {
      console.log("INVALID REQUEST", req)
      return res.status(400).end()
    }

    await upsertQueue.add(`upsertVideos-${req.body.channelId}-${req.body.date}`, {
        channelId: req.body.channelId,
        videos: req.body.videos
    })

    return res.status(200).end()
});

app.get('/getProgress', async (req, res) => {
    console.log("GET PROGRESS")
    console.log(req.query)
    if (!req.query.jobId) {
      console.log("INVALID REQUEST", req)
      return res.status(400).end()
    }

    const jobs = await upsertQueue.getJobs(['waiting', 'active', 'delayed', 'completed', 'failed', 'paused'], 0, 1000)

    const job: Job | undefined = jobs.find((j: Job) => j.name.startsWith(`upsertVideos-${req.query.jobId}`))

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
    console.log("Upserting videos for channel: " + job.data.channelId )
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

   
   
   
   
   
   
   
