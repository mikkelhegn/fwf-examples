import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import * as Variables from "@spinframework/spin-variables";
import { AutoRouter as Router } from "itty-router";

const BATCH_LOG_BYTES = 1024 * 1024; // emit one log line per ~1MB of chunks

let router = Router();

router.get('/stream/:bucket/:file', async ({ bucket, file }) => {

    let region = Variables.get("region")!;
    let client = new S3Client({
        region: region,
        endpoint: Variables.get("endpoint")!,
        credentials: {
            accessKeyId: Variables.get("access_key_id")!,
            secretAccessKey: Variables.get("secret_access_key")!,
        },
    });

    let params = {
        Bucket: bucket,
        Key: file,
    };

    let command = new GetObjectCommand(params);
    let data = await client.send(command);

    let stream = data.Body as ReadableStream;


    let headers = new Headers();
    headers.set('Content-Type', data.ContentType || 'application/octet-stream');
    if (data.ContentLength) headers.set('Content-Length', data.ContentLength.toString());

    // Chunks are grouped into batches of >= 1MB. Received and sent use the same
    // chunk boundaries, so batch N covers the same bytes in both log streams.
    const batchLogger = (label: string) => {
        let batchIndex = 0;
        let batchChunks = 0;
        let batchBytes = 0;
        let totalBytes = 0;
        const emit = () => {
            console.log(`[batch #${batchIndex}] ${label}: ${batchChunks} chunks, ${batchBytes} bytes in batch, ${totalBytes} bytes cumulative`);
            batchIndex++;
            batchChunks = 0;
            batchBytes = 0;
        };
        return {
            add(size: number) {
                batchChunks++;
                batchBytes += size;
                totalBytes += size;
                if (batchBytes >= BATCH_LOG_BYTES) emit();
            },
            // Emit any partial batch left at the end of the stream
            flush() {
                if (batchChunks > 0) emit();
            },
        };
    };

    const receivedLog = batchLogger('received');
    const sentLog = batchLogger('sent');

    // Pass chunks along unchanged, logging what we receive from S3
    const logStream = new TransformStream<Uint8Array, Uint8Array>({
        transform(chunk, controller) {
            receivedLog.add(chunk.length);
            controller.enqueue(chunk);
        },
        flush() {
            receivedLog.flush();
        },
    });

    // Client-facing stream: pull() runs only when the runtime asks for the next
    // chunk to send, so the log fires as chunks are handed to the client
    const reader = stream.pipeThrough(logStream).getReader();
    const clientStream = new ReadableStream<Uint8Array>({
        async pull(controller) {
            const { done, value } = await reader.read();
            if (done) {
                sentLog.flush();
                controller.close();
                return;
            }
            sentLog.add(value.length);
            controller.enqueue(value);
        },
        cancel(reason) {
            sentLog.flush();
            console.log('Client cancelled before the stream completed');
            return reader.cancel(reason);
        },
    });

    // Return the S3 body streamed through to the client
    return new Response(clientStream, {
        status: 200,
        headers,
    });
})
    .get("/largetxt", async () => {
        const res = await fetch("https://raw.githubusercontent.com/dscape/spell/refs/heads/master/test/resources/big.txt");
        const { status, headers, body } = res;

        // Create a TransformStream to log chunks and pass them along
        const transformStream = new TransformStream({
            transform(chunk, controller) {
                console.log('Chunk:', chunk.length); // Log the chunk
                controller.enqueue(chunk); // Pass the chunk along
            }
        });

        // Use pipeThrough to process the body through the TransformStream
        const transformedBody = body!.pipeThrough(transformStream);

        return new Response(transformedBody, { status, headers });
    });
;

//@ts-ignore
addEventListener('fetch', async (event: FetchEvent) => {
    event.respondWith(router.fetch(event.request));
});
