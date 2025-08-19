const { Webhook } = require('svix');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');

const sqs = new SQSClient({ region: process.env.AWS_REGION });

exports.handler = async (event) => {
    console.log('📨 Received webhook event');
    
    try {
        const RESEND_SIGNING_SECRET = process.env.RESEND_SIGNING_SECRET;
        const SQS_QUEUE_URL = process.env.SQS_QUEUE_URL;
        
        if (!RESEND_SIGNING_SECRET || !SQS_QUEUE_URL) {
            throw new Error("❌ Missing environment variables");
        }
        
        const payload = event.body;
        const headers = event.headers;
        
        
        const isValid = await verifyWithSvix(payload, headers, RESEND_SIGNING_SECRET);
        if (!isValid) {
            return {
                statusCode: 403,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    success: false,
                    error: "Invalid signature"
                })
            };
        }
        
        console.log('✅ Message signature successfully verified');
        
        const body = JSON.parse(payload);
        const extractedType = body.type.replace(/^email\./, "");
        

        
        const updatedBody = {
            resendEmailId: body.data.email_id,
            webhookReceivedAt: body.data.created_at,
            eventType: extractedType
        };
        
        await insertIntoQueue(SQS_QUEUE_URL, updatedBody);
        
        return {
            statusCode: 200,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                success: true,
                message: "Webhook received, verified, and inserted into queue",
                data: updatedBody,
                timestamp: new Date().toISOString()
            })
        };
        
    } catch (error) {
        console.error('❌ Error processing webhook:', error);
        
        return {
            statusCode: 500,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                success: false,
                error: error.message
            })
        };
    }
};

async function verifyWithSvix(payload, headers, signingSecret) {
    try {
        const svix = new Webhook(signingSecret);
        const headersObject = {};
        Object.entries(headers).forEach(([key, value]) => {
            if (value) headersObject[key] = value;
        });
        
        svix.verify(payload, headersObject);
        return true;
    } catch (error) {
        console.error("❌ Signature verification failed:", error.message);
        return false;
    }
}

async function insertIntoQueue(queueUrl, messageBody) {
    try {
        const command = new SendMessageCommand({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(messageBody)
        });
        
        const result = await sqs.send(command);
        console.log('✅ Message sent to SQS');
        
    } catch (error) {
        console.error('❌ Error sending to SQS:', error);
        throw new Error(`❌ Error inserting into queue: ${error.message}`);
    }
}