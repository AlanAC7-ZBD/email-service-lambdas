import { Client } from 'pg';

async function getStatusId(client, eventType) {
    try {
        const query = 'SELECT id FROM marketing_comms.status WHERE status = $1';
        const result = await client.query(query, [eventType]);
        
        if (result.rows.length === 0) {
            console.warn(`⚠️ Status '${eventType}' not found in database, using null`);
            return null;
        }
        
        return result.rows[0].id;
        
    } catch (error) {
        console.error('❌ Error looking up status ID:', error);
        return null;
    }
}

export const handler = async (event) => {
    console.log('🚀 Processing SQS messages for email events...');
    console.log(`📨 Received ${event.Records.length} messages from SQS`);
    
    if (!process.env.DB_USER || !process.env.DB_PASSWORD) {
        console.error('❌ Missing required environment variables: DB_USER, DB_PASSWORD');
        throw new Error('Database credentials not configured');
    }
    
    const client = new Client({
        host: process.env.DB_HOST,
        port: parseInt(process.env.DB_PORT),
        database: process.env.DB_NAME,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        ssl: {
            rejectUnauthorized: false
        },
        connectionTimeoutMillis: 10000,
        idleTimeoutMillis: 30000,
        query_timeout: 15000
    });
    
    try {
        await client.connect();
        console.log('✅ Database connected successfully!');
        
        const events = [];
        const invalidMessages = [];
        
        for (const record of event.Records) {
            try {
                const messageBody = JSON.parse(record.body);
             
                
                const { resendEmailId, webhookReceivedAt, eventType } = messageBody;
                
                if (resendEmailId && webhookReceivedAt && eventType) {
                    const statusId = await getStatusId(client, eventType);
                    
                    events.push({
                        resendEmailId,
                        statusId,
                        webhookReceivedAt,
                        eventType
                    });
                } else {
                    console.warn('⚠️ Missing properties in message:', messageBody);
                    invalidMessages.push({
                        messageId: record.messageId,
                        body: messageBody,
                        reason: 'Missing required properties (resendEmailId, webhookReceivedAt, eventType)'
                    });
                }
            } catch (parseError) {
                console.error(`❌ Failed to parse message ${record.messageId}:`, parseError);
                invalidMessages.push({
                    messageId: record.messageId,
                    body: record.body,
                    reason: 'JSON parse error'
                });
            }
        }
        
        console.log(`📊 Valid events: ${events.length}, Invalid messages: ${invalidMessages.length}`);
        
        if (event.Records.length === 0) {
            console.log('ℹ️ No messages to process');
            return {
                statusCode: 200,
                body: JSON.stringify({
                    success: true,
                    message: "There are no email events to process."
                })
            };
        }
        
        if (events.length === 0) {
            console.log('ℹ️ No valid events to process');
            return {
                statusCode: 200,
                body: JSON.stringify({
                    success: true,
                    message: "There are no valid events to process.",
                    invalidMessages: invalidMessages.length
                })
            };
        }
        
        console.log(`💾 Inserting ${events.length} email events...`);
        
        await client.query('BEGIN');
        
        try {
            const insertQuery = `
                INSERT INTO marketing_comms.email_events (
                    "resendEmailId", 
                    "statusId", 
                    "webhookReceivedAt",
                    "updatedAt"
                ) VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
            `;
            
            for (const event of events) {
                await client.query(insertQuery, [
                    event.resendEmailId,
                    event.statusId,
                    event.webhookReceivedAt
                ]);
            }
            
            await client.query('COMMIT');
            console.log('✅ Email events added successfully.');
            
            return {
                statusCode: 200,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    success: true,
                    message: "Email events added successfully.",
                    processed: events.length,
                    invalid: invalidMessages.length
                })
            };
            
        } catch (insertError) {
            await client.query('ROLLBACK');
            throw insertError;
        }
        
    } catch (error) {
        console.error('❌ Error trying to insert new email events:', error);
        
        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                success: false,
                error: error.message
            })
        };
        
    } finally {
        try {
            if (client) {
                await client.end();
                console.log('🔌 Database connection closed.');
            }
        } catch (closeError) {
            console.error('❌ Error closing database connection:', closeError.message);
        }
    }
};