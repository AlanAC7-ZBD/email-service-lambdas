const { Client } = require('pg');

exports.handler = async (event) => {

    
   
    if (!process.env.DB_USER || !process.env.DB_PASSWORD) {
        console.error('❌ Missing required environment variables: DB_USER, DB_PASSWORD');
        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
            },
            body: JSON.stringify({
                success: false,
                error: 'Database credentials not configured'
            })
        };
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

        let messageId;
        
    
        if (event.pathParameters && event.pathParameters.messageId) {
            messageId = event.pathParameters.messageId;
        }
      
        else if (event.rawPath) {
            const pathMatch = event.rawPath.match(/\/unsubscribe\/(.+)$/i);
            if (pathMatch) {
                messageId = pathMatch[1];
            }
        }
   
        else if (event.queryStringParameters && event.queryStringParameters.messageId) {
            messageId = event.queryStringParameters.messageId;
        }
    
        else if (event.body) {
            const body = typeof event.body === 'string' ? JSON.parse(event.body) : event.body;
            messageId = body.messageId;
        }
   
        else if (event.messageId) {
            messageId = event.messageId;
        }
        

        
   
        if (!messageId) {
            return {
                statusCode: 400,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({
                    success: false,
                    error: 'Missing required parameter: messageId.'
                })
            };
        }
        
 
        const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
        if (!uuidRegex.test(messageId)) {
            return {
                statusCode: 400,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({
                    success: false,
                    error: 'Invalid messageId format. Must be a valid UUID.',
                    receivedMessageId: messageId
                })
            };
        }
        
     
        await client.connect();
        console.log('✅ Database connected successfully');
        
      

        const queueCheckQuery = `
            SELECT id 
            FROM marketing_comms.queue 
            WHERE "messageId" = $1 
            LIMIT 1
        `;
        
        const queueResult = await client.query(queueCheckQuery, [messageId]);
        
        if (queueResult.rows.length === 0) {
            console.warn('⚠️ MessageId not found in queue');
            return {
                statusCode: 404,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({
                    success: false,
                    error: 'Invalid messageId. No matching record found in the queue.'
                })
            };
        }
        
        console.log('✅ MessageId found in queue');
       
        console.log('🔍 Checking if already unsubscribed:', messageId);
        const existingUnsubscribeQuery = `
            SELECT "messageId" 
            FROM marketing_comms.unsubscribe 
            WHERE "messageId" = $1 
            LIMIT 1
        `;
        
        const existingResult = await client.query(existingUnsubscribeQuery, [messageId]);
        
        if (existingResult.rows.length > 0) {
            console.warn('⚠️ Already unsubscribed');
            return {
                statusCode: 409,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({
                    success: false,
                    error: 'Already unsubscribed.',
                    messageId: messageId
                })
            };
        }
        
    
       
        const insertUnsubscribeQuery = `
            INSERT INTO marketing_comms.unsubscribe ("messageId", "createdAt", "updatedAt")
            VALUES ($1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING "messageId", "createdAt"
        `;
        
        const insertResult = await client.query(insertUnsubscribeQuery, [messageId]);
        
        console.log('✅ Unsubscribe successful');
        
 
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'
            },
            body: JSON.stringify({
                success: true,
                messageId: messageId,
                message: 'You have been successfully unsubscribed from future emails.',
                timestamp: new Date().toISOString(),
                unsubscribedAt: insertResult.rows[0].createdAt
            })
        };
        
    } catch (error) {
        console.error('❌ Error processing unsubscribe:', error);
        
        
        let errorMessage = error.message;
        let statusCode = 500;
        
        if (error.code === '23505') { 
            errorMessage = 'Already unsubscribed.';
            statusCode = 409;
        } else if (error.code === 'ECONNREFUSED') {
            errorMessage = 'Database connection failed.';
            statusCode = 503;
        } else if (error.code === '42P01') { 
            errorMessage = 'Database table not found.';
            statusCode = 500;
        }
        
        return {
            statusCode: statusCode,
            headers: {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            body: JSON.stringify({
                success: false,
                error: errorMessage,
                messageId: event.messageId || 'unknown',
                timestamp: new Date().toISOString()
            })
        };
        
    } finally {
      
        if (client) {
            try {
                await client.end();
                console.log('🔌 Database connection closed');
            } catch (closeError) {
                console.error('❌ Error closing database connection:', closeError.message);
            }
        }
    }
};