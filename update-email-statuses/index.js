const { Client } = require('pg');

exports.handler = async (event) => {
    console.log('🚀 Starting optimized email status synchronization...');
    
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
        connectionTimeoutMillis: 15000,
        idleTimeoutMillis: 60000,
        query_timeout: 300000 
    });
    
    try {
        await client.connect();
        console.log('✅ Database connected successfully!');
        
        
        const updateQuery = `
            WITH recent_queue AS (
                SELECT id, "resendEmailId", "statusId"
                FROM marketing_comms.queue 
                WHERE "statusId" IN (11, 5, 3, 2, 10, 4)
                AND "createdAt" >= NOW() - INTERVAL '6 months' 
                LIMIT 10000 -- 
            ),
            relevant_events AS (
                -- Get only events for the limited queue records
                SELECT DISTINCT ON (ee."resendEmailId") 
                    ee."resendEmailId",
                    ee."statusId" as event_status_id,
                    ee."webhookReceivedAt",
                    CASE ee."statusId"
                        WHEN 7 THEN 1 -- bounced (highest priority)
                        WHEN 6 THEN 1 -- complained (highest priority) 
                        WHEN 8 THEN 2 -- clicked
                        WHEN 9 THEN 3 -- opened
                        WHEN 4 THEN 4 -- delivered (lowest priority)
                        WHEN 5 THEN 5 -- delivery delayed
                    END as priority_order
                FROM marketing_comms.email_events ee
                WHERE ee."statusId" IN (7, 6, 8, 9, 4, 5)
                AND ee."resendEmailId" IN (SELECT "resendEmailId" FROM recent_queue)
                ORDER BY ee."resendEmailId", priority_order ASC, ee."createdAt" DESC
            ),
            updates_needed AS (
                SELECT 
                    rq.id as queue_id,
                    re.event_status_id as new_status_id,
                    re."webhookReceivedAt" as last_email_sent_at,
                    rq."statusId" as current_status_id
                FROM recent_queue rq
                JOIN relevant_events re ON rq."resendEmailId" = re."resendEmailId"
                WHERE rq."statusId" != re.event_status_id 
                LIMIT 3000 
            )
            UPDATE marketing_comms.queue q
            SET "statusId" = un.new_status_id,
                "lastEmailSentAt" = un.last_email_sent_at,
                "updatedAt" = CURRENT_TIMESTAMP
            FROM updates_needed un
            WHERE q.id = un.queue_id
        `;
        
        console.log('🔄 Updating email statuses...');
        
        const result = await client.query(updateQuery);
        
        console.log(`✅ Email status sync completed: Updated ${result.rowCount} queue records`);
        
        
        const moreToProcess = result.rowCount > 0;
        
        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                success: true,
                message: `Email status sync completed: Updated ${result.rowCount} queue records`,
                timestamp: new Date().toISOString(),
                batchSize: result.rowCount,
                moreToProcess: moreToProcess
            })
        };
        
    } catch (error) {
        console.error('❌ Error executing email status sync:', error);
        
        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                success: false,
                error: error.message,
                timestamp: new Date().toISOString()
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