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
            UPDATE marketing_comms.at_risk_reactivation_campaign arc
SET "emailSentAt" = q."lastEmailSentAt",
    "bonusStartAt" = q."lastEmailSentAt",
    "bonusExpiresAt" = q."lastEmailSentAt" + interval '7 days',
    "updatedAt" = current_timestamp
FROM marketing_comms.queue q
WHERE arc."zbdUserId" = q."zbdUserId" 
  AND q."statusId" IN (4,6,7,8,9)
  AND q."categoryId" = 43
  AND arc."emailSentAt" IS NULL;
        `;
        
        console.log('🔄 Updating at risk reactivation campaign timestamps...');
        
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
                message: `At Risk reactivation campaign sync completed: Updated ${result.rowCount} records`,
                timestamp: new Date().toISOString(),
                batchSize: result.rowCount,
                moreToProcess: moreToProcess
            })
        };
        
    } catch (error) {
        console.error('❌ Error executing at Risk reactivation campaign sync:', error);
        
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

