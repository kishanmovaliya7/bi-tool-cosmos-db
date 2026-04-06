// Add crypto polyfill for Node.js compatibility
if (typeof globalThis.crypto === 'undefined') {
  globalThis.crypto = require('crypto');
}

const { CosmosClient } = require("@azure/cosmos");

let cosmosClient;
let database;
let mockMode = false;

// Initialize CosmosDB client
const initializeCosmosClient = async () => {
  try {
    const key = process.env.AZURE_COSMOS_KEY;
    const cosmosEndpoint = process.env.AZURE_COSMOS_ENDPOINT;
    const databaseName = process.env.COSMOSDB_DATABASE || 'DashboardDB';

    if (!cosmosEndpoint || cosmosEndpoint === 'https://your-cosmosdb-account.documents.azure.com:443/' ||
        !key || key === 'your-cosmosdb-primary-key') {
      console.log('[COSMOS] Using mock mode');
      mockMode = true;
      return { cosmosClient: null, database: null };
    }

    cosmosClient = new CosmosClient({ endpoint: cosmosEndpoint, key });
    const { database: db } = await cosmosClient.databases.createIfNotExists({
      id: databaseName,
    });
    database = db;

    await database.containers.createIfNotExists({
      id: 'visual-responses',
      partitionKey: { paths: ['/dashboardId'] }
    });

    return { cosmosClient, database };
  } catch (error) {
    console.error('[COSMOS] Failed to initialize:', error.message);
    mockMode = true;
    return { cosmosClient: null, database: null };
  }
};

// Visual Response Service
const visualResponseService = {
  // Store visual response
  async storeVisualResponse(dashboardId, queryResult) {
    try {
      const item = {
        dashboardId,
        queryResult: queryResult,
        createdAt: new Date().toISOString()
      };

      if (mockMode) {
        return item;
      }

      const { resource } = await database.container('visual-responses').items.create(item);
      console.log(`[COSMOS] Stored response for dashboard ${dashboardId} with ${queryResult.length} widgets`);
      return resource;
    } catch (error) {
      console.error('[COSMOS] Error storing response:', error.message);
      throw error;
    }
  },
};

module.exports = {
  initializeCosmosClient,
  visualResponseService,
  isMockMode: () => mockMode
};
