const path = require('path');
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');

// Load environment variables
const ENV_FILE = path.join(__dirname, '.env');
dotenv.config({ path: ENV_FILE });

const { router } = require('./src/routes');
const { initializeCosmosClient } = require('./src/services/cosmosService');

// Initialize CosmosDB connection
initializeCosmosClient().catch(error => {
  console.error('[COSMOS] Failed to initialize CosmosDB client:', error);
  process.exit(1);
});

const app = express();

// Body parsing middleware
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

// CORS configuration
const corsOrigins = process.env.CORS_ORIGINS
  ? process.env.CORS_ORIGINS.split(',').map(s => s.trim())
  : true;

app.use(cors({ origin: corsOrigins, credentials: true }));
app.options("*", cors());

app.get("/", (req, res) => {
  res.send("Dashboard Visual Service is running");
});

// Routes
app.use('/api', router);


const PORT = Number(process.env.PORT || 8000);
app.listen(PORT, () => {
  console.log(`🚀 Dashboard Visual Service listening on port ${PORT}`);
  console.log(`📊 API endpoints: http://localhost:${PORT}/api`);
  console.log(`📦 CosmosDB: ${process.env.COSMOSDB_DATABASE || 'Not configured'}`);
});
