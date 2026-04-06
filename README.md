# Dashboard Visual Service - Simple Setup

## 🚀 **What This App Does**

1. **Gets user dashboards** from your existing API (my-chat-bot)
2. **Extracts visual data** and SQL queries from each dashboard
3. **Executes the SQL queries** using the same query API as my-chat-bot
4. **Stores responses in Cosmos DB** with timestamped filenames for easy comparison

## 📝 **Configuration**

Create `.env` file with your actual credentials:
```bash
# Azure Services Configuration
AZURE_OPENAI_API_KEY=your-azure-openai-api-key-here
AZURE_COSMOS_KEY=your-azure-cosmos-db-key-here

# Database Configuration
DB_CONNECTION_STRING=your-database-connection-string-here
DB_PASSWORD=your-database-password-here

# Other environment variables (optional)
PORT=8000
NODE_ENV=development
```

**Important**: Never commit your `.env` file to version control. It's already included in `.gitignore`.

## 🏃‍♂️ **How to Run**

### **1. Install Dependencies**
```bash
cd cosmos-db
npm install
```

### **2. Start the App**
```bash
# Development mode
npm run dev

# Production mode
npm start
```

### **3. Test the App**
```bash
# Start processing dashboards (uses hardcoded userId=5, accountId=2)
curl -X POST http://localhost:8080/api/process/5

# Get stored visual responses
curl http://localhost:8080/api/data/5

# Check health
curl http://localhost:8080/healthz
```

## 📊 **API Endpoints**

```
POST /api/process/:userId     # Process all dashboards for user
GET  /api/data/:userId         # Get stored visual responses
GET  /healthz                  # Health check
GET  /                         # App info
```

## �️ **File Structure (Cleaned Up)**

```
cosmos-db/
├── package.json               # Dependencies
├── index.js                   # Main server
├── .env.example              # Environment template
├── src/
│   ├── routes/
│   │   └── index.js          # API routes (only 2 endpoints)
│   ├── controllers/
│   │   └── dashboardController.js  # Main logic
│   └── services/
│       ├── cosmosService.js   # Cosmos DB operations
│       └── dashboardProcessor.js  # Dashboard processing
└── Dockerfile                # For Azure deployment
```

## � **Data Storage**

Responses are stored in Cosmos DB with:
- **Timestamped filenames**: `revenue_chart_20240404_112200.json`
- **User partitioning**: Organized by userId
- **Complete data**: Query, parameters, response, execution time

## � **Hardcoded Values**

As requested:
- **userId**: 5
- **accountId**: 2

You can change these in `src/services/dashboardProcessor.js`

## 🐳 **Docker Run**

```bash
# Build and run
docker build -t dashboard-visual-service .
docker run -p 8080:8080 --env-file .env dashboard-visual-service
```

That's it! No extra routes, no extra functionality - exactly what you need.
