# Use Node.js runtime as base image
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production && npm cache clean --force

# Create non-root user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S cosmosuser -u 1001

# Copy application code
COPY --chown=cosmosuser:nodejs . .

# Set default environment variables
ENV APP_NAME="Dashboard Visual Service"
ENV APP_VERSION=1.0.0
ENV DUCKDB_API_BASE_URL=https://wa-duckdb-bot-f6dzhzc6cdfga8bv.southeastasia-01.azurewebsites.net
ENV AZURE_OPENAI_ENDPOINT=https://kisha-m99t7813-eastus2.cognitiveservices.azure.com
ENV AZURE_OPENAI_API_KEY=${AZURE_OPENAI_API_KEY}
ENV AZURE_OPENAI_DEPLOYMENT=depmodel1
ENV AZURE_COSMOS_ENDPOINT=https://test-chatbot.documents.azure.com:443/
ENV AZURE_COSMOS_KEY=${AZURE_COSMOS_KEY}
ENV COSMOSDB_DATABASE=DashboardDB
ENV DB_CONNECTION_STRING="${DB_CONNECTION_STRING}"
ENV DB_USER=srinivas
ENV DB_PASSWORD=${DB_PASSWORD}
ENV DB_SERVER=sqldb-own.database.windows.net
ENV DB_DATABASE=sqldb
ENV DB_PORT=1433
ENV DB_SCHEMA=bi

# Change to non-root user
USER cosmosuser

# Expose port
EXPOSE 8000

# Start the application
CMD ["node", "index.js"]
