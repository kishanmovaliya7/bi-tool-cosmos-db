const { AzureOpenAI } = require("openai");

// azure client config
const endpoint = process.env.AZURE_OPENAI_ENDPOINT;
const apiKey = process.env.AZURE_OPENAI_API_KEY;
const modelDeployment = process.env.AZURE_OPENAI_DEPLOYMENT || "depmodel";
const apiVersion = process.env.AZURE_API_VERSION || "2024-12-01-preview";

const azureClient = new AzureOpenAI({
  endpoint,
  apiKey,
  apiVersion: apiVersion,
  deployment: modelDeployment,
});

module.exports = { azureClient };
