const { BlobServiceClient } = require("@azure/storage-blob");

const STORAGE_CONNECTION_STRING = process.env.STORAGE_CONNECTION_STRING;
const blobServiceClient = BlobServiceClient.fromConnectionString(
  STORAGE_CONNECTION_STRING,
);

// Container names
const WIDGET_DATA_CONTAINER = "dashboard-widget-data";
const COMPARISON_DATA_CONTAINER = "dashboard-comparison-data";

// Helper function to sanitize names for blob storage
const sanitizeName = (name) => {
  return name
    .replace(/[^a-zA-Z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .toLowerCase();
};

// Helper function to create folder path
const createFolderPath = (
  username,
  sourceId,
  dashboardName,
  dashboardId,
  timestamp,
  widgetName,
) => {
  const sanitizedUsername = sanitizeName(username);
  const sanitizedDashboardName = sanitizeName(dashboardName);
  const sanitizedWidgetName = sanitizeName(widgetName);
  const timestampStr = timestamp.replace(/[:.]/g, "-");

  return `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/${timestampStr}/${sanitizedWidgetName}`;
};

// Helper function to create comparison path
const createComparisonPath = (
  username,
  sourceId,
  dashboardName,
  dashboardId,
  timestamp,
) => {
  const sanitizedUsername = sanitizeName(username);
  const sanitizedDashboardName = sanitizeName(dashboardName);
  const timestampStr = timestamp.replace(/[:.]/g, "-");

  return `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/${timestampStr}/comparison`;
};

// Helper function to extract timestamp from blob name
const extractTimestampFromBlobName = (blobName) => {
  const parts = blobName.split("/");
  if (parts.length >= 4) {
    const timestampPart = parts[3];
    // Convert back from sanitized format to ISO format
    return timestampPart.replace(/-/g, ":");
  }
  return null;
};

// Widget Data Service
const widgetDataService = {
  // Store widget data for a specific timestamp
  async storeWidgetData(
    username,
    sourceId,
    dashboardName,
    dashboardId,
    widgetData,
  ) {
    try {
      const timestamp = new Date().toISOString();
      const containerClient = blobServiceClient.getContainerClient(
        WIDGET_DATA_CONTAINER,
      );

      // Ensure container exists
      await containerClient.createIfNotExists();

      // Store each widget data as a separate blob
      const storagePromises = widgetData.map(async (widget) => {
        const folderPath = createFolderPath(
          username,
          sourceId,
          dashboardName,
          dashboardId,
          timestamp,
          widget.widgetName,
        );

        const blockBlobClient = containerClient.getBlockBlobClient(
          `${folderPath}.json`,
        );

        const dataToStore = {
          data: widget.data,
          widgetId: widget.widgetId,
          widgetName: widget.widgetName,
          timestamp,
        };

        await blockBlobClient.upload(
          JSON.stringify(dataToStore),
          JSON.stringify(dataToStore).length,
          {
            overwrite: true,
          },
        );

        return {
          widgetId: widget.widgetId,
          widgetName: widget.widgetName,
          path: folderPath,
          timestamp,
        };
      });

      const results = await Promise.all(storagePromises);
      console.log(
        `[BLOB] Stored ${widgetData.length} widgets for dashboard ${dashboardId} at ${timestamp}`,
      );

      return {
        timestamp,
        widgets: results,
      };
    } catch (error) {
      console.error("[BLOB] Error storing widget data:", error.message);
      throw error;
    }
  },

  // Get latest widget data for a dashboard
  async getLatestWidgetData(username, sourceId, dashboardName, dashboardId) {
    try {
      const containerClient = blobServiceClient.getContainerClient(
        WIDGET_DATA_CONTAINER,
      );
      const sanitizedUsername = sanitizeName(username);
      const sanitizedDashboardName = sanitizeName(dashboardName);

      const prefix = `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/`;

      // List blobs to find the latest timestamp folder
      const blobs = [];
      for await (const blob of containerClient.listBlobsFlat({ prefix })) {
        blobs.push(blob.name);
      }

      if (blobs.length === 0) {
        return null;
      }

      // Extract timestamps and find the latest
      const timestampFolders = new Set();
      blobs.forEach((blobName) => {
        const parts = blobName.split("/");
        if (parts.length >= 4) {
          timestampFolders.add(parts[3]); // timestamp is at index 3
        }
      });

      if (timestampFolders.size === 0) {
        return null;
      }

      // Sort timestamps to get the latest
      const sortedTimestamps = Array.from(timestampFolders).sort((a, b) => {
        // Convert back from sanitized format to compare
        const timestampA = a.replace(/-/g, ":");
        const timestampB = b.replace(/-/g, ":");
        return new Date(timestampB) - new Date(timestampA);
      });

      const latestTimestamp = sortedTimestamps[0];
      const latestPrefix = `${prefix}${latestTimestamp}/`;

      // Get all widget blobs for the latest timestamp
      const widgetBlobs = blobs.filter((blobName) =>
        blobName.startsWith(latestPrefix),
      );
      const widgetData = [];

      for (const blobName of widgetBlobs) {
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        const downloadResponse = await blockBlobClient.download();
        const content = await this.streamToString(
          downloadResponse.readableStreamBody,
        );
        widgetData.push(JSON.parse(content));
      }

      return {
        timestamp: latestTimestamp.replace(/-/g, ":"),
        data: widgetData,
      };
    } catch (error) {
      console.error("[BLOB] Error getting latest widget data:", error.message);
      throw error;
    }
  },

  // Get previous widget data for comparison
  async getPreviousWidgetData(
    username,
    sourceId,
    dashboardName,
    dashboardId,
    currentTimestamp,
  ) {
    try {
      const containerClient = blobServiceClient.getContainerClient(
        WIDGET_DATA_CONTAINER,
      );
      const sanitizedUsername = sanitizeName(username);
      const sanitizedDashboardName = sanitizeName(dashboardName);

      const prefix = `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/`;

      // List blobs to find all timestamp folders
      const blobs = [];
      for await (const blob of containerClient.listBlobsFlat({ prefix })) {
        blobs.push(blob.name);
      }

      if (blobs.length === 0) {
        return null;
      }

      // Extract timestamps and sort them
      const timestampFolders = new Set();
      blobs.forEach((blobName) => {
        const parts = blobName.split("/");
        if (parts.length >= 4) {
          timestampFolders.add(parts[3]);
        }
      });

      if (timestampFolders.size <= 1) {
        return null; // No previous data available
      }

      // Sort timestamps to find the one before current
      const sortedTimestamps = Array.from(timestampFolders).sort((a, b) => {
        const timestampA = a.replace(/-/g, ":");
        const timestampB = b.replace(/-/g, ":");
        return new Date(timestampB) - new Date(timestampA);
      });

      // Find the current timestamp in the sorted list and get the previous one
      const currentTimestampSanitized = currentTimestamp.replace(/[:.]/g, "-");
      const currentIndex = sortedTimestamps.indexOf(currentTimestampSanitized);

      if (currentIndex === -1 || currentIndex === sortedTimestamps.length - 1) {
        return null; // Current timestamp not found or no previous data
      }

      const previousTimestamp = sortedTimestamps[currentIndex + 1];
      const previousPrefix = `${prefix}${previousTimestamp}/`;

      // Get all widget blobs for the previous timestamp
      const widgetBlobs = blobs.filter((blobName) =>
        blobName.startsWith(previousPrefix),
      );
      const widgetData = [];

      for (const blobName of widgetBlobs) {
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        const downloadResponse = await blockBlobClient.download();
        const content = await this.streamToString(
          downloadResponse.readableStreamBody,
        );
        widgetData.push(JSON.parse(content));
      }

      return {
        timestamp: previousTimestamp.replace(/-/g, ":"),
        data: widgetData,
      };
    } catch (error) {
      console.error(
        "[BLOB] Error getting previous widget data:",
        error.message,
      );
      throw error;
    }
  },

  // Get last 5 entries for comparison analysis
  async getLastFiveEntries(username, sourceId, dashboardName, dashboardId) {
    try {
      const containerClient = blobServiceClient.getContainerClient(
        WIDGET_DATA_CONTAINER,
      );
      const sanitizedUsername = sanitizeName(username);
      const sanitizedDashboardName = sanitizeName(dashboardName);

      const prefix = `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/`;

      // List blobs to find all timestamp folders
      const blobs = [];
      for await (const blob of containerClient.listBlobsFlat({ prefix })) {
        blobs.push(blob.name);
      }

      if (blobs.length === 0) {
        return [];
      }

      // Extract timestamps and sort them
      const timestampFolders = new Set();
      blobs.forEach((blobName) => {
        const timestamp = extractTimestampFromBlobName(blobName);
        if (timestamp) {
          timestampFolders.add(timestamp);
        }
      });

      // Sort timestamps in descending order
      const sortedTimestamps = Array.from(timestampFolders).sort((a, b) => {
        return new Date(b) - new Date(a);
      });

      // Get data for the last 5 timestamps
      const lastFiveTimestamps = sortedTimestamps.slice(0, 5);
      const entries = [];

      for (const timestamp of lastFiveTimestamps) {
        const timestampSanitized = timestamp.replace(/[:.]/g, '-');
        const timestampPrefix = `${prefix}${timestampSanitized}/`;
        const widgetBlobs = blobs.filter((blobName) =>
          blobName.startsWith(timestampPrefix),
        );
        const widgetData = [];

        for (const blobName of widgetBlobs) {
          const blockBlobClient = containerClient.getBlockBlobClient(blobName);
          const downloadResponse = await blockBlobClient.download();
          const content = await this.streamToString(
            downloadResponse.readableStreamBody,
          );
          widgetData.push(JSON.parse(content));
        }

        entries.push({
          timestamp,
          data: widgetData,
        });
      }

      return entries;
    } catch (error) {
      console.error("[BLOB] Error getting last five entries:", error.message);
      throw error;
    }
  },

  // Helper function to convert stream to string
  async streamToString(readableStream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      readableStream.on("data", (data) => {
        chunks.push(data.toString());
      });
      readableStream.on("end", () => {
        resolve(chunks.join(""));
      });
      readableStream.on("error", reject);
    });
  },
};

// Comparison Data Service
const comparisonDataService = {
  // Store comparison data
  async storeComparisonData(
    username,
    sourceId,
    dashboardName,
    dashboardId,
    comparisonData,
  ) {
    try {
      const timestamp = new Date().toISOString();
      const containerClient = blobServiceClient.getContainerClient(
        COMPARISON_DATA_CONTAINER,
      );

      // Ensure container exists
      await containerClient.createIfNotExists();

      const folderPath = createComparisonPath(
        username,
        sourceId,
        dashboardName,
        dashboardId,
        timestamp,
      );
      const blockBlobClient = containerClient.getBlockBlobClient(
        `${folderPath}.json`,
      );

      const dataToStore = {
        dashboardId,
        type: "comparison",
        timestamp,
        comparisonData,
        createdAt: new Date().toISOString(),
      };

      await blockBlobClient.upload(
        JSON.stringify(dataToStore),
        JSON.stringify(dataToStore).length,
        {
          overwrite: true,
        },
      );

      console.log(
        `[BLOB] Stored comparison data for dashboard ${dashboardId} at ${timestamp}`,
      );
      return dataToStore;
    } catch (error) {
      console.error("[BLOB] Error storing comparison data:", error.message);
      throw error;
    }
  },

  // Get last 5 comparison cycles for AI analysis
  async getLastFiveComparisons(username, sourceId, dashboardName, dashboardId) {
    try {
      const containerClient = blobServiceClient.getContainerClient(
        COMPARISON_DATA_CONTAINER,
      );
      const sanitizedUsername = sanitizeName(username);
      const sanitizedDashboardName = sanitizeName(dashboardName);

      const prefix = `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/`;

      // List blobs to find all comparison files
      const blobs = [];
      for await (const blob of containerClient.listBlobsFlat({ prefix })) {
        if (blob.name.endsWith("/comparison.json")) {
          blobs.push(blob.name);
        }
      }

      if (blobs.length === 0) {
        return [];
      }

      // Sort by timestamp (descending)
      const sortedBlobs = blobs.sort((a, b) => {
        // Extract timestamp from blob path
        const timestampA = extractTimestampFromBlobName(a);
        const timestampB = extractTimestampFromBlobName(b);
        return new Date(timestampB) - new Date(timestampA);
      });

      // Get data for the last 5 comparisons
      const lastFiveBlobs = sortedBlobs.slice(0, 5);
      const comparisons = [];

      for (const blobName of lastFiveBlobs) {
        const blockBlobClient = containerClient.getBlockBlobClient(blobName);
        const downloadResponse = await blockBlobClient.download();
        const content = await this.streamToString(
          downloadResponse.readableStreamBody,
        );
        comparisons.push(JSON.parse(content));
      }

      return comparisons;
    } catch (error) {
      console.error(
        "[BLOB] Error getting last five comparisons:",
        error.message,
      );
      throw error;
    }
  },

  // Helper function to convert stream to string
  async streamToString(readableStream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      readableStream.on("data", (data) => {
        chunks.push(data.toString());
      });
      readableStream.on("end", () => {
        resolve(chunks.join(""));
      });
      readableStream.on("error", reject);
    });
  },
};

module.exports = {
  widgetDataService,
  comparisonDataService,
  blobServiceClient,
};
