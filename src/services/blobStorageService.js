const { BlobServiceClient } = require("@azure/storage-blob");

const STORAGE_CONNECTION_STRING = process.env.STORAGE_CONNECTION_STRING;
const blobServiceClient = BlobServiceClient.fromConnectionString(
  STORAGE_CONNECTION_STRING,
);

// Container names
const WIDGET_DATA_CONTAINER = "dashboard-widget-data";
const COMPARISON_DATA_CONTAINER = "dashboard-comparison-data";
const WIDGET_COMPARISON_CONTAINER = "dashboard-widget-comparison";

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
  widgetId
) => {
  const sanitizedUsername = sanitizeName(username);
  const sanitizedDashboardName = sanitizeName(dashboardName);
  const sanitizedWidgetName = sanitizeName(widgetName);
  const timestampStr = timestamp.replace(/[:.]/g, "-");

  return `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/${timestampStr}/${sanitizedWidgetName}-${widgetId}`;
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

const createWidgetComparisonPath = (
  username,
  sourceId,
  dashboardName,
  dashboardId,
  widgetName,
  widgetId,
  timestamp,
) => {
  const sanitizedUsername = sanitizeName(username);
  const sanitizedDashboardName = sanitizeName(dashboardName);
  const sanitizedWidgetName = sanitizeName(widgetName || `widget-${widgetId}`);
  const timestampStr = timestamp.replace(/[:.]/g, "-");

  return `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/${sanitizedWidgetName}-${widgetId}/${timestampStr}/comparison`;
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

const extractComparisonTimestampFromBlobName = (blobName) => {
  const parts = blobName.split("/");
  if (parts.length >= 2) {
    const timestampPart = parts[parts.length - 2];
    return timestampPart.replace(/-/g, ":");
  }
  return null;
};

// dashboard Data Service
const dashboardDataService = {
  // Store dashboard data for a specific timestamp
  async storeDashboardData(
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
          widget.widgetId
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

  // Get last 2 entries for comparison analysis
  async getLastTwoEntries(username, sourceId, dashboardName, dashboardId) {
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
        // Convert timestamp format from '2026:04:29T05:15:06:731Z' to '2026-04-29T05:15:06.731Z'
        const normalizeTimestamp = (timestamp) => {
          // Manual conversion: 2026:04:29T05:15:06:731Z -> 2026-04-29T05:15:06.731Z
          return timestamp
            .replace(/^(\d+):(\d+):(\d+)T/, '$1-$2-$3T')  // Replace date colons with dashes
            .replace(/:(\d+):(\d+)Z/, ':$1.$2Z');          // Replace final colon with dot
        };
        
        const dateA = new Date(normalizeTimestamp(a));
        const dateB = new Date(normalizeTimestamp(b));
        
        return dateB - dateA;
      });

      // Get data for the last 2 timestamps
      const lastTwoTimestamps = sortedTimestamps.slice(0, 2);
      const entries = [];

      console.log("lastTwoTimestamps---", lastTwoTimestamps);
      

      for (const timestamp of lastTwoTimestamps) {
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
      console.error("[BLOB] Error getting last two entries:", error.message);
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

  async storeWidgetComparisonData(
    username,
    sourceId,
    dashboardName,
    dashboardId,
    widgetName,
    widgetId,
    comparisonData,
  ) {
    try {
      const timestamp = new Date().toISOString();
      const containerClient = blobServiceClient.getContainerClient(
        WIDGET_COMPARISON_CONTAINER,
      );

      await containerClient.createIfNotExists();

      const folderPath = createWidgetComparisonPath(
        username,
        sourceId,
        dashboardName,
        dashboardId,
        widgetName,
        widgetId,
        timestamp,
      );
      const blockBlobClient = containerClient.getBlockBlobClient(
        `${folderPath}.json`,
      );

      const dataToStore = {
        dashboardId,
        widgetId,
        widgetName,
        type: "widget-comparison",
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
        `[BLOB] Stored comparison data for widget ${widgetId} in dashboard ${dashboardId} at ${timestamp}`,
      );
      return dataToStore;
    } catch (error) {
      console.error(
        "[BLOB] Error storing widget comparison data:",
        error.message,
      );
      throw error;
    }
  },

  async getLastFiveWidgetComparisons(
    username,
    sourceId,
    dashboardName,
    dashboardId,
    widgetName,
    widgetId,
  ) {
    try {
      const containerClient = blobServiceClient.getContainerClient(
        WIDGET_COMPARISON_CONTAINER,
      );
      const sanitizedUsername = sanitizeName(username);
      const sanitizedDashboardName = sanitizeName(dashboardName);
      const sanitizedWidgetName = sanitizeName(widgetName || `widget-${widgetId}`);

      const prefix = `${sanitizedUsername}/${sourceId}/${sanitizedDashboardName}-${dashboardId}/${sanitizedWidgetName}-${widgetId}/`;

      const blobs = [];
      for await (const blob of containerClient.listBlobsFlat({ prefix })) {
        if (blob.name.endsWith("/comparison.json")) {
          blobs.push(blob.name);
        }
      }

      if (blobs.length === 0) {
        return [];
      }

      const sortedBlobs = blobs.sort((a, b) => {
        const timestampA = extractComparisonTimestampFromBlobName(a);
        const timestampB = extractComparisonTimestampFromBlobName(b);
        return new Date(timestampB) - new Date(timestampA);
      });

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
        "[BLOB] Error getting last five widget comparisons:",
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
  dashboardDataService,
  comparisonDataService,
  blobServiceClient,
};
