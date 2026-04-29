const {
  dashboardDataService,
  comparisonDataService,
} = require("../services/blobStorageService");
const { SQLquery } = require("../services/dbConnect");
const axios = require("axios");
const { azureClient } = require("../services/openaiClient");
const {
  generateAuthToken,
  fetchUserProfile,
} = require("../helpers/authHelper");

const DUCKDB_API_BASE_URL =
  process.env.DUCKDB_API_BASE_URL || "http://localhost:8080";

// Enhanced helper function to generate comparison JSON between refresh cycles
const generateComparisonJson = (latestData, previousData) => {
  const widgetDiffs = [];
  const stableMetrics = [];
  let changedWidgets = 0;
  let stableWidgetsCount = 0;
  const topMovers = [];

  // Create maps for easier comparison
  const latestMap = new Map();
  const previousMap = new Map();

  latestData.forEach((widget) => {
    latestMap.set(widget.widgetId, widget);
  });

  previousData.forEach((widget) => {
    previousMap.set(widget.widgetId, widget);
  });

  // Compare widgets with enhanced multi-data comparison
  for (const [widgetId, latestWidget] of latestMap) {
    const previousWidget = previousMap.get(widgetId);

    if (!previousWidget) {
      // New widget, skip for now
      continue;
    }

    // Enhanced comparison that handles multiple data points
    const widgetComparison = generateWidgetComparison(
      latestWidget,
      previousWidget,
    );

    if (widgetComparison.hasChanges) {
      changedWidgets++;
      widgetDiffs.push(...widgetComparison.diffs);

      // Track top movers (top 3 by percent change)
      widgetComparison.diffs.forEach((diff) => {
        if (diff.percentChange && Math.abs(diff.percentChange) > 5) {
          topMovers.push({
            widgetName: diff.widgetName,
            percentChange: diff.percentChange,
          });
        }
      });
    } else {
      stableWidgetsCount++;
      stableMetrics.push(latestWidget.widgetName);
    }
  }

  // Sort top movers by absolute percent change
  topMovers.sort(
    (a, b) => Math.abs(b.percentChange) - Math.abs(a.percentChange),
  );

  return {
    summaryStats: {
      changedWidgets,
      stableWidgets: stableWidgetsCount,
      topMovers: Math.min(3, topMovers.length),
    },
    widgetDiffs: widgetDiffs.slice(0, 10), // Limit to top 10 changes
    stableMetrics: stableMetrics.slice(0, 5), // Limit to first 5 stable metrics
  };
};

// Enhanced function to generate widget-level comparison with multi-data support
const generateWidgetComparison = (latestWidget, previousWidget) => {
  const diffs = [];
  const latestRows = Array.isArray(latestWidget.data) ? latestWidget.data : [];
  const previousRows = Array.isArray(previousWidget.data)
    ? previousWidget.data
    : [];

  let hasChanges = false;

  // Handle single row data (existing logic)
  if (latestRows.length === 1 && previousRows.length === 1) {
    const latestRow = latestRows[0] || {};
    const previousRow = previousRows[0] || {};

    const allFields = Array.from(
      new Set([...Object.keys(previousRow), ...Object.keys(latestRow)]),
    );

    allFields.forEach((fieldName) => {
      const latestValueRaw = latestRow[fieldName];
      const previousValueRaw = previousRow[fieldName];
      const latestValue = normalizeComparableValue(latestValueRaw);
      const previousValue = normalizeComparableValue(previousValueRaw);

      if (!valuesAreEqual(latestValue, previousValue)) {
        hasChanges = true;
        const diff = createFieldDiff(
          latestWidget,
          fieldName,
          latestValue,
          previousValue,
          latestValueRaw,
          previousValueRaw,
        );
        diffs.push(diff);
      }
    });
  }
  // Handle multi-row data - compare by key fields
  else if (latestRows.length > 0 && previousRows.length > 0) {
    const multiRowComparison = compareMultiRowData(
      latestWidget,
      latestRows,
      previousRows,
    );
    if (multiRowComparison.hasChanges) {
      hasChanges = true;
      // Consolidate diffs by field name to avoid duplicates
      const consolidatedDiffs = consolidateDiffsByField(
        multiRowComparison.diffs,
      );
      diffs.push(...consolidatedDiffs);
    }
  }

  return {
    hasChanges,
    diffs,
    widgetName: latestWidget.widgetName || latestWidget.widgetId,
  };
};

// Function to compare multi-row data by identifying common keys
const compareMultiRowData = (widget, latestRows, previousRows) => {
  const diffs = [];
  let hasChanges = false;

  // Identify potential key fields (common fields that could be used as identifiers)
  const latestFirstRow = latestRows[0] || {};
  const previousFirstRow = previousRows[0] || {};
  const allFields = Array.from(
    new Set([...Object.keys(previousFirstRow), ...Object.keys(latestFirstRow)]),
  );

  // Try to find matching records by common identifier fields
  const keyFields = ["id", "client_id", "user_id", "name", "code", "key"];
  const identifiedKeyField =
    keyFields.find((field) => allFields.includes(field)) || allFields[0];

  if (identifiedKeyField) {
    // Create maps for matching records
    const latestMap = new Map();
    const previousMap = new Map();

    latestRows.forEach((row) => {
      const key = row[identifiedKeyField];
      if (key !== undefined && key !== null) {
        latestMap.set(key, row);
      }
    });

    previousRows.forEach((row) => {
      const key = row[identifiedKeyField];
      if (key !== undefined && key !== null) {
        previousMap.set(key, row);
      }
    });

    // Compare matching records
    for (const [key, latestRow] of latestMap) {
      const previousRow = previousMap.get(key);

      if (previousRow) {
        // Compare fields for matching records
        const recordDiffs = compareRecordFields(
          widget,
          key,
          latestRow,
          previousRow,
        );
        if (recordDiffs.length > 0) {
          hasChanges = true;
          diffs.push(...recordDiffs);
        }
      }
    }
  }

  // If no key field matching, do aggregate comparison
  if (!hasChanges) {
    const aggregateComparison = compareAggregateData(
      widget,
      latestRows,
      previousRows,
    );
    if (aggregateComparison.hasChanges) {
      hasChanges = true;
      diffs.push(...aggregateComparison.diffs);
    }
  }

  return { hasChanges, diffs };
};

// Function to compare fields between matching records
const compareRecordFields = (widget, recordKey, latestRow, previousRow) => {
  const diffs = [];
  const allFields = Array.from(
    new Set([...Object.keys(previousRow), ...Object.keys(latestRow)]),
  );

  allFields.forEach((fieldName) => {
    if (fieldName === recordKey) return; // Skip the key field itself

    const latestValueRaw = latestRow[fieldName];
    const previousValueRaw = previousRow[fieldName];
    const latestValue = normalizeComparableValue(latestValueRaw);
    const previousValue = normalizeComparableValue(previousValueRaw);

    if (!valuesAreEqual(latestValue, previousValue)) {
      const diff = createFieldDiff(
        widget,
        fieldName,
        latestValue,
        previousValue,
        latestValueRaw,
        previousValueRaw,
      );
      diff.entity = recordKey; // Add the record identifier
      diffs.push(diff);
    }
  });

  return diffs;
};

// Function to compare aggregate data when no key matching is possible
const compareAggregateData = (widget, latestRows, previousRows) => {
  const diffs = [];
  let hasChanges = false;

  // Calculate aggregate metrics
  const latestAggregates = calculateAggregates(latestRows);
  const previousAggregates = calculateAggregates(previousRows);

  // Compare numeric aggregates
  Object.keys(latestAggregates).forEach((field) => {
    if (previousAggregates[field] !== undefined) {
      const latestValue = latestAggregates[field];
      const previousValue = previousAggregates[field];

      if (!valuesAreEqual(latestValue, previousValue)) {
        hasChanges = true;
        const diff = createFieldDiff(
          widget,
          `aggregate_${field}`,
          latestValue,
          previousValue,
          latestValue,
          previousValue,
        );
        diff.entity = "aggregate";
        diffs.push(diff);
      }
    }
  });

  return { hasChanges, diffs };
};

// Function to calculate aggregate metrics
const calculateAggregates = (rows) => {
  const aggregates = {};

  if (rows.length === 0) return aggregates;

  const firstRow = rows[0];
  Object.keys(firstRow).forEach((field) => {
    const values = rows
      .map((row) => normalizeComparableValue(row[field]))
      .filter((val) => typeof val === "number" && Number.isFinite(val));

    if (values.length > 0) {
      aggregates[field] = {
        sum: values.reduce((a, b) => a + b, 0),
        avg: values.reduce((a, b) => a + b, 0) / values.length,
        count: values.length,
        min: Math.min(...values),
        max: Math.max(...values),
      };
    }
  });

  return aggregates;
};

// Function to consolidate diffs by field name to avoid duplicates
const consolidateDiffsByField = (diffs) => {
const consolidatedMap = new Map();

diffs.forEach((diff) => {
const key = `${diff.fieldName}_${diff.latestValue}_${diff.previousValue}`;

if (!consolidatedMap.has(key)) {
// First occurrence, store the diff
consolidatedMap.set(key, { ...diff });
} else {
// Merge with existing diff
const existing = consolidatedMap.get(key);

// Track multiple entities if they exist
if (diff.entity && !existing.entities) {
existing.entities = [existing.entity || diff.entity].filter(Boolean);
}
if (diff.entity && existing.entities && !existing.entities.includes(diff.entity)) {
existing.entities.push(diff.entity);
}
}
});

// Convert map back to array and clean up
return Array.from(consolidatedMap.values()).map((diff) => {
// Remove entity if we have multiple entities, or keep single entity
if (diff.entities && diff.entities.length > 1) {
diff.entity = `${diff.entities.length} entities`;
delete diff.entities;
}
return diff;
});
};

// Function to create field difference object
const createFieldDiff = (
  widget,
  fieldName,
  latestValue,
  previousValue,
  latestValueRaw,
  previousValueRaw,
) => {
  const diff = {
    widgetName: widget.widgetName || widget.widgetId,
    widgetId: widget.widgetId,
    fieldName,
    latestValue,
    previousValue,
    latestValueRaw,
    previousValueRaw,
    changeType: detectFieldChangeType(latestValueRaw, previousValueRaw),
  };

  // Add numeric change calculations if applicable
  if (
    typeof latestValue === "number" &&
    typeof previousValue === "number" &&
    Number.isFinite(latestValue) &&
    Number.isFinite(previousValue)
  ) {
    const change = calculateChange(latestValue, previousValue);
    diff.absoluteChange = change.absoluteChange;
    diff.percentChange = change.percentChange;
    diff.isSignificant = change.isSignificant;
    diff.direction = change.direction;
    diff.metricType = detectMetricType(latestValue, previousValue);
  } else {
    diff.isSignificant = true;
    diff.direction = "changed";
    diff.metricType = "text";
  }

  return diff;
};

// Enhanced helper function to generate widget comparison JSON with proper format support
const generateEnhancedWidgetComparisonJson = (latestWidget, previousWidget) => {
  const latestRows = Array.isArray(latestWidget.data) ? latestWidget.data : [];
  const previousRows = Array.isArray(previousWidget.data)
    ? previousWidget.data
    : [];

  const widgetDiffs = [];
  const stableMetrics = [];
  let changedFields = 0;
  let stableFields = 0;
  const topMovers = [];

  // Use the enhanced widget comparison logic
  const widgetComparison = generateWidgetComparison(
    latestWidget,
    previousWidget,
  );

  if (widgetComparison.hasChanges) {
    changedFields = widgetComparison.diffs.length;

    // Convert diffs to the required format
    widgetComparison.diffs.forEach((diff) => {
      const formattedDiff = {
        widgetName: diff.widgetName,
      };

      // Add entity if available
      if (diff.entity) {
        formattedDiff.entity = diff.entity;
      }

      // Handle numeric values with change calculations
      if (
        typeof diff.latestValue === "number" &&
        typeof diff.previousValue === "number"
      ) {
        formattedDiff.latestValue = diff.latestValue;
        formattedDiff.previousValue = diff.previousValue;
        formattedDiff.absoluteChange = diff.absoluteChange;
        formattedDiff.percentChange = diff.percentChange;
        formattedDiff.isSignificant = diff.isSignificant;
        formattedDiff.direction = diff.direction;
        formattedDiff.metricType = diff.metricType;
      } else {
        // Handle text/other changes
        formattedDiff.latestValue = diff.latestValueRaw;
        formattedDiff.previousValue = diff.previousValueRaw;
        formattedDiff.changeType = diff.changeType;
        formattedDiff.isSignificant = true;
        formattedDiff.direction = "changed";
        formattedDiff.metricType = "text";
      }

      widgetDiffs.push(formattedDiff);

      // Track top movers
      if (diff.percentChange && Math.abs(diff.percentChange) > 5) {
        topMovers.push({
          fieldName: diff.fieldName,
          percentChange: diff.percentChange,
        });
      }
    });
  } else {
    stableFields = 1; // Widget is stable
    stableMetrics.push(latestWidget.widgetName || latestWidget.widgetId);
  }

  // Sort top movers by absolute percent change
  topMovers.sort(
    (a, b) => Math.abs(b.percentChange) - Math.abs(a.percentChange),
  );

  return {
    timestamp: formatComparisonTimestamp(new Date().toISOString()),
    widgetName: latestWidget.widgetName || latestWidget.widgetId,
    widgetId: latestWidget.widgetId,
    summaryStats: {
      changedWidgets: changedFields > 0 ? 1 : 0,
      stableWidgets: stableFields > 0 ? 1 : 0,
      topMovers: Math.min(3, topMovers.length),
    },
    widgetDiffs: widgetDiffs,
    stableMetrics: stableMetrics,
    latestData: latestRows,
    previousData: previousRows,
  };
};

const normalizeComparableValue = (value) => {
  // Handle null and undefined as null
  if (value == null) {
    return null;
  }

  // Handle finite numbers directly
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  // Handle strings - try to convert to number if possible
  if (typeof value === "string") {
    const trimmedValue = value.trim();

    // Empty string becomes null
    if (trimmedValue === "") {
      return null;
    }

    // Try to convert to number
    if (!Number.isNaN(Number(trimmedValue))) {
      return Number(trimmedValue);
    }

    return trimmedValue;
  }

  // Handle other types
  return value;
};

const valuesAreEqual = (latestValue, previousValue) => {
  // Handle null and undefined as equivalent
  if (latestValue == null && previousValue == null) {
    return true;
  }

  // Handle exact equality
  if (latestValue === previousValue) {
    return true;
  }

  // Handle numeric comparisons
  if (
    typeof latestValue === "number" &&
    typeof previousValue === "number" &&
    Number.isFinite(latestValue) &&
    Number.isFinite(previousValue)
  ) {
    return latestValue === previousValue;
  }

  // Handle string to number conversions (e.g., "1" === 1)
  const normalizedLatest = normalizeComparableValue(latestValue);
  const normalizedPrevious = normalizeComparableValue(previousValue);

  if (
    typeof normalizedLatest === "number" &&
    typeof normalizedPrevious === "number"
  ) {
    return normalizedLatest === normalizedPrevious;
  }

  // Final fallback to string comparison
  return String(latestValue) === String(previousValue);
};

const detectFieldChangeType = (latestValue, previousValue) => {
  const normalizedLatest = normalizeComparableValue(latestValue);
  const normalizedPrevious = normalizeComparableValue(previousValue);

  if (
    typeof normalizedLatest === "number" &&
    typeof normalizedPrevious === "number" &&
    Number.isFinite(normalizedLatest) &&
    Number.isFinite(normalizedPrevious)
  ) {
    return "numeric";
  }

  return "text";
};

const formatComparisonTimestamp = (timestamp) => {
  const date = new Date(timestamp);

  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  const hours = String(date.getHours()).padStart(2, "0");
  const minutes = String(date.getMinutes()).padStart(2, "0");
  const seconds = String(date.getSeconds()).padStart(2, "0");

  return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
};

// Helper function to extract numeric value from widget data
const extractValue = (data) => {
  if (!data || !Array.isArray(data) || data.length === 0) return null;

  // Try to find a numeric value in the first row
  const firstRow = data[0];
  const values = Object.values(firstRow);

  for (const val of values) {
    if (typeof val === "number") {
      return val;
    }
    if (typeof val === "string" && !isNaN(parseFloat(val))) {
      return parseFloat(val);
    }
  }

  return null;
};

// Helper function to calculate change between values
const calculateChange = (latest, previous) => {
  if (previous === 0) {
    return {
      absoluteChange: latest,
      percentChange: latest > 0 ? 100 : 0,
      isSignificant: latest > 0,
      direction: latest > 0 ? "up" : "stable",
    };
  }

  const absoluteChange = latest - previous;
  const percentChange = (absoluteChange / previous) * 100;

  // Consider significant if change > 2% or absolute change > 100
  const isSignificant =
    Math.abs(percentChange) > 2 || Math.abs(absoluteChange) > 100;

  return {
    absoluteChange,
    percentChange,
    isSignificant,
    direction: percentChange > 0 ? "up" : percentChange < 0 ? "down" : "stable",
  };
};

// Helper function to detect metric type
const detectMetricType = (latest, previous) => {
  // Check if values look like currency (typically have decimals and are large)
  if (latest > 1000 || previous > 1000) {
    return "currency";
  }
  return "count";
};

// Helper function to extract entity from widget data
const extractEntity = (data) => {
  if (!data || !Array.isArray(data) || data.length === 0) return null;

  const firstRow = data[0];
  // Look for common entity field names
  const entityFields = ["name", "title", "category", "type", "game", "entity"];

  for (const field of entityFields) {
    if (firstRow[field] && typeof firstRow[field] === "string") {
      return firstRow[field];
    }
  }

  return null;
};

const dashboardController = {
  async getAllDashboards(req, res) {
    try {
      console.log("Webhook received:", req.body.data.source);

      // Get dashboard data with visuals in one query with source filtering
      const dashboardsWithVisuals = await SQLquery(
        `SELECT 
           d.id as dashboard_id,
           d.name as dashboard_name,
           d.source_id,
           d.account_id,
           d.user_id,
           v.id as visual_id,
           v.report_name as visual_name,
           v.sql_query as visual_sql_query
         FROM bi.dashboard d
         INNER JOIN [abs].[data_source] ds ON d.source_id = ds.id AND ds.enable_flag = 1
         LEFT JOIN bi.widgets v ON d.id = v.dashboard_id AND v.status_id = 2
         WHERE d.status_id = 2
           AND ds.source_id = @param0 AND v.chart_type NOT IN ('table', 'pivot')
         ORDER BY d.created_at DESC, v.created_at ASC`,
        { param0: req.body.data.source.id },
      );

      if (!dashboardsWithVisuals || !dashboardsWithVisuals.length) {
        return res.status(200).json({
          success: true,
          message: "No dashboards found",
          data: [],
        });
      }

      // Group visuals by dashboard
      const dashboardMap = {};
      dashboardsWithVisuals.forEach((row) => {
        const dashboardId = row.dashboard_id;
        if (!dashboardMap[dashboardId]) {
          dashboardMap[dashboardId] = {
            id: row.dashboard_id,
            name: row.dashboard_name,
            source_id: row.source_id,
            user_id: row.user_id,
            account_id: row.account_id,
            visuals: [],
          };
        }

        // Add visual if it exists (LEFT JOIN may produce nulls)
        if (row.visual_id) {
          dashboardMap[dashboardId].visuals.push({
            id: row.visual_id,
            name: row.visual_name,
            sql_query: row.visual_sql_query,
          });
        }
      });

      // Convert to array and process each dashboard
      const dashboards = Object.values(dashboardMap);

      // Initialize token variable
      const userProfile = await fetchUserProfile(dashboards[0]?.user_id);
      let token = null;

      // Load parquet file once for all dashboards
      if (dashboards.length > 0) {
        // Generate JWT token with 1d expiry
        token = generateAuthToken({
          userId: userProfile.user_id,
          accountId: userProfile.default_account,
          email: userProfile.email,
          roleId: userProfile.role_id,
        });

        await axios.post(
          `${DUCKDB_API_BASE_URL}/api/duckdb/loadParquetFile`,
          { source_id: dashboards[0].source_id.toString() },
          {
            headers: {
              "Content-Type": "application/json",
              Authorization: `Bearer ${token}`,
            },
          },
        );
      }

      // Process each dashboard and store data in Blob Storage
      const processedDashboards = await Promise.all(
        dashboards.map(async (dashboard) => {
          let visualData = [];

          if (dashboard.visuals.length > 0) {
            const validVisuals = dashboard.visuals.filter(
              (visual) => visual.sql_query && visual.sql_query.trim() !== "",
            );

            if (validVisuals.length > 0) {
              try {
                const queryResult = await axios.post(
                  `${DUCKDB_API_BASE_URL}/api/duckdb/query/batch`,
                  {
                    source_id: dashboard.source_id.toString(),
                    account_id: dashboard.account_id.toString(),
                    queries: validVisuals.map((visual) => ({
                      sql: visual.sql_query,
                      widgetId: visual.id,
                    })),
                    // options: {},
                  },
                  {
                    headers: {
                      "Content-Type": "application/json",
                      Authorization: `Bearer ${token}`,
                    },
                  },
                );

                visualData = queryResult.data.data.results.map((item) => ({
                  data: item.data,
                  widgetId: item.widgetId,
                  widgetName:
                    validVisuals.find((v) => v.id === item.widgetId)?.name ||
                    `Widget ${item.widgetId}`,
                }));

                // Store visual data in Blob Storage
                const username = userProfile.username; // Use email prefix as username

                await dashboardDataService.storeDashboardData(
                  username,
                  dashboard.source_id,
                  dashboard.name,
                  dashboard.id,
                  visualData,
                );

                // after store data in blob storage - generate AI summary
                try {
                  // Get last 2 entries for this dashboard
                  const blobData = await dashboardDataService.getLastTwoEntries(
                    username,
                    dashboard.source_id,
                    dashboard.name,
                    dashboard.id,
                  );

                  // for dashboard summary
                  if (blobData.length >= 2) {
                    const latest = blobData[0].data || [];
                    const previous = blobData[1].data || [];

                    // Generate comparison JSON for current refresh cycle
                    const comparisonJson = generateComparisonJson(
                      latest,
                      previous,
                    );

                    // Store comparison data in blob storage
                    try {
                      await comparisonDataService.storeComparisonData(
                        username,
                        dashboard.source_id,
                        dashboard.name,
                        dashboard.id,
                        comparisonJson,
                      );
                    } catch (storeError) {
                      console.error(
                        `[DASHBOARD] Failed to store comparison data for dashboard ${dashboard.id}:`,
                        storeError.message,
                      );
                    }

                    // Get last 5 comparison cycles for AI analysis
                    const comparisonCycles =
                      await comparisonDataService.getLastFiveComparisons(
                        username,
                        dashboard.source_id,
                        dashboard.name,
                        dashboard.id,
                      );

                    // Prepare AI prompt with comparison data
                    let comparisonContext = "";
                    if (comparisonCycles.length > 0) {
                      comparisonContext = "Recent refresh cycle comparisons:\n";
                      comparisonCycles.forEach((cycle) => {
                        const timestamp = new Date(
                          cycle.timestamp,
                        ).toLocaleString();
                        comparisonContext += `\n${timestamp}\n${JSON.stringify(cycle.comparisonData, null, 2)}\n`;
                      });
                    }

                    const messages = [
                      {
                        role: "function",
                        content: `Based on the following dashboard refresh cycle comparison data, write a short client-friendly summary in plain English.${comparisonContext}
                            Write only these section titles exactly:
                            **Dashboard Insights**
                            **Key Takeaways**
                            **Focus Areas**
                            **Anomalies**
                            **Outliers**

                            Rules:
                            - Mention only meaningful changes and important stable trends.
                            - Do NOT list every unchanged widget.
                            - If several KPIs are unchanged, summarize them in one short line.
                            - Use actual widgetName values only.
                            - For changed values, show previous and latest values clearly.
                            - Highlight only business-useful metrics such as counts, percentages, amounts, ranking changes, trend shifts, and notable category movement.
                            - Ignore IDs, technical fields, and minor noise.
                            - Keep the summary concise, useful, and easy for non-technical users.
                            - Do not invent reasons or facts not present in the data.
                            - If no meaningful outliers exist, say so briefly.
                            - Focus on what a business user should notice first.

                            Formatting:
                            - Bold important numbers, important metric names, and important changes
                            - No HTML
                            - No markdown other than the required bold section titles
                            - No extra intro or closing text

                            - Return the response as a JSON object with these exact keys:
                            {
                              "dashboardInsights": ["bullet points"],
                              "keyTakeaways": ["bullet points"], 
                              "focusAreas": ["bullet points"],
                              "anomalies": ["bullet points"],
                              "outliers": ["bullet points"]
                            }`,
                        name: "askDatabase",
                      },
                      {
                        role: "user",
                        content:
                          "Do **not** include any troubleshooting steps.",
                      },
                    ];

                    const completion =
                      await azureClient.chat.completions.create({
                        model: "gpt-4o",
                        messages,
                        temperature: 0.2,
                        max_tokens: 300,
                      });

                    const aiSummary =
                      completion.choices[0].message.content.trim();

                    // Update dashboard summary in database
                    try {
                      await SQLquery(
                        `UPDATE bi.dashboard SET dashboard_summary = @param0, summary_displayed = 0, modified_at = CURRENT_TIMESTAMP WHERE id = @param1`,
                        {
                          param0: aiSummary,
                          param1: dashboard.id,
                        },
                      );
                    } catch (updateError) {
                      console.error(
                        `[DASHBOARD] Failed to update summary for dashboard ${dashboard.id}:`,
                        updateError.message,
                      );
                    }
                  }

                  // for widget summary - generate individual summaries for each widget with enhanced comparison
                  if (blobData.length >= 2) {
                    const latest = blobData[0].data || [];
                    const previous = blobData[1].data || [];

                    const latestMap = new Map();
                    const previousMap = new Map();

                    latest.forEach((widget) => {
                      latestMap.set(widget.widgetId, widget);
                    });

                    previous.forEach((widget) => {
                      previousMap.set(widget.widgetId, widget);
                    });

                    for (const [widgetId, latestWidget] of latestMap) {
                      const previousWidget = previousMap.get(widgetId);

                      if (!previousWidget) {
                        continue;
                      }

                      // Generate enhanced widget comparison data with multi-data support
                      const widgetComparisonData =
                        generateEnhancedWidgetComparisonJson(
                          latestWidget,
                          previousWidget,
                        );

                      try {
                        await comparisonDataService.storeWidgetComparisonData(
                          username,
                          dashboard.source_id,
                          dashboard.name,
                          dashboard.id,
                          latestWidget.widgetName || `Widget ${widgetId}`,
                          widgetId,
                          widgetComparisonData,
                        );
                      } catch (storeError) {
                        console.error(
                          `[WIDGET] Failed to store comparison data for widget ${widgetId}:`,
                          storeError.message,
                        );
                      }

                      let widgetComparisonCycles = [];
                      try {
                        widgetComparisonCycles =
                          await comparisonDataService.getLastFiveWidgetComparisons(
                            username,
                            dashboard.source_id,
                            dashboard.name,
                            dashboard.id,
                            latestWidget.widgetName || `Widget ${widgetId}`,
                            widgetId,
                          );
                      } catch (comparisonReadError) {
                        console.error(
                          `[WIDGET] Failed to get comparison history for widget ${widgetId}:`,
                          comparisonReadError.message,
                        );
                      }

                      let comparisonContext = "";
                      if (widgetComparisonCycles.length > 0) {
                        comparisonContext =
                          "Recent widget comparison cycles:\n";
                        widgetComparisonCycles.forEach((cycle) => {
                          comparisonContext += `\n${formatComparisonTimestamp(cycle.timestamp)}\n${JSON.stringify(cycle.comparisonData, null, 2)}\n`;
                        });
                      }

                      const messages = [
                        {
                          role: "function",
                          content: `Compare the latest widget comparison results with the previous snapshot and write a short client-friendly summary in plain English.${comparisonContext}
                            Write only these section titles exactly:
                            **Visual Insights**
                            **Key Takeaways**
                            **Focus Areas**
                            **Anomalies**
                            **Outliers**
                                                    
                            Rules:
                            - Mention only meaningful changes and important stable trends.
                            - Do NOT list every unchanged widget.
                            - If several KPIs are unchanged, summarize them in one short line.
                            - Use actual widgetName values only.
                            - For changed values, show previous and latest values clearly.
                            - Highlight only business-useful metrics such as counts, percentages, amounts, ranking changes, trend shifts, and notable category movement.
                            - Ignore IDs, technical fields, and minor noise.
                            - Keep the summary concise, useful, and easy for non-technical users.
                            - Do not invent reasons or facts not present in the data.
                            - If no meaningful outliers exist, say so briefly.
                            - Focus on what a business user should notice first.
                                                    
                            Formatting:
                            - Use → bullets under Visual Insights and Focus Areas
                            - Use 💡 bullets under Key Takeaways
                            - Use 🔍 bullets under Outliers
                            - Bold important numbers, important metric names, and important changes
                            - No HTML
                            - No markdown other than the required bold section titles
                            - No extra intro or closing text`,
                          name: "askDatabase",
                        },
                        {
                          role: "user",
                          content:
                            "Do **not** include any troubleshooting steps. Use only the provided widget comparison data.",
                        },
                      ];

                      try {
                        const completion =
                          await azureClient.chat.completions.create({
                            model: "gpt-4o",
                            messages,
                            temperature: 0.2,
                            max_tokens: 300,
                          });

                        const aiSummary =
                          completion.choices[0].message.content.trim();

                        // Store widget summary in database (optional - you can add this if needed)
                        await SQLquery(
                          `UPDATE bi.widgets SET widget_summary = @param0, modified_at = CURRENT_TIMESTAMP WHERE id = @param1`,
                          {
                            param0: aiSummary,
                            param1: widgetId,
                          },
                        );
                      } catch (widgetError) {
                        console.error(
                          `[WIDGET] AI summary error for widget ${widgetId}:`,
                          widgetError.message,
                        );
                      }
                    }
                  }
                } catch (aiError) {
                  console.error(
                    `[DASHBOARD] AI summary error for dashboard ${dashboard.id}:`,
                    aiError.message,
                  );
                }
              } catch (queryError) {
                console.error(
                  `[DASHBOARD] Query error for dashboard ${dashboard.id}:`,
                  queryError,
                );

                return res.status(400).json({
                  success: false,
                  message: "Failed to process dashboards",
                  error: queryError.message,
                });
              }
            }
          }

          await SQLquery(
            `UPDATE bi.dashboard_ai_suggestions SET is_status = 0, updated_at = CURRENT_TIMESTAMP WHERE source_id = @param0`,
            {
              param0: dashboard.source_id.toString(),
            },
          );
          return {
            ...dashboard,
            visualDataStored: visualData.length > 0,
            visualCount: visualData.length,
          };
        }),
      );

      res.status(200).json({
        success: true,
        message: `Successfully processed and stored data for ${dashboards.length} dashboards`,
        sourceId: processedDashboards[0].source_id,
        data: processedDashboards.map((dashboard) => dashboard.id),
      });
    } catch (error) {
      console.error("[DASHBOARD] Error in getAllDashboards:", error);

      res.status(500).json({
        success: false,
        message: "Failed to process dashboards",
        error: error.message,
      });
    }
  },
};

module.exports = dashboardController;
