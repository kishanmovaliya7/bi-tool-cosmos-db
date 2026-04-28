const { widgetDataService, comparisonDataService } = require("../services/blobStorageService");
const { SQLquery } = require("../services/dbConnect");
const axios = require("axios");
const { azureClient } = require("../services/openaiClient");
const {
  generateAuthToken,
  fetchUserProfile,
} = require("../helpers/authHelper");

const DUCKDB_API_BASE_URL =
  process.env.DUCKDB_API_BASE_URL || "http://localhost:8080";


// Helper function to generate comparison JSON between refresh cycles
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

  // Compare widgets
  for (const [widgetId, latestWidget] of latestMap) {
    const previousWidget = previousMap.get(widgetId);

    if (!previousWidget) {
      // New widget, skip for now
      continue;
    }

    const latestValue = extractValue(latestWidget.data);
    const previousValue = extractValue(previousWidget.data);

    if (latestValue !== null && previousValue !== null) {
      const change = calculateChange(latestValue, previousValue);

      if (change.isSignificant) {
        changedWidgets++;
        const diff = {
          widgetName: latestWidget.widgetName,
          latestValue: latestValue,
          previousValue: previousValue,
          absoluteChange: change.absoluteChange,
          percentChange: change.percentChange,
          isSignificant: change.isSignificant,
          direction: change.direction,
          metricType: detectMetricType(latestValue, previousValue),
        };

        // Add entity if available
        const entity = extractEntity(latestWidget.data);
        if (entity) {
          diff.entity = entity;
        }

        widgetDiffs.push(diff);

        // Track top movers (top 3 by percent change)
        if (Math.abs(change.percentChange) > 5) {
          topMovers.push({
            widgetName: latestWidget.widgetName,
            percentChange: change.percentChange,
          });
        }
      } else {
        stableWidgetsCount++;
        stableMetrics.push(latestWidget.widgetName);
      }
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
                
                await widgetDataService.storeWidgetData(
                  username,
                  dashboard.source_id,
                  dashboard.name,
                  dashboard.id,
                  visualData
                );

                // after store data in blob storage - generate AI summary
                try {
                  // Get last 5 entries for this dashboard
                  const blobData = await widgetDataService.getLastFiveEntries(
                    username,
                    dashboard.source_id,
                    dashboard.name,
                    dashboard.id
                  );

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
                        comparisonJson
                      );
                    } catch (storeError) {
                      console.error(
                        `[DASHBOARD] Failed to store comparison data for dashboard ${dashboard.id}:`,
                        storeError.message,
                      );
                    }

                    // Get last 5 comparison cycles for AI analysis
                    const comparisonCycles = await comparisonDataService.getLastFiveComparisons(
                      username,
                      dashboard.source_id,
                      dashboard.name,
                      dashboard.id
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
