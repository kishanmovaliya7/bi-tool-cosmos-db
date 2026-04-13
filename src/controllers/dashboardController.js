const {
  visualResponseService,
  initializeCosmosClient,
} = require("../services/cosmosService");
const { SQLquery } = require("../services/dbConnect");
const axios = require("axios");
const { azureClient } = require("../services/openaiClient");

const DUCKDB_API_BASE_URL =
  process.env.DUCKDB_API_BASE_URL || "http://localhost:8080";

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
           AND ds.source_id = @param0
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

      // Load parquet file once for all dashboards
      if (dashboards.length > 0) {
        await axios.post(
          `${DUCKDB_API_BASE_URL}/api/duckdb/loadParquetFile`,
          { source_id: dashboards[0].source_id.toString() },
          {
            headers: {
              "Content-Type": "application/json",
              Authorization: "Bearer null",
            },
          },
        );
      }

      // Process each dashboard and store data in CosmosDB
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
                      Authorization: "Bearer null",
                    },
                  },
                );

                visualData = queryResult.data.data.results.map((item) => ({
                  data: item.data,
                  widgetId: item.widgetId,
                  widgetName:
                    validVisuals.find((v) => v.id === item.widgetId)
                      ?.name || `Widget ${item.widgetId}`,
                }));                

                // Store visual data in CosmosDB with dashboard ID as partition key
                await visualResponseService.storeVisualResponse(
                  dashboard.id,
                  visualData,
                );

                // after store data in cosmos db - generate AI summary
                try {
                  // Get last 2 entries for this dashboard
                  const { database } = await initializeCosmosClient();
                  const container = database.container("visual-responses");

                  const sqlquery = {
                    query:
                      "SELECT TOP 2 * FROM c WHERE c.dashboardId = @dashboardId ORDER BY c.createdAt DESC",
                    parameters: [
                      {
                        name: "@dashboardId",
                        value: dashboard.id,
                      },
                    ],
                  };

                  const { resources: cosmosData } = await container.items
                    .query(sqlquery)
                    .fetchAll();

                  if (cosmosData.length >= 2) {
                    const latest = cosmosData[0].queryResult || [];
                    const previous = cosmosData[1].queryResult || [];
                    
                    // Limit data size to prevent token limit exceeded errors
                    const maxDataSize = 50000; // characters limit
                    let limitedLatest = latest;
                    let limitedPrevious = previous;
                    
                    if (JSON.stringify(latest).length > maxDataSize) {
                      limitedLatest = latest.slice(0, 20); // Limit to first 20 widgets
                    }
                    if (JSON.stringify(previous).length > maxDataSize) {
                      limitedPrevious = previous.slice(0, 20); // Limit to first 20 widgets
                    }

                    const messages = [
                      {
                        role: "function",
                        content: `Compare latest and previous dashboard data, then write a short client-friendly summary in plain text. Use exactly these bold section titles only: **Dashboard Insights**, **Key Takeaways**, **Focus Areas**, **Outliers**. Do not number the sections. Use actual widgetName values, not "Widget X". For changed widgets, show old vs new values like "Country Names in Bar Chart: users increased from 27 to 28". Use ➤ for bullet points in Dashboard Insights and Focus Areas sections and use • for all nested points, bold important numbers and changes, use 💡 for takeaways and 🔍 for outliers, focus on meaningful values only (counts, percentages, amounts), not IDs. No HTML, no technical wording, no extra text, no invented facts. Data: Latest=${JSON.stringify(limitedLatest)}, Previous=${JSON.stringify(limitedPrevious)}`,
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

                    console.log("aisummary-----", aiSummary);

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
