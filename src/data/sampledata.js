// data.js
export const warehouseData = [
  {
    id: 1,
    warehouseName: "PROD_DW",
    totalQueries: 1250,
    totalCreditsConsumed: 450.75,
    activeHoursPerDay: 8.2,
    idleCreditBurnRate: 15.2,
    queries0To1Sec: 200,
    queries1To10Sec: 650,
    queries10To30Sec: 250,
    queries30To60Sec: 80,
    queries1To5Min: 50,
    queries5MinPlus: 20,
    overProvisionedQueries: 18,
    underProvisionedQueries: 12,
    selectStarQueries: 78,
    unpartitionedScanQueries: 45,
    cartesianJoinQueries: 3,
    zeroResultQueries: 42,
    failedCancelledQueries: 15,
    repeatedQueries: 85,
    highCompileTimeQueries: 22,
    spilledToLocalQueries: 23,
    spilledToRemoteQueries: 18,
    weekendIdleCredits: 28.5,
    peakHourCostPct: 42.3,
    singleUserMonopolization: "analyst1@company.com (38%)",
    queueWaitTimeAvgMs: 1250
  },
  {
    id: 2,
    warehouseName: "DEV_ANALYTICS",
    totalQueries: 650,
    totalCreditsConsumed: 120.25,
    activeHoursPerDay: 4.5,
    idleCreditBurnRate: 8.3,
    queries0To1Sec: 150,
    queries1To10Sec: 350,
    queries10To30Sec: 100,
    queries30To60Sec: 30,
    queries1To5Min: 15,
    queries5MinPlus: 5,
    overProvisionedQueries: 8,
    underProvisionedQueries: 5,
    selectStarQueries: 42,
    unpartitionedScanQueries: 22,
    cartesianJoinQueries: 1,
    zeroResultQueries: 25,
    failedCancelledQueries: 8,
    repeatedQueries: 45,
    highCompileTimeQueries: 12,
    spilledToLocalQueries: 12,
    spilledToRemoteQueries: 8,
    weekendIdleCredits: 12.8,
    peakHourCostPct: 28.5,
    singleUserMonopolization: "dev1@company.com (42%)",
    queueWaitTimeAvgMs: 850
  }
];

export const databaseData = [
  {
    id: 1,
    databaseName: "SALES_DB",
    totalStorageGB: 850,
    tableCount: 25,
    storageGrowthGBPerWeek: 25.5,
    timeTravelStorageGB: 42.8,
    crossDatabaseQueries: 35,
    selectStarOnLargeTables: 68,
    unpartitionedScansCount: 45,
    fullTableScanQueries: 120,
    expensiveAggregations: 28,
    unusedTablesCount: 8,
    zombieTablesStorageGB: 125,
    unclusteredLargeTables: 12,
    tablesWithoutPrimaryKey: 5,
    wideTablesCount: 3
  },
  {
    id: 2,
    databaseName: "MARKETING_DB",
    totalStorageGB: 320,
    tableCount: 18,
    storageGrowthGBPerWeek: 12.2,
    timeTravelStorageGB: 22.5,
    crossDatabaseQueries: 18,
    selectStarOnLargeTables: 32,
    unpartitionedScansCount: 22,
    fullTableScanQueries: 65,
    expensiveAggregations: 15,
    unusedTablesCount: 5,
    zombieTablesStorageGB: 65,
    unclusteredLargeTables: 6,
    tablesWithoutPrimaryKey: 2,
    wideTablesCount: 1
  }
];

export const tableData = [
  {
    id: 1,
    tableName: "customer_transactions",
    databaseName: "SALES_DB",
    storageSizeGB: 250,
    rowCount: 125000000,
    totalScansCount: 1200,
    fullTableScansCount: 85,
    selectStarQueriesCount: 45,
    unfilteredQueriesCount: 120,
    queriesPerDay: 180,
    lastAccessedDaysAgo: 1,
    partitionPruningEfficiencyPct: 65,
    cartesianJoinsInvolving: 3,
    expensiveAggregationsCount: 25,
    clusteringRatio: 0.35
  },
  {
    id: 2,
    tableName: "user_activity_logs",
    databaseName: "MARKETING_DB",
    storageSizeGB: 85,
    rowCount: 42000000,
    totalScansCount: 850,
    fullTableScansCount: 45,
    selectStarQueriesCount: 28,
    unfilteredQueriesCount: 75,
    queriesPerDay: 125,
    lastAccessedDaysAgo: 3,
    partitionPruningEfficiencyPct: 85,
    cartesianJoinsInvolving: 1,
    expensiveAggregationsCount: 15,
    clusteringRatio: 0.82
  }
];

export const serverlessData = [
  {
    id: 1,
    serviceName: "Snowpipe_Loader",
    serviceType: "SNOWPIPE",
    totalCreditsConsumed: 85.5,
    executionsCount: 12500,
    successRatePct: 98.5,
    filesProcessedCount: 12500,
    errorRatePct: 1.5,
    duplicateFilesProcessed: 85
  },
  {
    id: 2,
    serviceName: "Data_Validation_Task",
    serviceType: "TASK",
    totalCreditsConsumed: 22.3,
    executionsCount: 720,
    successRatePct: 97.2,
    taskRunsCount: 720,
    failedTaskCreditsWasted: 2.8,
    suspendedTasksCount: 3
  },
  {
    id: 3,
    serviceName: "Customer_Stream",
    serviceType: "STREAM",
    totalCreditsConsumed: 15.2,
    executionsCount: 365,
    successRatePct: 99.1,
    streamLagMinutesAvg: 8.5,
    streamOffsetResets: 3
  }
];

export const userData = [
  {
    id: 1,
    userName: "analyst1@company.com",
    warehouseName: "PROD_DW",
    queryCountInCategory: 45,
    creditsConsumed: 85.5,
    percentageOfWarehouseUsage: 38,
    specificQueriesCount: 45
  },
  {
    id: 2,
    userName: "dev1@company.com",
    warehouseName: "DEV_ANALYTICS",
    queryCountInCategory: 28,
    creditsConsumed: 42.3,
    percentageOfWarehouseUsage: 42,
    specificQueriesCount: 28
  },
  {
    id: 3,
    userName: "bi_engineer@company.com",
    warehouseName: "PROD_DW",
    queryCountInCategory: 32,
    creditsConsumed: 65.2,
    percentageOfWarehouseUsage: 28,
    specificQueriesCount: 32
  }
];

export const queryHistoryData = [
  {
    id: 1,
    queryId: "01a32bcd-1234-5678-90ef-ghijklmnopqr",
    userName: "analyst1@company.com",
    warehouseName: "PROD_DW",
    queryTextPreview: "SELECT * FROM customer_transactions ct JOIN products p ON ct.product_id = p.id WHE...",
    startTime: "2023-08-04 09:15:32",
    executionTimeMs: 125000,
    bytesScanned: "2.5 TB",
    creditsUsed: 0.85,
    errorCode: null
  },
  {
    id: 2,
    queryId: "02b43cde-2345-6789-01fg-hijklmnopqrs",
    userName: "dev1@company.com",
    warehouseName: "DEV_ANALYTICS",
    queryTextPreview: "SELECT COUNT(*) FROM user_activity_logs WHERE event_date > '2023-07-01' GROUP BY...",
    startTime: "2023-08-04 11:22:15",
    executionTimeMs: 45000,
    bytesScanned: "850 GB",
    creditsUsed: 0.42,
    errorCode: "02000"
  },
  {
    id: 3,
    queryId: "03c54def-3456-7890-12gh-ijklmnopqrst",
    userName: "bi_engineer@company.com",
    warehouseName: "PROD_DW",
    queryTextPreview: "WITH customer_summary AS (SELECT customer_id, SUM(amount) AS total_spent FROM cus...",
    startTime: "2023-08-04 14:35:48",
    executionTimeMs: 185000,
    bytesScanned: "3.2 TB",
    creditsUsed: 1.25,
    errorCode: null
  }
];

export const queryDetailsData = {
  "01a32bcd-1234-5678-90ef-ghijklmnopqr": {
    queryId: "01a32bcd-1234-5678-90ef-ghijklmnopqr",
    fullQueryText: "SELECT * \nFROM customer_transactions ct \nJOIN products p ON ct.product_id = p.id \nWHERE ct.transaction_date > '2023-01-01' \nORDER BY ct.transaction_date DESC",
    executionPlan: "Full table scan on customer_transactions (2.5TB), Hash Join with products table, Sort on transaction_date",
    queryProfileUrl: "https://snowflake.prod/query/01a32bcd-1234-5678-90ef-ghijklmnopqr",
    compilationTime: 1250,
    queueTime: 320,
    rowsProduced: 1250000,
    partitionsScanned: 0,
    spillageDetails: {
      local: "0 GB",
      remote: "4.2 GB"
    },
    costBreakdown: {
      computeCredits: 0.75,
      storageCredits: 0.10,
      totalCost: "$3.85"
    },
    optimizationRecommendations: [
      "Add partition filter on transaction_date",
      "Use specific columns instead of SELECT *",
      "Cluster customer_transactions table on product_id",
      "Consider materialized view for frequent joins"
    ]
  },
  "02b43cde-2345-6789-01fg-hijklmnopqrs": {
    queryId: "02b43cde-2345-6789-01fg-hijklmnopqrs",
    fullQueryText: "SELECT COUNT(*) AS total_events, user_id \nFROM user_activity_logs \nWHERE event_date > '2023-07-01' \nGROUP BY user_id \nHAVING COUNT(*) > 1000 \nORDER BY total_events DESC",
    executionPlan: "Full table scan on user_activity_logs (850GB), Hash Aggregate, Filter, Sort",
    queryProfileUrl: "https://snowflake.prod/query/02b43cde-2345-6789-01fg-hijklmnopqrs",
    compilationTime: 850,
    queueTime: 150,
    rowsProduced: 12500,
    partitionsScanned: 0,
    spillageDetails: {
      local: "1.2 GB",
      remote: "0 GB"
    },
    costBreakdown: {
      computeCredits: 0.42,
      storageCredits: 0.05,
      totalCost: "$1.88"
    },
    optimizationRecommendations: [
      "Add date partitioning to event_date column",
      "Create aggregation table for daily counts",
      "Use approximate count for large datasets",
      "Add clustering key on user_id"
    ]
  }
};