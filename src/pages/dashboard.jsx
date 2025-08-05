import React, { useState, useContext, createContext } from 'react';
import { Search, Filter, Download, ArrowRight, Home, Database, Server, Users, Layers, Activity, AlertTriangle, TrendingUp, Clock, HardDrive, X, ChevronDown, BarChart3, Zap, Eye } from 'lucide-react';

// Context for drill-down state management
const DrillDownContext = createContext();

// Sample data - Complete dataset
const sampleWarehouseData = [
  {
    warehouseName: "COMPUTE_WH_LARGE",
    totalQueries: 1250,
    totalCredits: 450.75,
    activeHoursPerDay: 18.5,
    idleCreditBurnRate: 2.3,
    queries0To1Sec: 200,
    queries1To10Sec: 800,
    queries10To30Sec: 300,
    queries30To60Sec: 120,
    queries1To5Min: 25,
    queries5MinPlus: 5,
    overProvisionedQueries: 85,
    underProvisionedQueries: 32,
    selectStarQueries: 78,
    unpartitionedScanQueries: 45,
    cartesianJoinQueries: 12,
    zeroResultQueries: 156,
    failedCancelledQueries: 23,
    repeatedQueries: 89,
    highCompileTimeQueries: 34,
    spilledToLocalQueries: 23,
    spilledToRemoteQueries: 8,
    weekendIdleCredits: 125.5,
    peakHourCostPct: 65,
    singleUserMonopolization: 35,
    queueWaitTimeAvgMs: 450
  },
  {
    warehouseName: "COMPUTE_WH_MEDIUM",
    totalQueries: 890,
    totalCredits: 220.30,
    activeHoursPerDay: 12.2,
    idleCreditBurnRate: 1.8,
    queries0To1Sec: 350,
    queries1To10Sec: 420,
    queries10To30Sec: 100,
    queries30To60Sec: 18,
    queries1To5Min: 2,
    queries5MinPlus: 0,
    overProvisionedQueries: 45,
    underProvisionedQueries: 15,
    selectStarQueries: 34,
    unpartitionedScanQueries: 23,
    cartesianJoinQueries: 3,
    zeroResultQueries: 89,
    failedCancelledQueries: 12,
    repeatedQueries: 67,
    highCompileTimeQueries: 19,
    spilledToLocalQueries: 8,
    spilledToRemoteQueries: 2,
    weekendIdleCredits: 45.2,
    peakHourCostPct: 42,
    singleUserMonopolization: 18,
    queueWaitTimeAvgMs: 200
  },
  {
    warehouseName: "COMPUTE_WH_SMALL",
    totalQueries: 456,
    totalCredits: 89.45,
    activeHoursPerDay: 8.5,
    idleCreditBurnRate: 0.8,
    queries0To1Sec: 400,
    queries1To10Sec: 50,
    queries10To30Sec: 6,
    queries30To60Sec: 0,
    queries1To5Min: 0,
    queries5MinPlus: 0,
    overProvisionedQueries: 12,
    underProvisionedQueries: 8,
    selectStarQueries: 15,
    unpartitionedScanQueries: 8,
    cartesianJoinQueries: 1,
    zeroResultQueries: 34,
    failedCancelledQueries: 5,
    repeatedQueries: 23,
    highCompileTimeQueries: 7,
    spilledToLocalQueries: 2,
    spilledToRemoteQueries: 0,
    weekendIdleCredits: 15.2,
    peakHourCostPct: 25,
    singleUserMonopolization: 12,
    queueWaitTimeAvgMs: 50
  }
];

const sampleDatabaseData = [
  {
    databaseName: "SALES_DB",
    totalStorageGb: 2340.5,
    tableCount: 156,
    storageGrowthGbPerWeek: 45.2,
    timeTravelStorageGb: 890.3,
    crossDatabaseQueries: 234,
    selectStarOnLargeTables: 78,
    unpartitionedScansCount: 145,
    fullTableScanQueries: 89,
    expensiveAggregations: 56,
    unusedTablesCount: 23,
    zombieTablesStorageGb: 156.7,
    unclusteredLargeTables: 12,
    tablesWithoutPrimaryKey: 8
  },
  {
    databaseName: "MARKETING_DB",
    totalStorageGb: 890.2,
    tableCount: 67,
    storageGrowthGbPerWeek: 12.8,
    timeTravelStorageGb: 234.1,
    crossDatabaseQueries: 89,
    selectStarOnLargeTables: 34,
    unpartitionedScansCount: 67,
    fullTableScanQueries: 45,
    expensiveAggregations: 23,
    unusedTablesCount: 8,
    zombieTablesStorageGb: 45.3,
    unclusteredLargeTables: 5,
    tablesWithoutPrimaryKey: 3
  },
  {
    databaseName: "FINANCE_DB",
    totalStorageGb: 456.8,
    tableCount: 89,
    storageGrowthGbPerWeek: 8.5,
    timeTravelStorageGb: 123.4,
    crossDatabaseQueries: 45,
    selectStarOnLargeTables: 12,
    unpartitionedScansCount: 34,
    fullTableScanQueries: 23,
    expensiveAggregations: 15,
    unusedTablesCount: 5,
    zombieTablesStorageGb: 23.4,
    unclusteredLargeTables: 3,
    tablesWithoutPrimaryKey: 2
  }
];

const sampleTableData = [
  {
    tableName: "SALES_TRANSACTIONS",
    databaseName: "SALES_DB",
    storageSizeGb: 567.8,
    rowCount: 45000000,
    totalScansCount: 234,
    fullTableScansCount: 45,
    selectStarQueriesCount: 23,
    unfilteredQueriesCount: 67,
    queriesPerDay: 125,
    lastAccessedDaysAgo: 0,
    partitionPruningEfficiencyPct: 65,
    cartesianJoinsInvolving: 3,
    expensiveAggregationsCount: 12,
    clusteringRatio: 78
  },
  {
    tableName: "CUSTOMER_MASTER",
    databaseName: "SALES_DB",
    storageSizeGb: 234.5,
    rowCount: 12000000,
    totalScansCount: 456,
    fullTableScansCount: 89,
    selectStarQueriesCount: 56,
    unfilteredQueriesCount: 123,
    queriesPerDay: 89,
    lastAccessedDaysAgo: 1,
    partitionPruningEfficiencyPct: 45,
    cartesianJoinsInvolving: 8,
    expensiveAggregationsCount: 23,
    clusteringRatio: 34
  },
  {
    tableName: "PRODUCT_CATALOG",
    databaseName: "SALES_DB",
    storageSizeGb: 89.3,
    rowCount: 500000,
    totalScansCount: 123,
    fullTableScansCount: 12,
    selectStarQueriesCount: 8,
    unfilteredQueriesCount: 34,
    queriesPerDay: 45,
    lastAccessedDaysAgo: 2,
    partitionPruningEfficiencyPct: 85,
    cartesianJoinsInvolving: 1,
    expensiveAggregationsCount: 5,
    clusteringRatio: 92
  },
  {
    tableName: "CAMPAIGN_DATA",
    databaseName: "MARKETING_DB",
    storageSizeGb: 156.7,
    rowCount: 2000000,
    totalScansCount: 89,
    fullTableScansCount: 23,
    selectStarQueriesCount: 15,
    unfilteredQueriesCount: 45,
    queriesPerDay: 34,
    lastAccessedDaysAgo: 0,
    partitionPruningEfficiencyPct: 72,
    cartesianJoinsInvolving: 2,
    expensiveAggregationsCount: 8,
    clusteringRatio: 67
  }
];

const sampleServerlessData = [
  {
    serviceName: "SALES_DATA_PIPE",
    serviceType: "SNOWPIPE",
    totalCreditsConsumed: 45.67,
    executionsCount: 1245,
    successRatePct: 98.5,
    filesProcessedCount: 2340,
    errorRatePct: 1.5,
    duplicateFilesProcessed: 23
  },
  {
    serviceName: "DAILY_AGGREGATION_TASK",
    serviceType: "TASKS",
    totalCreditsConsumed: 123.45,
    executionsCount: 720,
    successRatePct: 95.2,
    taskRunsCount: 720,
    failedTaskCreditsWasted: 12.34,
    suspendedTasksCount: 2
  },
  {
    serviceName: "CUSTOMER_STREAM",
    serviceType: "STREAMS",
    totalCreditsConsumed: 67.89,
    executionsCount: 8760,
    successRatePct: 99.8,
    streamLagMinutesAvg: 2.5,
    streamOffsetResets: 3
  }
];

const sampleUserData = [
  {
    userName: "john.doe@company.com",
    warehouseName: "COMPUTE_WH_LARGE",
    queryCountInCategory: 25,
    creditsConsumed: 450.75,
    percentageOfWarehouseUsage: 35.2,
    specificQueriesCount: 25
  },
  {
    userName: "jane.smith@company.com",
    warehouseName: "COMPUTE_WH_LARGE",
    queryCountInCategory: 20,
    creditsConsumed: 380.50,
    percentageOfWarehouseUsage: 28.4,
    specificQueriesCount: 20
  },
  {
    userName: "mike.johnson@company.com",
    warehouseName: "COMPUTE_WH_LARGE",
    queryCountInCategory: 15,
    creditsConsumed: 234.20,
    percentageOfWarehouseUsage: 18.7,
    specificQueriesCount: 15
  },
  {
    userName: "sarah.wilson@company.com",
    warehouseName: "COMPUTE_WH_MEDIUM",
    queryCountInCategory: 12,
    creditsConsumed: 156.80,
    percentageOfWarehouseUsage: 22.3,
    specificQueriesCount: 12
  }
];

const sampleQueryHistory = [
  {
    queryId: "01234567-89ab-cdef-0123-456789abcdef",
    userName: "john.doe@company.com",
    warehouseName: "COMPUTE_WH_LARGE",
    queryTextPreview: "SELECT * FROM sales_transactions WHERE date >= '2024-01-01' AND region = 'US'...",
    startTime: "2024-08-04 14:30:25",
    executionTimeMs: 15420,
    bytesScanned: 2340000000,
    creditsUsed: 0.045,
    errorCode: null,
    fullQueryText: "SELECT * FROM sales_transactions WHERE date >= '2024-01-01' AND region = 'US' ORDER BY transaction_amount DESC LIMIT 1000",
    compilationTime: 234,
    queueTime: 45,
    rowsProduced: 1000,
    partitionsScanned: 45,
    spillageDetails: "No spillage",
    costBreakdown: "Compute: $0.043, Storage: $0.002"
  },
  {
    queryId: "11234567-89ab-cdef-0123-456789abcdef",
    userName: "john.doe@company.com",
    warehouseName: "COMPUTE_WH_LARGE",
    queryTextPreview: "SELECT * FROM customer_master c JOIN sales_transactions s ON c.id = s.customer_id...",
    startTime: "2024-08-04 13:15:10",
    executionTimeMs: 28540,
    bytesScanned: 5670000000,
    creditsUsed: 0.089,
    errorCode: null,
    fullQueryText: "SELECT * FROM customer_master c JOIN sales_transactions s ON c.id = s.customer_id WHERE s.date >= '2024-07-01'",
    compilationTime: 456,
    queueTime: 123,
    rowsProduced: 15000,
    partitionsScanned: 89,
    spillageDetails: "Local disk: 2.3GB",
    costBreakdown: "Compute: $0.085, Storage: $0.004"
  },
  {
    queryId: "21234567-89ab-cdef-0123-456789abcdef",
    userName: "john.doe@company.com",
    warehouseName: "COMPUTE_WH_LARGE",
    queryTextPreview: "SELECT COUNT(*) FROM sales_transactions WHERE 1=1...",
    startTime: "2024-08-04 12:45:33",
    executionTimeMs: 67890,
    bytesScanned: 12340000000,
    creditsUsed: 0.234,
    errorCode: null,
    fullQueryText: "SELECT COUNT(*) FROM sales_transactions WHERE 1=1",
    compilationTime: 123,
    queueTime: 678,
    rowsProduced: 1,
    partitionsScanned: 156,
    spillageDetails: "Remote disk: 8.7GB",
    costBreakdown: "Compute: $0.220, Storage: $0.014"
  }
];

// Utility function to format numbers
const formatNumber = (num) => {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toString();
};

// Utility function to get severity color
const getSeverityColor = (value, thresholds) => {
  if (value >= thresholds.high) return 'text-red-400';
  if (value >= thresholds.medium) return 'text-orange-400';
  return 'text-green-400';
};

// Clickable Metric Component
const ClickableMetric = ({ value, onClick, severity = 'normal', suffix = '', prefix = '' }) => {
  const colorClass = severity === 'high' ? 'text-red-400 hover:text-red-300' :
                    severity === 'medium' ? 'text-orange-400 hover:text-orange-300' :
                    'text-blue-400 hover:text-blue-300';
  
  return (
    <button
      onClick={onClick}
      className={`${colorClass} hover:underline font-medium transition-colors duration-200 hover:bg-gray-700 px-2 py-1 rounded`}
    >
      {prefix}{formatNumber(value)}{suffix}
    </button>
  );
};

// Breadcrumb Component
const BreadcrumbTrail = ({ path, onNavigate }) => {
  return (
    <div className="flex items-center space-x-2 mb-6 text-sm text-gray-400 bg-gray-800 p-3 rounded-lg">
      <button 
        onClick={() => onNavigate(-1)}
        className="flex items-center hover:text-blue-400 transition-colors"
      >
        <Home className="w-4 h-4 mr-1" />
        Dashboard
      </button>
      {path.map((item, index) => (
        <React.Fragment key={index}>
          <ArrowRight className="w-3 h-3" />
          <button
            onClick={() => onNavigate(index)}
            className="hover:text-blue-400 transition-colors"
          >
            {item.label}
          </button>
        </React.Fragment>
      ))}
    </div>
  );
};

// Warehouse Metrics Table
const WarehouseTable = ({ onDrillDown }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: null, direction: 'asc' });

  const handleSort = (key) => {
    setSortConfig({
      key,
      direction: sortConfig.key === key && sortConfig.direction === 'asc' ? 'desc' : 'asc'
    });
  };

  const filteredData = sampleWarehouseData.filter(warehouse =>
    warehouse.warehouseName.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const sortedData = [...filteredData].sort((a, b) => {
    if (!sortConfig.key) return 0;
    const aVal = a[sortConfig.key];
    const bVal = b[sortConfig.key];
    return sortConfig.direction === 'asc' ? aVal - bVal : bVal - aVal;
  });

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-bold text-white flex items-center">
          <Server className="w-6 h-6 mr-2 text-blue-400" />
          Warehouse Metrics
        </h2>
        <div className="flex space-x-3">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <input 
              type="text"
              placeholder="Search warehouses..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 pr-4 py-2 bg-gray-700 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-400"
            />
          </div>
          <button className="flex items-center space-x-2 px-4 py-2 bg-gray-700 rounded-lg hover:bg-gray-600 transition-colors">
            <Download className="w-4 h-4" />
            <span>Export</span>
          </button>
        </div>
      </div>
      
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left p-3 text-gray-300">Warehouse</th>
              <th className="text-left p-3 text-gray-300 cursor-pointer hover:text-white" onClick={() => handleSort('totalQueries')}>
                Total Queries {sortConfig.key === 'totalQueries' && <ChevronDown className="w-3 h-3 inline" />}
              </th>
              <th className="text-left p-3 text-gray-300 cursor-pointer hover:text-white" onClick={() => handleSort('totalCredits')}>
                Credits {sortConfig.key === 'totalCredits' && <ChevronDown className="w-3 h-3 inline" />}
              </th>
              <th className="text-left p-3 text-gray-300">Active Hours/Day</th>
              <th className="text-left p-3 text-gray-300">Performance Issues</th>
              <th className="text-left p-3 text-gray-300">Bad Practices</th>
              <th className="text-left p-3 text-gray-300">Cost Issues</th>
            </tr>
          </thead>
          <tbody>
            {sortedData.map((warehouse, index) => (
              <tr key={index} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
                <td className="p-3 text-white font-medium">{warehouse.warehouseName}</td>
                <td className="p-3">
                  <ClickableMetric 
                    value={warehouse.totalQueries}
                    onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'total_queries', warehouse.totalQueries)}
                  />
                </td>
                <td className="p-3">
                  <ClickableMetric 
                    value={warehouse.totalCredits}
                    onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'total_credits', warehouse.totalCredits)}
                    severity="medium"
                    prefix="$"
                  />
                </td>
                <td className="p-3 text-gray-300">{warehouse.activeHoursPerDay}h</td>
                <td className="p-3 space-y-1">
                  <div className="flex space-x-2">
                    <ClickableMetric 
                      value={warehouse.spilledToLocalQueries + warehouse.spilledToRemoteQueries}
                      onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'spilled_queries', warehouse.spilledToLocalQueries + warehouse.spilledToRemoteQueries)}
                      severity="medium"
                      suffix=" spilled"
                    />
                  </div>
                  <div className="flex space-x-2">
                    <ClickableMetric 
                      value={warehouse.queueWaitTimeAvgMs}
                      onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'queue_wait', warehouse.queueWaitTimeAvgMs)}
                      severity={warehouse.queueWaitTimeAvgMs > 300 ? 'high' : 'normal'}
                      suffix="ms queue"
                    />
                  </div>
                </td>
                <td className="p-3 space-y-1">
                  <div className="flex space-x-2">
                    <ClickableMetric 
                      value={warehouse.selectStarQueries}
                      onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'select_star_queries', warehouse.selectStarQueries)}
                      severity="high"
                      suffix=" SELECT *"
                    />
                  </div>
                  <div className="flex space-x-2">
                    <ClickableMetric 
                      value={warehouse.unpartitionedScanQueries}
                      onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'unpartitioned_scan_queries', warehouse.unpartitionedScanQueries)}
                      severity="high"
                      suffix=" unpart."
                    />
                  </div>
                  <div className="flex space-x-2">
                    <ClickableMetric 
                      value={warehouse.failedCancelledQueries}
                      onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'failed_cancelled_queries', warehouse.failedCancelledQueries)}
                      severity="high"
                      suffix=" failed"
                    />
                  </div>
                </td>
                <td className="p-3 space-y-1">
                  <div className="flex space-x-2">
                    <ClickableMetric 
                      value={warehouse.weekendIdleCredits}
                      onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'weekend_idle', warehouse.weekendIdleCredits)}
                      severity="medium"
                      prefix="$"
                      suffix=" idle"
                    />
                  </div>
                  <div className="flex space-x-2">
                    <ClickableMetric 
                      value={warehouse.singleUserMonopolization}
                      onClick={() => onDrillDown('warehouse', warehouse.warehouseName, 'monopolization', warehouse.singleUserMonopolization)}
                      severity={warehouse.singleUserMonopolization > 30 ? 'high' : 'medium'}
                      suffix="% monopolized"
                    />
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// Table Metrics Table
const TableTable = ({ onDrillDown }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [filterDatabase, setFilterDatabase] = useState('');

  const filteredData = sampleTableData.filter(table =>
    table.tableName.toLowerCase().includes(searchTerm.toLowerCase()) &&
    (filterDatabase === '' || table.databaseName === filterDatabase)
  );

  const uniqueDatabases = [...new Set(sampleTableData.map(t => t.databaseName))];

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-bold text-white flex items-center">
          <Layers className="w-6 h-6 mr-2 text-purple-400" />
          Table Metrics
        </h2>
        <div className="flex space-x-3">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <input 
              type="text"
              placeholder="Search tables..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 pr-4 py-2 bg-gray-700 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-400"
            />
          </div>
          <select 
            value={filterDatabase}
            onChange={(e) => setFilterDatabase(e.target.value)}
            className="px-4 py-2 bg-gray-700 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            <option value="">All Databases</option>
            {uniqueDatabases.map(db => (
              <option key={db} value={db}>{db}</option>
            ))}
          </select>
          <button className="flex items-center space-x-2 px-4 py-2 bg-gray-700 rounded-lg hover:bg-gray-600 transition-colors">
            <Download className="w-4 h-4" />
            <span>Export</span>
          </button>
        </div>
      </div>
      
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left p-3 text-gray-300">Table</th>
              <th className="text-left p-3 text-gray-300">Database</th>
              <th className="text-left p-3 text-gray-300">Size & Rows</th>
              <th className="text-left p-3 text-gray-300">Access Patterns</th>
              <th className="text-left p-3 text-gray-300">Performance Issues</th>
              <th className="text-left p-3 text-gray-300">Last Access</th>
            </tr>
          </thead>
          <tbody>
            {filteredData.map((table, index) => (
              <tr key={index} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
                <td className="p-3 text-white font-medium">{table.tableName}</td>
                <td className="p-3">
                  <span className="px-2 py-1 bg-purple-900 text-purple-300 rounded text-xs">
                    {table.databaseName}
                  </span>
                </td>
                <td className="p-3">
                  <ClickableMetric 
                    value={table.storageSizeGb}
                    onClick={() => onDrillDown('table', table.tableName, 'storage_size', table.storageSizeGb)}
                    suffix=" GB"
                  />
                  <div className="text-xs text-gray-400 mt-1">
                    {formatNumber(table.rowCount)} rows
                  </div>
                </td>
                <td className="p-3 space-y-1">
                  <div>
                    <ClickableMetric 
                      value={table.fullTableScansCount}
                      onClick={() => onDrillDown('table', table.tableName, 'full_table_scans', table.fullTableScansCount)}
                      severity="high"
                      suffix=" full scans"
                    />
                  </div>
                  <div>
                    <ClickableMetric 
                      value={table.selectStarQueriesCount}
                      onClick={() => onDrillDown('table', table.tableName, 'select_star_queries', table.selectStarQueriesCount)}
                      severity="medium"
                      suffix=" SELECT *"
                    />
                  </div>
                  <div>
                    <ClickableMetric 
                      value={table.unfilteredQueriesCount}
                      onClick={() => onDrillDown('table', table.tableName, 'unfiltered_queries', table.unfilteredQueriesCount)}
                      severity="medium"
                      suffix=" unfiltered"
                    />
                  </div>
                </td>
                <td className="p-3 space-y-1">
                  <div>
                    <span className="text-gray-300">Clustering: </span>
                    <span className={`font-medium ${table.clusteringRatio > 80 ? 'text-green-400' : table.clusteringRatio > 60 ? 'text-orange-400' : 'text-red-400'}`}>
                      {table.clusteringRatio}%
                    </span>
                  </div>
                  <div>
                    <span className="text-gray-300">Pruning: </span>
                    <span className={`font-medium ${table.partitionPruningEfficiencyPct > 80 ? 'text-green-400' : table.partitionPruningEfficiencyPct > 60 ? 'text-orange-400' : 'text-red-400'}`}>
                      {table.partitionPruningEfficiencyPct}%
                    </span>
                  </div>
                  <div>
                    <ClickableMetric 
                      value={table.cartesianJoinsInvolving}
                      onClick={() => onDrillDown('table', table.tableName, 'cartesian_joins', table.cartesianJoinsInvolving)}
                      severity={table.cartesianJoinsInvolving > 5 ? 'high' : table.cartesianJoinsInvolving > 0 ? 'medium' : 'normal'}
                      suffix=" cartesian joins"
                    />
                  </div>
                </td>
                <td className="p-3">
                  <span className={`${table.lastAccessedDaysAgo === 0 ? 'text-green-400' : table.lastAccessedDaysAgo < 7 ? 'text-orange-400' : 'text-red-400'}`}>
                    {table.lastAccessedDaysAgo === 0 ? 'Today' : `${table.lastAccessedDaysAgo} days ago`}
                  </span>
                  <div className="text-xs text-gray-400 mt-1">
                    {table.queriesPerDay} queries/day
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// Serverless Services Table
const ServerlessTable = ({ onDrillDown }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [filterType, setFilterType] = useState('');

  const filteredData = sampleServerlessData.filter(service =>
    service.serviceName.toLowerCase().includes(searchTerm.toLowerCase()) &&
    (filterType === '' || service.serviceType === filterType)
  );

  const uniqueTypes = [...new Set(sampleServerlessData.map(s => s.serviceType))];

  return (
    <div className="bg-gray-800 rounded-lg p-6">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-bold text-white flex items-center">
          <Activity className="w-6 h-6 mr-2 text-cyan-400" />
          Serverless Services
        </h2>
        <div className="flex space-x-3">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <input 
              type="text"
              placeholder="Search services..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 pr-4 py-2 bg-gray-700 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-400"
            />
          </div>
          <select 
            value={filterType}
            onChange={(e) => setFilterType(e.target.value)}
            className="px-4 py-2 bg-gray-700 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-400"
          >
            <option value="">All Types</option>
            {uniqueTypes.map(type => (
              <option key={type} value={type}>{type}</option>
            ))}
          </select>
          <button className="flex items-center space-x-2 px-4 py-2 bg-gray-700 rounded-lg hover:bg-gray-600 transition-colors">
            <Download className="w-4 h-4" />
            <span>Export</span>
          </button>
        </div>
      </div>
      
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700">
              <th className="text-left p-3 text-gray-300">Service</th>
              <th className="text-left p-3 text-gray-300">Type</th>
              <th className="text-left p-3 text-gray-300">Credits</th>
              <th className="text-left p-3 text-gray-300">Executions</th>
              <th className="text-left p-3 text-gray-300">Success Rate</th>
              <th className="text-left p-3 text-gray-300">Service Details</th>
            </tr>
          </thead>
          <tbody>
            {filteredData.map((service, index) => (
              <tr key={index} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
                <td className="p-3 text-white font-medium">{service.serviceName}</td>
                <td className="p-3">
                  <span className={`px-2 py-1 rounded text-xs ${
                    service.serviceType === 'SNOWPIPE' ? 'bg-cyan-900 text-cyan-300' :
                    service.serviceType === 'TASKS' ? 'bg-blue-900 text-blue-300' :
                    'bg-purple-900 text-purple-300'
                  }`}>
                    {service.serviceType}
                  </span>
                </td>
                <td className="p-3">
                  <ClickableMetric 
                    value={service.totalCreditsConsumed}
                    onClick={() => onDrillDown('serverless', service.serviceName, 'credits', service.totalCreditsConsumed)}
                    prefix="$"
                  />
                </td>
                <td className="p-3">
                  <ClickableMetric 
                    value={service.executionsCount}
                    onClick={() => onDrillDown('serverless', service.serviceName, 'executions', service.executionsCount)}
                  />
                </td>
                <td className="p-3">
                  <span className={`font-medium ${
                    service.successRatePct >= 99 ? 'text-green-400' :
                    service.successRatePct >= 95 ? 'text-orange-400' : 'text-red-400'
                  }`}>
                    {service.successRatePct}%
                  </span>
                </td>
                <td className="p-3 space-y-1">
                  {service.serviceType === 'SNOWPIPE' && (
                    <>
                      <div>
                        <ClickableMetric 
                          value={service.filesProcessedCount}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'files', service.filesProcessedCount)}
                          suffix=" files processed"
                        />
                      </div>
                      <div>
                        <ClickableMetric 
                          value={service.duplicateFilesProcessed}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'duplicates', service.duplicateFilesProcessed)}
                          severity="medium"
                          suffix=" duplicates"
                        />
                      </div>
                      <div>
                        <ClickableMetric 
                          value={service.errorRatePct}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'errors', service.errorRatePct)}
                          severity={service.errorRatePct > 2 ? 'high' : 'medium'}
                          suffix="% error rate"
                        />
                      </div>
                    </>
                  )}
                  {service.serviceType === 'TASKS' && (
                    <>
                      <div>
                        <ClickableMetric 
                          value={service.taskRunsCount}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'task_runs', service.taskRunsCount)}
                          suffix=" task runs"
                        />
                      </div>
                      <div>
                        <ClickableMetric 
                          value={service.failedTaskCreditsWasted}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'failed_credits', service.failedTaskCreditsWasted)}
                          severity="high"
                          prefix="$"
                          suffix=" wasted"
                        />
                      </div>
                      <div>
                        <ClickableMetric 
                          value={service.suspendedTasksCount}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'suspended', service.suspendedTasksCount)}
                          severity="high"
                          suffix=" suspended"
                        />
                      </div>
                    </>
                  )}
                  {service.serviceType === 'STREAMS' && (
                    <>
                      <div>
                        <span className="text-gray-300">Avg Lag: </span>
                        <ClickableMetric 
                          value={service.streamLagMinutesAvg}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'stream_lag', service.streamLagMinutesAvg)}
                          severity={service.streamLagMinutesAvg > 5 ? 'high' : 'normal'}
                          suffix=" min"
                        />
                      </div>
                      <div>
                        <ClickableMetric 
                          value={service.streamOffsetResets}
                          onClick={() => onDrillDown('serverless', service.serviceName, 'offset_resets', service.streamOffsetResets)}
                          severity={service.streamOffsetResets > 1 ? 'medium' : 'normal'}
                          suffix=" offset resets"
                        />
                      </div>
                    </>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// User Breakdown Modal
const UserBreakdownModal = ({ isOpen, onClose, drillDownData, onUserClick }) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-gray-800 rounded-lg p-6 max-w-5xl w-full mx-4 max-h-[80vh] overflow-auto">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-xl font-bold text-white flex items-center">
            <Users className="w-5 h-5 mr-2" />
            User Breakdown - {drillDownData?.metric?.replace(/_/g, ' ').toUpperCase()} on {drillDownData?.entityName}
          </h3>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="bg-blue-900 bg-opacity-30 p-4 rounded-lg mb-6">
          <div className="flex items-center text-blue-300">
            <AlertTriangle className="w-4 h-4 mr-2" />
            <span className="font-medium">Impact Analysis</span>
          </div>
          <p className="text-blue-200 text-sm mt-2">
            This breakdown shows which users are contributing to the "{drillDownData?.metric?.replace(/_/g, ' ')}" issue. 
            Focus on users with high query counts and credit consumption for maximum impact.
          </p>
        </div>
        
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="text-left p-3 text-gray-300">User</th>
                <th className="text-left p-3 text-gray-300">Query Count</th>
                <th className="text-left p-3 text-gray-300">Credits Consumed</th>
                <th className="text-left p-3 text-gray-300">% of Total Usage</th>
                <th className="text-left p-3 text-gray-300">Impact Level</th>
                <th className="text-left p-3 text-gray-300">Actions</th>
              </tr>
            </thead>
            <tbody>
              {sampleUserData.map((user, index) => (
                <tr key={index} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
                  <td className="p-3">
                    <div className="flex items-center">
                      <div className="w-8 h-8 bg-blue-600 rounded-full flex items-center justify-center text-white text-xs font-medium mr-3">
                        {user.userName.charAt(0).toUpperCase()}
                      </div>
                      <div>
                        <div className="text-white font-medium">{user.userName}</div>
                        <div className="text-gray-400 text-xs">{user.warehouseName}</div>
                      </div>
                    </div>
                  </td>
                  <td className="p-3">
                    <span className="text-blue-400 font-medium">{user.queryCountInCategory}</span>
                  </td>
                  <td className="p-3">
                    <span className="text-orange-400 font-medium">${user.creditsConsumed}</span>
                  </td>
                  <td className="p-3">
                    <div className="flex items-center">
                      <div className="w-16 bg-gray-700 rounded-full h-2 mr-2">
                        <div 
                          className={`h-2 rounded-full ${
                            user.percentageOfWarehouseUsage > 30 ? 'bg-red-400' :
                            user.percentageOfWarehouseUsage > 20 ? 'bg-orange-400' : 'bg-green-400'
                          }`}
                          style={{ width: `${Math.min(user.percentageOfWarehouseUsage * 2, 100)}%` }}
                        />
                      </div>
                      <span className="text-gray-300 text-sm">{user.percentageOfWarehouseUsage}%</span>
                    </div>
                  </td>
                  <td className="p-3">
                    <span className={`px-2 py-1 rounded text-xs font-medium ${
                      user.percentageOfWarehouseUsage > 30 ? 'bg-red-900 text-red-300' :
                      user.percentageOfWarehouseUsage > 20 ? 'bg-orange-900 text-orange-300' :
                      'bg-green-900 text-green-300'
                    }`}>
                      {user.percentageOfWarehouseUsage > 30 ? 'HIGH' :
                       user.percentageOfWarehouseUsage > 20 ? 'MEDIUM' : 'LOW'}
                    </span>
                  </td>
                  <td className="p-3">
                    <button
                      onClick={() => onUserClick(user)}
                      className="flex items-center space-x-1 text-blue-400 hover:text-blue-300 underline transition-colors"
                    >
                      <Eye className="w-3 h-3" />
                      <span>View Queries</span>
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="mt-6 flex justify-between items-center">
          <div className="text-sm text-gray-400">
            Showing {sampleUserData.length} users contributing to this issue
          </div>
          <div className="flex space-x-2">
            <button className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors text-sm">
              Export Report
            </button>
            <button className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors text-sm">
              Generate Recommendations
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

// Query History Modal
const QueryHistoryModal = ({ isOpen, onClose, userData, onQueryClick }) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-gray-800 rounded-lg p-6 max-w-7xl w-full mx-4 max-h-[80vh] overflow-auto">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-xl font-bold text-white flex items-center">
            <BarChart3 className="w-5 h-5 mr-2" />
            Query History - {userData?.userName}
          </h3>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        <div className="bg-orange-900 bg-opacity-30 p-4 rounded-lg mb-6">
          <div className="flex items-center text-orange-300">
            <Zap className="w-4 h-4 mr-2" />
            <span className="font-medium">Query Performance Summary</span>
          </div>
          <div className="grid grid-cols-4 gap-4 mt-3 text-sm">
            <div>
              <div className="text-orange-200">Total Queries</div>
              <div className="text-white font-medium">{userData?.queryCountInCategory}</div>
            </div>
            <div>
              <div className="text-orange-200">Credits Consumed</div>
              <div className="text-white font-medium">${userData?.creditsConsumed}</div>
            </div>
            <div>
              <div className="text-orange-200">Avg Credits/Query</div>
              <div className="text-white font-medium">${(userData?.creditsConsumed / userData?.queryCountInCategory).toFixed(3)}</div>
            </div>
            <div>
              <div className="text-orange-200">Warehouse Usage</div>
              <div className="text-white font-medium">{userData?.percentageOfWarehouseUsage}%</div>
            </div>
          </div>
        </div>
        
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="text-left p-3 text-gray-300">Query ID</th>
                <th className="text-left p-3 text-gray-300">Query Preview</th>
                <th className="text-left p-3 text-gray-300">Start Time</th>
                <th className="text-left p-3 text-gray-300">Duration</th>
                <th className="text-left p-3 text-gray-300">Credits</th>
                <th className="text-left p-3 text-gray-300">Data Scanned</th>
                <th className="text-left p-3 text-gray-300">Status</th>
                <th className="text-left p-3 text-gray-300">Actions</th>
              </tr>
            </thead>
            <tbody>
              {sampleQueryHistory.map((query, index) => (
                <tr key={index} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
                  <td className="p-3">
                    <span className="text-gray-300 font-mono text-xs bg-gray-700 px-2 py-1 rounded">
                      {query.queryId.substring(0, 8)}...
                    </span>
                  </td>
                  <td className="p-3 max-w-md">
                    <div className="text-white truncate" title={query.queryTextPreview}>
                      {query.queryTextPreview}
                    </div>
                  </td>
                  <td className="p-3 text-gray-300 text-xs">{query.startTime}</td>
                  <td className="p-3">
                    <span className={`font-medium ${
                      query.executionTimeMs > 30000 ? 'text-red-400' :
                      query.executionTimeMs > 10000 ? 'text-orange-400' : 'text-green-400'
                    }`}>
                      {query.executionTimeMs < 1000 ? `${query.executionTimeMs}ms` : `${(query.executionTimeMs/1000).toFixed(1)}s`}
                    </span>
                  </td>
                  <td className="p-3">
                    <span className={`font-medium ${
                      query.creditsUsed > 0.1 ? 'text-red-400' :
                      query.creditsUsed > 0.05 ? 'text-orange-400' : 'text-green-400'
                    }`}>
                      ${query.creditsUsed}
                    </span>
                  </td>
                  <td className="p-3 text-gray-300">{formatNumber(query.bytesScanned)} bytes</td>
                  <td className="p-3">
                    <span className={`px-2 py-1 rounded text-xs ${
                      query.errorCode ? 'bg-red-900 text-red-300' : 'bg-green-900 text-green-300'
                    }`}>
                      {query.errorCode ? 'ERROR' : 'SUCCESS'}
                    </span>
                  </td>
                  <td className="p-3">
                    <button
                      onClick={() => onQueryClick(query)}
                      className="flex items-center space-x-1 text-blue-400 hover:text-blue-300 underline transition-colors"
                    >
                      <Eye className="w-3 h-3" />
                      <span>Details</span>
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="mt-6 flex justify-between items-center">
          <div className="text-sm text-gray-400">
            Showing {sampleQueryHistory.length} queries for {userData?.userName}
          </div>
          <div className="flex space-x-2">
            <button 
              onClick={onClose}
              className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors text-sm"
            >
              Back to Users
            </button>
            <button className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors text-sm">
              Export Query History
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};

// Query Details Modal
const QueryDetailsModal = ({ isOpen, onClose, queryData }) => {
  if (!isOpen) return null;

  const getOptimizationRecommendations = (query) => {
    const recommendations = [];
    
    if (query.queryTextPreview.includes('SELECT *')) {
      recommendations.push({
        type: 'high',
        title: 'Avoid SELECT *',
        description: 'Use specific column names instead of SELECT * to reduce data transfer and improve performance.',
        impact: 'High - Can reduce data scanned by 60-80%'
      });
    }
    
    if (query.executionTimeMs > 30000) {
      recommendations.push({
        type: 'medium',
        title: 'Long Execution Time',
        description: 'Consider adding filters, improving clustering, or optimizing joins to reduce execution time.',
        impact: 'Medium - Can improve query speed by 40-60%'
      });
    }
    
    if (query.spillageDetails.includes('GB')) {
      recommendations.push({
        type: 'high',
        title: 'Memory Spillage Detected',
        description: 'Query is spilling to disk. Consider using a larger warehouse or optimizing the query.',
        impact: 'High - Can reduce costs by 30-50%'
      });
    }
    
    if (query.queueTime > 500) {
      recommendations.push({
        type: 'medium',
        title: 'High Queue Time',
        description: 'Consider using auto-scaling warehouses or distributing queries across time.',
        impact: 'Medium - Can reduce waiting time significantly'
      });
    }
    
    return recommendations;
  };

  const recommendations = getOptimizationRecommendations(queryData);

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-gray-800 rounded-lg p-6 max-w-6xl w-full mx-4 max-h-[90vh] overflow-auto">
        <div className="flex items-center justify-between mb-6">
          <h3 className="text-xl font-bold text-white flex items-center">
            <Search className="w-5 h-5 mr-2" />
            Query Details & Analysis
          </h3>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>
        
        <div className="space-y-6">
          {/* Query Information */}
          <div className="bg-gray-100 rounded-lg p-4">
            <h4 className="text-lg font-semibold text-white mb-4 flex items-center">
              <Database className="w-4 h-4 mr-2" />
              Query Information
            </h4>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div className="space-y-2">
                <div>
                  <span className="text-gray-400">Query ID:</span>
                  <span className="text-white ml-2 font-mono text-xs bg-gray-800 px-2 py-1 rounded">
                    {queryData?.queryId}
                  </span>
                </div>
                <div>
                  <span className="text-gray-400">User:</span>
                  <span className="text-white ml-2">{queryData?.userName}</span>
                </div>
                <div>
                  <span className="text-gray-400">Warehouse:</span>
                  <span className="text-blue-400 ml-2">{queryData?.warehouseName}</span>
                </div>
              </div>
              <div className="space-y-2">
                <div>
                  <span className="text-gray-400">Start Time:</span>
                  <span className="text-white ml-2">{queryData?.startTime}</span>
                </div>
                <div>
                  <span className="text-gray-400">Status:</span>
                  <span className={`ml-2 px-2 py-1 rounded text-xs ${
                    queryData?.errorCode ? 'bg-red-900 text-red-300' : 'bg-green-900 text-green-300'
                  }`}>
                    {queryData?.errorCode ? `ERROR: ${queryData.errorCode}` : 'SUCCESS'}
                  </span>
                </div>
              </div>
            </div>
          </div>

          {/* Performance Metrics */}
          <div className="bg-gray-100 rounded-lg p-4">
            <h4 className="text-lg font-semibold text-white mb-4 flex items-center">
              <BarChart3 className="w-4 h-4 mr-2" />
              Performance Metrics
            </h4>
            <div className="grid grid-cols-3 gap-4 text-sm">
              <div className="bg-gray-800 p-4 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-gray-400">Execution Time</div>
                  <Clock className="w-4 h-4 text-blue-400" />
                </div>
                <div className="text-2xl font-bold text-blue-400">
                  {queryData?.executionTimeMs < 1000 ? 
                    `${queryData?.executionTimeMs}ms` : 
                    `${(queryData?.executionTimeMs/1000).toFixed(1)}s`
                  }
                </div>
                <div className={`text-xs mt-1 ${
                  queryData?.executionTimeMs > 30000 ? 'text-red-400' :
                  queryData?.executionTimeMs > 10000 ? 'text-orange-400' : 'text-green-400'
                }`}>
                  {queryData?.executionTimeMs > 30000 ? 'Slow' :
                   queryData?.executionTimeMs > 10000 ? 'Medium' : 'Fast'}
                </div>
              </div>
              
              <div className="bg-gray-800 p-4 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-gray-400">Queue Time</div>
                  <Clock className="w-4 h-4 text-orange-400" />
                </div>
                <div className="text-2xl font-bold text-orange-400">{queryData?.queueTime}ms</div>
                <div className={`text-xs mt-1 ${
                  queryData?.queueTime > 500 ? 'text-red-400' : 'text-green-400'
                }`}>
                  {queryData?.queueTime > 500 ? 'High contention' : 'Normal'}
                </div>
              </div>
              
              <div className="bg-gray-800 p-4 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-gray-400">Compile Time</div>
                  <Zap className="w-4 h-4 text-yellow-400" />
                </div>
                <div className="text-2xl font-bold text-yellow-400">{queryData?.compilationTime}ms</div>
                <div className={`text-xs mt-1 ${
                  queryData?.compilationTime > 300 ? 'text-red-400' : 'text-green-400'
                }`}>
                  {queryData?.compilationTime > 300 ? 'Complex query' : 'Normal'}
                </div>
              </div>
              
              <div className="bg-gray-800 p-4 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-gray-400">Credits Used</div>
                  <TrendingUp className="w-4 h-4 text-red-400" />
                </div>
                <div className="text-2xl font-bold text-red-400">${queryData?.creditsUsed}</div>
                <div className={`text-xs mt-1 ${
                  queryData?.creditsUsed > 0.1 ? 'text-red-400' :
                  queryData?.creditsUsed > 0.05 ? 'text-orange-400' : 'text-green-400'
                }`}>
                  {queryData?.creditsUsed > 0.1 ? 'Expensive' :
                   queryData?.creditsUsed > 0.05 ? 'Moderate' : 'Cheap'}
                </div>
              </div>
              
              <div className="bg-gray-800 p-4 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-gray-400">Rows Produced</div>
                  <Layers className="w-4 h-4 text-green-400" />
                </div>
                <div className="text-2xl font-bold text-green-400">{formatNumber(queryData?.rowsProduced)}</div>
                <div className="text-xs text-green-300 mt-1">Result set size</div>
              </div>
              
              <div className="bg-gray-800 p-4 rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <div className="text-gray-400">Partitions Scanned</div>
                  <HardDrive className="w-4 h-4 text-purple-400" />
                </div>
                <div className="text-2xl font-bold text-purple-400">{queryData?.partitionsScanned}</div>
                <div className={`text-xs mt-1 ${
                  queryData?.partitionsScanned > 100 ? 'text-red-400' :
                  queryData?.partitionsScanned > 50 ? 'text-orange-400' : 'text-green-400'
                }`}>
                  {queryData?.partitionsScanned > 100 ? 'Poor pruning' :
                   queryData?.partitionsScanned > 50 ? 'Moderate pruning' : 'Good pruning'}
                </div>
              </div>
            </div>
          </div>

          {/* Query Text */}
          <div className="bg-gray-100 rounded-lg p-4">
            <h4 className="text-lg font-semibold text-white mb-4 flex items-center">
              <Search className="w-4 h-4 mr-2" />
              Query Text
            </h4>
            <div className="bg-gray-800 rounded-lg p-4 overflow-x-auto">
              <pre className="text-sm text-gray-300 whitespace-pre-wrap">
                {queryData?.fullQueryText}
              </pre>
            </div>
          </div>

          {/* Cost & Resource Breakdown */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-gray-100 rounded-lg p-4">
              <h4 className="text-lg font-semibold text-white mb-4 flex items-center">
                <TrendingUp className="w-4 h-4 mr-2" />
                Cost Breakdown
              </h4>
              <div className="space-y-3 text-sm">
                <div className="flex justify-between items-center">
                  <span className="text-gray-400">Total Cost:</span>
                  <span className="text-white font-medium">${queryData?.creditsUsed}</span>
                </div>
                <div className="text-gray-300">
                  <div className="text-xs text-gray-400 mb-1">Breakdown:</div>
                  <div className="ml-2">{queryData?.costBreakdown}</div>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-400">Data Scanned:</span>
                  <span className="text-white">{formatNumber(queryData?.bytesScanned)} bytes</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-400">Spillage:</span>
                  <span className={`${queryData?.spillageDetails === 'No spillage' ? 'text-green-400' : 'text-red-400'}`}>
                    {queryData?.spillageDetails}
                  </span>
                </div>
              </div>
            </div>

            <div className="bg-gray-100 rounded-lg p-4">
              <h4 className="text-lg font-semibold text-white mb-4 flex items-center">
                <Activity className="w-4 h-4 mr-2" />
                Resource Usage
              </h4>
              <div className="space-y-3 text-sm">
                <div className="flex justify-between items-center">
                  <span className="text-gray-400">CPU Time:</span>
                  <span className="text-white">{(queryData?.executionTimeMs - queryData?.queueTime)}ms</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-400">Memory Usage:</span>
                  <span className={`${queryData?.spillageDetails === 'No spillage' ? 'text-green-400' : 'text-orange-400'}`}>
                    {queryData?.spillageDetails === 'No spillage' ? 'Within limits' : 'Exceeded - Spilled'}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-400">I/O Operations:</span>
                  <span className="text-white">{queryData?.partitionsScanned} partition reads</span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-400">Network Transfer:</span>
                  <span className="text-white">{formatNumber(queryData?.bytesScanned)} bytes</span>
                </div>
              </div>
            </div>
          </div>

          {/* Optimization Recommendations */}
          {recommendations.length > 0 && (
            <div className="bg-gray-100 rounded-lg p-4">
              <h4 className="text-lg font-semibold text-white mb-4 flex items-center">
                <AlertTriangle className="w-4 h-4 mr-2" />
                Optimization Recommendations
              </h4>
              <div className="space-y-4">
                {recommendations.map((rec, index) => (
                  <div key={index} className={`p-4 rounded-lg border-l-4 ${
                    rec.type === 'high' ? 'bg-red-900 bg-opacity-20 border-red-400' :
                    'bg-orange-900 bg-opacity-20 border-orange-400'
                  }`}>
                    <div className="flex items-start justify-between">
                      <div className="flex-1">
                        <h5 className={`font-medium ${
                          rec.type === 'high' ? 'text-red-300' : 'text-orange-300'
                        }`}>
                          {rec.title}
                        </h5>
                        <p className="text-gray-300 text-sm mt-1">{rec.description}</p>
                        <p className={`text-xs mt-2 ${
                          rec.type === 'high' ? 'text-red-400' : 'text-orange-400'
                        }`}>
                          💡 {rec.impact}
                        </p>
                      </div>
                      <span className={`px-2 py-1 rounded text-xs font-medium ${
                        rec.type === 'high' ? 'bg-red-800 text-red-200' : 'bg-orange-800 text-orange-200'
                      }`}>
                        {rec.type.toUpperCase()}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex justify-between items-center pt-4 border-t border-gray-700">
            <div className="text-sm text-gray-400">
              Query executed on {queryData?.startTime}
            </div>
            <div className="flex space-x-3">
              <button 
                onClick={onClose}
                className="px-4 py-2 bg-gray-700 hover:bg-gray-600 rounded-lg transition-colors text-sm"
              >
                Back to History
              </button>
              <button className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors text-sm">
                View Query Profile
              </button>
              <button className="px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg transition-colors text-sm">
                Save Optimization Report
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

// Main Dashboard Component
const FinOpsDashboard = () => {
  const [activeTab, setActiveTab] = useState('warehouses');
  const [drillDownPath, setDrillDownPath] = useState([]);
  const [showUserBreakdown, setShowUserBreakdown] = useState(false);
  const [showQueryHistory, setShowQueryHistory] = useState(false);
  const [showQueryDetails, setShowQueryDetails] = useState(false);
  const [currentDrillDown, setCurrentDrillDown] = useState(null);
  const [currentUser, setCurrentUser] = useState(null);
  const [currentQuery, setCurrentQuery] = useState(null);

  const tabs = [
    { id: 'warehouses', label: 'Warehouses', icon: Server, color: 'blue' },
    { id: 'databases', label: 'Databases', icon: Database, color: 'green' },
    { id: 'tables', label: 'Tables', icon: Layers, color: 'purple' },
    { id: 'serverless', label: 'Serverless', icon: Activity, color: 'cyan' },
  ];

  const handleDrillDown = (entityType, entityName, metric, value) => {
    const newPath = [...drillDownPath, {
      label: `${entityName} - ${metric.replace(/_/g, ' ')}`,
      entityType,
      entityName,
      metric,
      value
    }];
    setDrillDownPath(newPath);
    setCurrentDrillDown({ entityType, entityName, metric, value });
    setShowUserBreakdown(true);
  };

  const handleUserClick = (user) => {
    setCurrentUser(user);
    setShowUserBreakdown(false);
    setShowQueryHistory(true);
  };

  const handleQueryClick = (query) => {
    setCurrentQuery(query);
    setShowQueryHistory(false);
    setShowQueryDetails(true);
  };

  const handleBreadcrumbNavigate = (index) => {
    if (index === -1) {
      // Navigate to root
      setDrillDownPath([]);
      closeAllModals();
    } else {
      // Navigate to specific level
      setDrillDownPath(drillDownPath.slice(0, index + 1));
    }
  };

  const closeAllModals = () => {
    setShowUserBreakdown(false);
    setShowQueryHistory(false);
    setShowQueryDetails(false);
    setCurrentDrillDown(null);
    setCurrentUser(null);
    setCurrentQuery(null);
  };

  const renderTabContent = () => {
    switch (activeTab) {
      case 'warehouses':
        return <WarehouseTable onDrillDown={handleDrillDown} />;
      case 'databases':
        // return <DatabaseTable onDrillDown={handleDrillDown} />;
      case 'tables':
        return <TableTable onDrillDown={handleDrillDown} />;
      case 'serverless':
        return <ServerlessTable onDrillDown={handleDrillDown} />;
      default:
        return <WarehouseTable onDrillDown={handleDrillDown} />;
    }
  };

  // Calculate summary metrics
  const summaryMetrics = {
    totalCredits: sampleWarehouseData.reduce((sum, w) => sum + w.totalCredits, 0),
    activeWarehouses: sampleWarehouseData.length,
    totalStorage: sampleDatabaseData.reduce((sum, d) => sum + d.totalStorageGb, 0),
    problemQueries: sampleWarehouseData.reduce((sum, w) => sum + w.selectStarQueries + w.unpartitionedScanQueries + w.failedCancelledQueries, 0)
  };

  return (
    <DrillDownContext.Provider value={{ drillDownPath, setDrillDownPath }}>
      <div className="min-h-screen bg-gray-100 text-white">
        {/* Header */}
        <div className="bg-gray-800 border-b border-gray-700 p-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-white">FinOps Dashboard</h1>
              <p className="text-gray-400 mt-1">Comprehensive Snowflake cost and performance analysis</p>
            </div>
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2 text-sm">
                <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
                <span className="text-gray-400">Live data - Last updated: 2 minutes ago</span>
              </div>
              <button className="bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded-lg transition-colors text-sm font-medium">
                Refresh Data
              </button>
            </div>
          </div>
        </div>

        {/* Navigation Tabs */}
        <div className="bg-gray-800 border-b border-gray-700 px-6">
          <div className="flex space-x-8">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => {
                    setActiveTab(tab.id);
                    closeAllModals();
                    setDrillDownPath([]);
                  }}
                  className={`flex items-center space-x-2 py-4 px-2 border-b-2 transition-colors ${
                    activeTab === tab.id
                      ? `border-${tab.color}-400 text-${tab.color}-400`
                      : 'border-transparent text-gray-400 hover:text-white'
                  }`}
                >
                  <Icon className="w-5 h-5" />
                  <span className="font-medium">{tab.label}</span>
                </button>
              );
            })}
          </div>
        </div>

        {/* Main Content */}
        <div className="p-6">
          {/* Breadcrumb */}
          {drillDownPath.length > 0 && (
            <BreadcrumbTrail 
              path={drillDownPath}
              onNavigate={handleBreadcrumbNavigate}
            />
          )}

          {/* Summary Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-6">
            <div className="bg-gradient-to-br from-blue-900 to-blue-800 rounded-lg p-6 border border-blue-700">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-blue-200 text-sm font-medium">Total Credits</p>
                  <p className="text-3xl font-bold text-white">${summaryMetrics.totalCredits.toFixed(0)}</p>
                  <p className="text-green-300 text-sm mt-1">↓ 5.2% from last week</p>
                </div>
                <div className="bg-blue-700 p-3 rounded-full">
                  <TrendingUp className="w-8 h-8 text-blue-200" />
                </div>
              </div>
            </div>
            
            <div className="bg-gradient-to-br from-green-900 to-green-800 rounded-lg p-6 border border-green-700">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-green-200 text-sm font-medium">Active Warehouses</p>
                  <p className="text-3xl font-bold text-white">{summaryMetrics.activeWarehouses}</p>
                  <p className="text-orange-300 text-sm mt-1">2 over-provisioned</p>
                </div>
                <div className="bg-green-700 p-3 rounded-full">
                  <Server className="w-8 h-8 text-green-200" />
                </div>
              </div>
            </div>
            
            <div className="bg-gradient-to-br from-purple-900 to-purple-800 rounded-lg p-6 border border-purple-700">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-purple-200 text-sm font-medium">Storage Usage</p>
                  <p className="text-3xl font-bold text-white">{(summaryMetrics.totalStorage/1000).toFixed(1)} TB</p>
                  <p className="text-orange-300 text-sm mt-1">Growing 66 GB/week</p>
                </div>
                <div className="bg-purple-700 p-3 rounded-full">
                  <HardDrive className="w-8 h-8 text-purple-200" />
                </div>
              </div>
            </div>
            
            <div className="bg-gradient-to-br from-red-900 to-red-800 rounded-lg p-6 border border-red-700">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-red-200 text-sm font-medium">Problem Queries</p>
                  <p className="text-3xl font-bold text-white">{summaryMetrics.problemQueries}</p>
                  <p className="text-red-300 text-sm mt-1">23 high-impact</p>
                </div>
                <div className="bg-red-700 p-3 rounded-full">
                  <AlertTriangle className="w-8 h-8 text-red-200" />
                </div>
              </div>
            </div>
          </div>

          {/* Main Table */}
          {renderTabContent()}
        </div>

        {/* Modals */}
        <UserBreakdownModal
          isOpen={showUserBreakdown}
          onClose={() => setShowUserBreakdown(false)}
          drillDownData={currentDrillDown}
          onUserClick={handleUserClick}
        />

        <QueryHistoryModal
          isOpen={showQueryHistory}
          onClose={() => {
            setShowQueryHistory(false);
            setShowUserBreakdown(true);
          }}
          userData={currentUser}
          onQueryClick={handleQueryClick}
        />

        <QueryDetailsModal
          isOpen={showQueryDetails}
          onClose={() => {
            setShowQueryDetails(false);
            setShowQueryHistory(true);
          }}
          queryData={currentQuery}
        />
      </div>
    </DrillDownContext.Provider>
  );
};

export default FinOpsDashboard;
// )}
//           </tbody>
//         </table>
//       </div>
//     </div>
//   );
// };

// // Database Metrics Table
// const DatabaseTable = ({ onDrillDown }) => {
//   const [searchTerm, setSearchTerm] = useState('');

//   const filteredData = sampleDatabaseData.filter(database =>
//     database.databaseName.toLowerCase().includes(searchTerm.toLowerCase())
//   );

//   return (
//     <div className="bg-gray-800 rounded-lg p-6">
//       <div className="flex items-center justify-between mb-6">
//         <h2 className="text-2xl font-bold text-white flex items-center">
//           <Database className="w-6 h-6 mr-2 text-green-400" />
//           Database Metrics
//         </h2>
//         <div className="flex space-x-3">
//           <div className="relative">
//             <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
//             <input 
//               type="text"
//               placeholder="Search databases..."
//               value={searchTerm}
//               onChange={(e) => setSearchTerm(e.target.value)}
//               className="pl-10 pr-4 py-2 bg-gray-700 rounded-lg text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-400"
//             />
//           </div>
//           <button className="flex items-center space-x-2 px-4 py-2 bg-gray-700 rounded-lg hover:bg-gray-600 transition-colors">
//             <Download className="w-4 h-4" />
//             <span>Export</span>
//           </button>
//         </div>
//       </div>
      
//       <div className="overflow-x-auto">
//         <table className="w-full text-sm">
//           <thead>
//             <tr className="border-b border-gray-700">
//               <th className="text-left p-3 text-gray-300">Database</th>
//               <th className="text-left p-3 text-gray-300">Storage</th>
//               <th className="text-left p-3 text-gray-300">Tables</th>
//               <th className="text-left p-3 text-gray-300">Growth/Week</th>
//               <th className="text-left p-3 text-gray-300">Query Patterns</th>
//               <th className="text-left p-3 text-gray-300">Table Health Issues</th>
//             </tr>
//           </thead>
//           <tbody>
//             {filteredData.map((database, index) => (
//               <tr key={index} className="border-b border-gray-700 hover:bg-gray-750 transition-colors">
//                 <td className="p-3 text-white font-medium">{database.databaseName}</td>
//                 <td className="p-3">
//                   <ClickableMetric 
//                     value={database.totalStorageGb}
//                     onClick={() => onDrillDown('database', database.databaseName, 'total_storage', database.totalStorageGb)}
//                     suffix=" GB"
//                   />
//                   <div className="text-xs text-gray-400 mt-1">
//                     Time Travel: {database.timeTravelStorageGb} GB
//                   </div>
//                 </td>
//                 <td className="p-3">
//                   <ClickableMetric 
//                     value={database.tableCount}
//                     onClick={() => onDrillDown('database', database.databaseName, 'table_count', database.tableCount)}
//                   />
//                 </td>
//                 <td className="p-3 text-gray-300">+{database.storageGrowthGbPerWeek} GB</td>
//                 <td className="p-3 space-y-1">
//                   <div>
//                     <ClickableMetric 
//                       value={database.fullTableScanQueries}
//                       onClick={() => onDrillDown('database', database.databaseName, 'full_table_scans', database.fullTableScanQueries)}
//                       severity="high"
//                       suffix=" full scans"
//                     />
//                   </div>
//                   <div>
//                     <ClickableMetric 
//                       value={database.selectStarOnLargeTables}
//                       onClick={() => onDrillDown('database', database.databaseName, 'select_star_large', database.selectStarOnLargeTables)}
//                       severity="medium"
//                       suffix=" SELECT * on large"
//                     />
//                   </div>
//                   <div>
//                     <ClickableMetric 
//                       value={database.crossDatabaseQueries}
//                       onClick={() => onDrillDown('database', database.databaseName, 'cross_database', database.crossDatabaseQueries)}
//                       severity="normal"
//                       suffix=" cross-DB"
//                     />
//                   </div>
//                 </td>
//                 <td className="p-3 space-y-1">
//                   <div>
//                     <ClickableMetric 
//                       value={database.unusedTablesCount}
//                       onClick={() => onDrillDown('database', database.databaseName, 'unused_tables', database.unusedTablesCount)}
//                       severity="medium"
//                       suffix=" unused tables"
//                     />
//                   </div>
//                   <div>
//                     <ClickableMetric 
//                       value={database.zombieTablesStorageGb}
//                       onClick={() => onDrillDown('database', database.databaseName, 'zombie_storage', database.zombieTablesStorageGb)}
//                       severity="high"
//                       suffix=" GB zombie"
//                     />
//                   </div>
//                   <div>
//                     <ClickableMetric 
//                       value={database.unclusteredLargeTables}
//                       onClick={() => onDrillDown('database', database.databaseName, 'unclustered', database.unclusteredLargeTables)}
//                       severity="medium"
//                       suffix=" unclustered"
//                     />
//                   </div>
//                 </td>
//               </tr>
//             )