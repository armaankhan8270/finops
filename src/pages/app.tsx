import React, { createContext, useContext, useState, useEffect } from 'react';
import { 
  ChevronRight, 
  Database, 
  Server, 
  Users, 
  Activity, 
  AlertTriangle,
  TrendingUp,
  Clock,
  DollarSign,
  Search,
  Filter,
  Download,
  Info,
  X,
  Home
} from 'lucide-react';

// Types
interface DrillDownContext {
  entityType: 'warehouse' | 'database' | 'table' | 'user' | 'serverless' | null;
  entityName: string | null;
  metricCategory: string | null;
  filterContext: any;
  breadcrumb: BreadcrumbItem[];
}

interface BreadcrumbItem {
  label: string;
  type: string;
  value?: any;
}

interface WarehouseMetrics {
  warehouseName: string;
  totalQueries: number;
  totalCredits: number;
  activeHoursPerDay: number;
  idleCreditBurnRate: number;
  queries0To1Sec: number;
  queries1To10Sec: number;
  queries10To30Sec: number;
  queries30To60Sec: number;
  queries1To5Min: number;
  queries5MinPlus: number;
  overProvisionedQueries: number;
  underProvisionedQueries: number;
  selectStarQueries: number;
  unpartitionedScanQueries: number;
  cartesianJoinQueries: number;
  zeroResultQueries: number;
  failedCancelledQueries: number;
  repeatedQueries: number;
  highCompileTimeQueries: number;
  spilledToLocalQueries: number;
  spilledToRemoteQueries: number;
  weekendIdleCredits: number;
  peakHourCostPct: number;
  singleUserMonopolization: number;
  queueWaitTimeAvgMs: number;
}

interface DatabaseMetrics {
  databaseName: string;
  totalStorageGb: number;
  tableCount: number;
  storageGrowthGbPerWeek: number;
  timeTravelStorageGb: number;
  crossDatabaseQueries: number;
  selectStarOnLargeTables: number;
  unpartitionedScansCount: number;
  fullTableScanQueries: number;
  expensiveAggregations: number;
  unusedTablesCount: number;
  zombieTablesStorageGb: number;
  unclusteredLargeTables: number;
  tablesWithoutPrimaryKey: number;
}

interface TableMetrics {
  tableName: string;
  databaseName: string;
  storageSizeGb: number;
  rowCount: number;
  totalScansCount: number;
  fullTableScansCount: number;
  selectStarQueriesCount: number;
  unfilteredQueriesCount: number;
  queriesPerDay: number;
  lastAccessedDaysAgo: number;
  partitionPruningEfficiencyPct: number;
  cartesianJoinsInvolving: number;
  expensiveAggregationsCount: number;
  clusteringRatio: number;
}

interface ServerlessMetrics {
  serviceName: string;
  serviceType: 'SNOWPIPE' | 'TASKS' | 'STREAMS' | 'FUNCTIONS';
  totalCreditsConsumed: number;
  executionsCount: number;
  successRatePct: number;
  filesProcessedCount?: number;
  errorRatePct?: number;
  duplicateFilesProcessed?: number;
  taskRunsCount?: number;
  failedTaskCreditsWasted?: number;
  suspendedTasksCount?: number;
  streamLagMinutesAvg?: number;
  streamOffsetResets?: number;
}

interface UserMetrics {
  userName: string;
  warehouseName: string;
  queryCountInCategory: number;
  creditsConsumed: number;
  percentageOfWarehouseUsage: number;
  specificQueriesCount: number;
}

interface QueryHistory {
  queryId: string;
  userName: string;
  warehouseName: string;
  queryTextPreview: string;
  startTime: string;
  executionTimeMs: number;
  bytesScanned: number;
  creditsUsed: number;
  errorCode?: string;
}

// Sample Data
const sampleWarehouseData: WarehouseMetrics[] = [
  {
    warehouseName: "COMPUTE_WH_LARGE",
    totalQueries: 1250,
    totalCredits: 450.75,
    activeHoursPerDay: 18.5,
    idleCreditBurnRate: 2.3,
    queries0To1Sec: 200,
    queries1To10Sec: 800,
    queries10To30Sec: 300,
    queries30To60Sec: 150,
    queries1To5Min: 80,
    queries5MinPlus: 20,
    overProvisionedQueries: 45,
    underProvisionedQueries: 23,
    selectStarQueries: 78,
    unpartitionedScanQueries: 45,
    cartesianJoinQueries: 12,
    zeroResultQueries: 156,
    failedCancelledQueries: 34,
    repeatedQueries: 89,
    highCompileTimeQueries: 67,
    spilledToLocalQueries: 23,
    spilledToRemoteQueries: 8,
    weekendIdleCredits: 45.2,
    peakHourCostPct: 65,
    singleUserMonopolization: 35,
    queueWaitTimeAvgMs: 1250
  },
  {
    warehouseName: "COMPUTE_WH_MEDIUM",
    totalQueries: 890,
    totalCredits: 234.50,
    activeHoursPerDay: 12.3,
    idleCreditBurnRate: 1.8,
    queries0To1Sec: 150,
    queries1To10Sec: 600,
    queries10To30Sec: 200,
    queries30To60Sec: 90,
    queries1To5Min: 40,
    queries5MinPlus: 10,
    overProvisionedQueries: 32,
    underProvisionedQueries: 18,
    selectStarQueries: 56,
    unpartitionedScanQueries: 34,
    cartesianJoinQueries: 8,
    zeroResultQueries: 112,
    failedCancelledQueries: 23,
    repeatedQueries: 67,
    highCompileTimeQueries: 45,
    spilledToLocalQueries: 15,
    spilledToRemoteQueries: 5,
    weekendIdleCredits: 28.7,
    peakHourCostPct: 58,
    singleUserMonopolization: 28,
    queueWaitTimeAvgMs: 890
  }
];

const sampleDatabaseData: DatabaseMetrics[] = [
  {
    databaseName: "SALES_DB",
    totalStorageGb: 2450.5,
    tableCount: 156,
    storageGrowthGbPerWeek: 45.2,
    timeTravelStorageGb: 890.3,
    crossDatabaseQueries: 234,
    selectStarOnLargeTables: 89,
    unpartitionedScansCount: 156,
    fullTableScanQueries: 120,
    expensiveAggregations: 67,
    unusedTablesCount: 23,
    zombieTablesStorageGb: 345.7,
    unclusteredLargeTables: 34,
    tablesWithoutPrimaryKey: 12
  }
];

const sampleUserData: UserMetrics[] = [
  {
    userName: "john.doe",
    warehouseName: "COMPUTE_WH_LARGE",
    queryCountInCategory: 25,
    creditsConsumed: 450.75,
    percentageOfWarehouseUsage: 35.2,
    specificQueriesCount: 25
  },
  {
    userName: "jane.smith",
    warehouseName: "COMPUTE_WH_LARGE",
    queryCountInCategory: 20,
    creditsConsumed: 380.50,
    percentageOfWarehouseUsage: 28.7,
    specificQueriesCount: 20
  }
];

const sampleQueryHistory: QueryHistory[] = [
  {
    queryId: "01b2c3d4-e5f6-7890-abcd-ef1234567890",
    userName: "john.doe",
    warehouseName: "COMPUTE_WH_LARGE",
    queryTextPreview: "SELECT * FROM sales_data WHERE date >= '2024-01-01' AND region IN ('US', 'CA') ORDER BY...",
    startTime: "2024-01-15 14:30:25",
    executionTimeMs: 45230,
    bytesScanned: 2450000000,
    creditsUsed: 12.45,
    errorCode: undefined
  }
];

// Context
const DrillDownContext = createContext<{
  context: DrillDownContext;
  setContext: (context: Partial<DrillDownContext>) => void;
  resetContext: () => void;
}>({
  context: {
    entityType: null,
    entityName: null,
    metricCategory: null,
    filterContext: null,
    breadcrumb: []
  },
  setContext: () => {},
  resetContext: () => {}
});

// Context Provider
const DrillDownProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [context, setContextState] = useState<DrillDownContext>({
    entityType: null,
    entityName: null,
    metricCategory: null,
    filterContext: null,
    breadcrumb: []
  });

  const setContext = (newContext: Partial<DrillDownContext>) => {
    setContextState(prev => ({ ...prev, ...newContext }));
  };

  const resetContext = () => {
    setContextState({
      entityType: null,
      entityName: null,
      metricCategory: null,
      filterContext: null,
      breadcrumb: []
    });
  };

  return (
    <DrillDownContext.Provider value={{ context, setContext, resetContext }}>
      {children}
    </DrillDownContext.Provider>
  );
};

// Utility Components
const MetricButton: React.FC<{
  value: number;
  label: string;
  onClick: () => void;
  variant?: 'default' | 'warning' | 'danger';
  icon?: React.ReactNode;
}> = ({ value, label, onClick, variant = 'default', icon }) => {
  const getVariantClasses = () => {
    switch (variant) {
      case 'warning':
        return 'text-orange-400 hover:text-orange-300 hover:bg-orange-900/20';
      case 'danger':
        return 'text-red-400 hover:text-red-300 hover:bg-red-900/20';
      default:
        return 'text-blue-400 hover:text-blue-300 hover:bg-blue-900/20';
    }
  };

  return (
    <button
      onClick={onClick}
      className={`group flex items-center space-x-2 px-3 py-2 rounded-lg transition-all duration-200 ${getVariantClasses()}`}
      title={`Click to drill down into ${label}`}
    >
      {icon && <span className="text-sm">{icon}</span>}
      <span className="font-mono font-bold text-lg">{value.toLocaleString()}</span>
      <ChevronRight className="w-4 h-4 opacity-0 group-hover:opacity-100 transition-opacity" />
    </button>
  );
};

const BreadcrumbTrail: React.FC = () => {
  const { context, resetContext } = useContext(DrillDownContext);

  if (context.breadcrumb.length === 0) return null;

  return (
    <div className="flex items-center space-x-2 mb-6 p-4 bg-gray-800 rounded-lg">
      <button
        onClick={resetContext}
        className="flex items-center space-x-1 text-gray-400 hover:text-white transition-colors"
      >
        <Home className="w-4 h-4" />
        <span>Dashboard</span>
      </button>
      {context.breadcrumb.map((item, index) => (
        <React.Fragment key={index}>
          <ChevronRight className="w-4 h-4 text-gray-500" />
          <span className="text-gray-300">{item.label}</span>
        </React.Fragment>
      ))}
    </div>
  );
};

// Main Tables
const WarehouseMetricsTable: React.FC = () => {
  const { setContext } = useContext(DrillDownContext);
  const [searchTerm, setSearchTerm] = useState('');

  const handleMetricClick = (warehouseName: string, metricCategory: string, value: number) => {
    setContext({
      entityType: 'warehouse',
      entityName: warehouseName,
      metricCategory,
      filterContext: { value },
      breadcrumb: [
        { label: 'Warehouses', type: 'entity' },
        { label: warehouseName, type: 'name' },
        { label: metricCategory, type: 'metric' }
      ]
    });
  };

  const filteredData = sampleWarehouseData.filter(warehouse =>
    warehouse.warehouseName.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-white flex items-center space-x-2">
          <Server className="w-6 h-6" />
          <span>Warehouse Metrics</span>
        </h2>
        <div className="flex items-center space-x-4">
          <div className="relative">
            <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              placeholder="Search warehouses..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-10 pr-4 py-2 bg-gray-800 border border-gray-700 rounded-lg text-white focus:outline-none focus:border-blue-500"
            />
          </div>
          <button className="flex items-center space-x-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg text-white transition-colors">
            <Download className="w-4 h-4" />
            <span>Export</span>
          </button>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full bg-gray-800 rounded-lg overflow-hidden">
          <thead className="bg-gray-700">
            <tr>
              <th className="px-6 py-4 text-left text-white font-semibold">Warehouse</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Core Metrics</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Performance Buckets</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Bad Practices</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Cost Efficiency</th>
            </tr>
          </thead>
          <tbody>
            {filteredData.map((warehouse) => (
              <tr key={warehouse.warehouseName} className="border-t border-gray-700 hover:bg-gray-750">
                <td className="px-6 py-4">
                  <div className="font-mono font-bold text-white">{warehouse.warehouseName}</div>
                </td>
                <td className="px-6 py-4 space-y-2">
                  <MetricButton
                    value={warehouse.totalQueries}
                    label="Total Queries"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'total_queries', warehouse.totalQueries)}
                    icon={<Activity className="w-4 h-4" />}
                  />
                  <MetricButton
                    value={warehouse.totalCredits}
                    label="Total Credits"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'total_credits', warehouse.totalCredits)}
                    icon={<DollarSign className="w-4 h-4" />}
                  />
                  <MetricButton
                    value={warehouse.activeHoursPerDay}
                    label="Active Hours/Day"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'active_hours', warehouse.activeHoursPerDay)}
                    icon={<Clock className="w-4 h-4" />}
                  />
                </td>
                <td className="px-6 py-4 space-y-1">
                  <MetricButton
                    value={warehouse.queries0To1Sec}
                    label="0-1 sec queries"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'queries_0_to_1_sec', warehouse.queries0To1Sec)}
                  />
                  <MetricButton
                    value={warehouse.queries1To10Sec}
                    label="1-10 sec queries"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'queries_1_to_10_sec', warehouse.queries1To10Sec)}
                  />
                  <MetricButton
                    value={warehouse.queries10To30Sec}
                    label="10-30 sec queries"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'queries_10_to_30_sec', warehouse.queries10To30Sec)}
                    variant="warning"
                  />
                  <MetricButton
                    value={warehouse.queries5MinPlus}
                    label="5+ min queries"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'queries_5_min_plus', warehouse.queries5MinPlus)}
                    variant="danger"
                  />
                </td>
                <td className="px-6 py-4 space-y-1">
                  <MetricButton
                    value={warehouse.unpartitionedScanQueries}
                    label="Unpartitioned Scans"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'unpartitioned_scan_queries', warehouse.unpartitionedScanQueries)}
                    variant="danger"
                    icon={<AlertTriangle className="w-4 h-4" />}
                  />
                  <MetricButton
                    value={warehouse.selectStarQueries}
                    label="SELECT * Queries"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'select_star_queries', warehouse.selectStarQueries)}
                    variant="warning"
                  />
                  <MetricButton
                    value={warehouse.spilledToRemoteQueries}
                    label="Remote Spill Queries"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'spilled_to_remote_queries', warehouse.spilledToRemoteQueries)}
                    variant="danger"
                  />
                </td>
                <td className="px-6 py-4 space-y-1">
                  <MetricButton
                    value={warehouse.weekendIdleCredits}
                    label="Weekend Idle Credits"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'weekend_idle_credits', warehouse.weekendIdleCredits)}
                    variant="warning"
                  />
                  <MetricButton
                    value={warehouse.singleUserMonopolization}
                    label="User Monopolization %"
                    onClick={() => handleMetricClick(warehouse.warehouseName, 'single_user_monopolization', warehouse.singleUserMonopolization)}
                    variant="warning"
                  />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const UserBreakdownTable: React.FC = () => {
  const { context, setContext } = useContext(DrillDownContext);

  const handleUserClick = (userName: string) => {
    setContext({
      breadcrumb: [
        ...context.breadcrumb,
        { label: `User: ${userName}`, type: 'user' }
      ]
    });
  };

  return (
    <div className="space-y-4">
      <h3 className="text-xl font-bold text-white">
        Users - {context.metricCategory} in {context.entityName}
      </h3>
      <div className="overflow-x-auto">
        <table className="w-full bg-gray-800 rounded-lg overflow-hidden">
          <thead className="bg-gray-700">
            <tr>
              <th className="px-6 py-4 text-left text-white font-semibold">User</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Query Count</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Credits Consumed</th>
              <th className="px-6 py-4 text-left text-white font-semibold">% of Warehouse Usage</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {sampleUserData.map((user) => (
              <tr key={user.userName} className="border-t border-gray-700 hover:bg-gray-750">
                <td className="px-6 py-4 font-mono text-white">{user.userName}</td>
                <td className="px-6 py-4">
                  <MetricButton
                    value={user.queryCountInCategory}
                    label="Query Count"
                    onClick={() => handleUserClick(user.userName)}
                  />
                </td>
                <td className="px-6 py-4 text-green-400 font-mono">${user.creditsConsumed.toFixed(2)}</td>
                <td className="px-6 py-4 text-yellow-400 font-mono">{user.percentageOfWarehouseUsage}%</td>
                <td className="px-6 py-4">
                  <button
                    onClick={() => handleUserClick(user.userName)}
                    className="px-3 py-1 bg-blue-600 hover:bg-blue-700 rounded text-white text-sm transition-colors"
                  >
                    View Queries
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const QueryHistoryTable: React.FC = () => {
  const [selectedQuery, setSelectedQuery] = useState<QueryHistory | null>(null);

  return (
    <div className="space-y-4">
      <h3 className="text-xl font-bold text-white">Query History</h3>
      <div className="overflow-x-auto">
        <table className="w-full bg-gray-800 rounded-lg overflow-hidden">
          <thead className="bg-gray-700">
            <tr>
              <th className="px-6 py-4 text-left text-white font-semibold">Query ID</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Query Preview</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Start Time</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Execution Time</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Credits Used</th>
              <th className="px-6 py-4 text-left text-white font-semibold">Actions</th>
            </tr>
          </thead>
          <tbody>
            {sampleQueryHistory.map((query) => (
              <tr key={query.queryId} className="border-t border-gray-700 hover:bg-gray-750">
                <td className="px-6 py-4 font-mono text-sm text-gray-300">{query.queryId.slice(0, 8)}...</td>
                <td className="px-6 py-4 max-w-md">
                  <div className="text-gray-300 text-sm truncate">{query.queryTextPreview}</div>
                </td>
                <td className="px-6 py-4 text-gray-300 text-sm">{query.startTime}</td>
                <td className="px-6 py-4 text-yellow-400 font-mono">{query.executionTimeMs}ms</td>
                <td className="px-6 py-4 text-green-400 font-mono">${query.creditsUsed}</td>
                <td className="px-6 py-4">
                  <button
                    onClick={() => setSelectedQuery(query)}
                    className="px-3 py-1 bg-blue-600 hover:bg-blue-700 rounded text-white text-sm transition-colors"
                  >
                    View Details
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Query Details Modal */}
      {selectedQuery && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-gray-800 rounded-lg p-6 max-w-4xl max-h-[80vh] overflow-y-auto">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-xl font-bold text-white">Query Details</h3>
              <button
                onClick={() => setSelectedQuery(null)}
                className="text-gray-400 hover:text-white"
              >
                <X className="w-6 h-6" />
              </button>
            </div>
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">Query ID</label>
                <div className="font-mono text-sm text-gray-400">{selectedQuery.queryId}</div>
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-300 mb-2">Full Query Text</label>
                <pre className="bg-gray-900 p-4 rounded text-sm text-gray-300 overflow-x-auto">
                  {selectedQuery.queryTextPreview}
                </pre>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-300 mb-2">Execution Time</label>
                  <div className="text-yellow-400 font-mono">{selectedQuery.executionTimeMs}ms</div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-300 mb-2">Credits Used</label>
                  <div className="text-green-400 font-mono">${selectedQuery.creditsUsed}</div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-300 mb-2">Bytes Scanned</label>
                  <div className="text-blue-400 font-mono">{(selectedQuery.bytesScanned / 1024 / 1024 / 1024).toFixed(2)} GB</div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-300 mb-2">User</label>
                  <div className="text-white">{selectedQuery.userName}</div>
                </div>
              </div>
              <div className="bg-blue-900/20 p-4 rounded-lg">
                <h4 className="text-blue-400 font-semibold mb-2">Optimization Recommendations</h4>
                <ul className="text-sm text-gray-300 space-y-1">
                  <li>• Consider adding WHERE clause to reduce data scanned</li>
                  <li>• Use specific column names instead of SELECT *</li>
                  <li>• Consider partitioning strategy for better performance</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Main Dashboard Component
const FinOpsDashboard: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'warehouses' | 'databases' | 'tables' | 'users' | 'serverless'>('warehouses');
  const { context } = useContext(DrillDownContext);

  const tabs = [
    { id: 'warehouses', label: 'Warehouses', icon: <Server className="w-5 h-5" /> },
    { id: 'databases', label: 'Databases', icon: <Database className="w-5 h-5" /> },
    { id: 'tables', label: 'Tables', icon: <Activity className="w-5 h-5" /> },
    { id: 'users', label: 'Users', icon: <Users className="w-5 h-5" /> },
    { id: 'serverless', label: 'Serverless', icon: <TrendingUp className="w-5 h-5" /> }
  ];

  const renderContent = () => {
    // If we're in a drill-down context, show the appropriate breakdown
    if (context.entityType === 'warehouse' && context.metricCategory) {
      if (context.breadcrumb.some(b => b.type === 'user')) {
        return <QueryHistoryTable />;
      }
      return <UserBreakdownTable />;
    }

    // Otherwise show the main table based on active tab
    switch (activeTab) {
      case 'warehouses':
        return <WarehouseMetricsTable />;
      case 'databases':
        return <div className="text-white">Database metrics table (to be implemented)</div>;
      case 'tables':
        return <div className="text-white">Table metrics table (to be implemented)</div>;
      case 'users':
        return <div className="text-white">User metrics table (to be implemented)</div>;
      case 'serverless':
        return <div className="text-white">Serverless metrics table (to be implemented)</div>;
      default:
        return <WarehouseMetricsTable />;
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white">
      <div className="container mx-auto px-6 py-8">
        <header className="mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">FinOps Analytics Dashboard</h1>
          <p className="text-gray-400">Comprehensive cost optimization and performance monitoring</p>
        </header>

        <BreadcrumbTrail />

        {/* Navigation Tabs */}
        {!context.entityType && (
          <nav className="mb-8">
            <div className="flex space-x-1 bg-gray-800 p-1 rounded-lg">
              {tabs.map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id as any)}
                  className={`flex items-center space-x-2 px-4 py-2 rounded-md transition-colors ${
                    activeTab === tab.id
                      ? 'bg-blue-600 text-white'
                      : 'text-gray-400 hover:text-white hover:bg-gray-700'
                  }`}
                >
                  {tab.icon}
                  <span>{tab.label}</span>
                </button>
              ))}
            </div>
          </nav>
        )}

        {/* Main Content */}
        <main>
          {renderContent()}
        </main>
      </div>
    </div>
  );
};

// App Component with Provider
const App2: React.FC = () => {
  return (
    <DrillDownProvider>
      <FinOpsDashboard />
    </DrillDownProvider>
  );
};

export default App2;