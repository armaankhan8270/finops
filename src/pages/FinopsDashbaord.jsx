import React, { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, LineChart, Line } from 'recharts';
import { RefreshCw, Database, Users, Search, Activity, HardDrive, AlertTriangle, TrendingUp, DollarSign } from 'lucide-react';

const COLORS = ['#3498db', '#e74c3c', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c'];
const API_BASE_URL = 'http://localhost:5000/api';

const FinOpsDashboard2 = () => {
  const [activeTable, setActiveTable] = useState('warehouses');
  const [tableData, setTableData] = useState({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [lastRefresh, setLastRefresh] = useState({});
  const [drillDownPath, setDrillDownPath] = useState([]);

  // Available tables configuration
  const tables = {
    warehouses: { 
      name: 'Warehouses', 
      icon: HardDrive, 
      color: '#3498db',
      drillTargets: ['users', 'queries']
    },
    users: { 
      name: 'Users', 
      icon: Users, 
      color: '#2ecc71',
      drillTargets: ['queries', 'query_details']
    },
    queries: { 
      name: 'Queries', 
      icon: Search, 
      color: '#e74c3c',
      drillTargets: ['query_details']
    },
    query_details: { 
      name: 'Query Details', 
      icon: Activity, 
      color: '#f39c12',
      drillTargets: []
    },
    databases: { 
      name: 'Databases', 
      icon: Database, 
      color: '#9b59b6',
      drillTargets: ['tables', 'users']
    },
    tables: { 
      name: 'Tables', 
      icon: HardDrive, 
      color: '#1abc9c',
      drillTargets: ['queries']
    }
  };

  // Fetch data from Flask API
  const fetchTableData = async (tableName, refresh = false) => {
    setLoading(true);
    setError('');
    
    try {
      const endpoint = refresh ? `${API_BASE_URL}/tables/${tableName}/refresh` : `${API_BASE_URL}/tables/${tableName}`;
      const response = await fetch(endpoint);
      const data = await response.json();
      
      if (data.error) {
        setError(data.error);
      } else {
        setTableData(prev => ({ ...prev, [tableName]: data }));
        setLastRefresh(prev => ({ ...prev, [tableName]: new Date().toISOString() }));
      }
    } catch (err) {
      setError(`Failed to fetch ${tableName}: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };

  // Initialize with warehouses data
  useEffect(() => {
    fetchTableData('warehouses');
  }, []);

  // Handle table switch
  const handleTableSwitch = (tableName) => {
    setActiveTable(tableName);
    if (!tableData[tableName]) {
      fetchTableData(tableName);
    }
  };

  // Handle drill down
  const handleDrillDown = (sourceTable, targetTable, filters = {}) => {
    setDrillDownPath([...drillDownPath, { table: sourceTable, filters }]);
    handleTableSwitch(targetTable);
  };

  // Handle drill back
  const handleDrillBack = () => {
    if (drillDownPath.length > 0) {
      const previous = drillDownPath[drillDownPath.length - 1];
      setDrillDownPath(drillDownPath.slice(0, -1));
      setActiveTable(previous.table);
    }
  };

  // Format number with commas
  const formatNumber = (num) => {
    if (num == null) return '0';
    return num.toLocaleString();
  };

  // Format currency
  const formatCurrency = (num) => {
    if (num == null) return '$0.00';
    return `$${parseFloat(num).toFixed(2)}`;
  };

  // Get table summary stats
  const getTableStats = (tableName) => {
    const data = tableData[tableName];
    if (!data || !data.data || data.data.length === 0) return {};

    const records = data.data;
    
    switch (tableName) {
      case 'warehouses':
        return {
          totalWarehouses: records.length,
          totalCredits: records.reduce((sum, r) => sum + (parseFloat(r.TOTAL_CREDITS_USED) || 0), 0),
          totalQueries: records.reduce((sum, r) => sum + (parseInt(r.TOTAL_QUERIES) || 0), 0),
          avgUtilization: records.reduce((sum, r) => sum + (parseFloat(r.AVG_UTILIZATION_PERCENT) || 0), 0) / records.length
        };
      case 'users':
        return {
          totalUsers: records.length,
          totalCredits: records.reduce((sum, r) => sum + (parseFloat(r.TOTAL_CREDITS_CONSUMED) || 0), 0),
          totalQueries: records.reduce((sum, r) => sum + (parseInt(r.TOTAL_QUERIES) || 0), 0),
          activeUsers: records.filter(r => parseInt(r.TOTAL_QUERIES) > 0).length
        };
      case 'queries':
        return {
          totalQueries: records.length,
          failedQueries: records.filter(r => r.EXECUTION_STATUS === 'FAIL').length,
          avgExecutionTime: records.reduce((sum, r) => sum + (parseInt(r.TOTAL_ELAPSED_TIME) || 0), 0) / records.length,
          totalDataScanned: records.reduce((sum, r) => sum + (parseFloat(r.BYTES_SCANNED_GB) || 0), 0)
        };
      default:
        return { totalRecords: records.length };
    }
  };

  // Render performance distribution chart
  const renderPerformanceChart = (data) => {
    if (!data || data.length === 0) return null;

    const performanceBuckets = [
      { name: '0-1s', key: 'QUERIES_0_TO_1_SEC' },
      { name: '1-10s', key: 'QUERIES_1_TO_10_SEC' },
      { name: '10-30s', key: 'QUERIES_10_TO_30_SEC' },
      { name: '30-60s', key: 'QUERIES_30_TO_60_SEC' },
      { name: '1-5m', key: 'QUERIES_1_TO_5_MIN' },
      { name: '5-15m', key: 'QUERIES_5_TO_15_MIN' },
      { name: '15m+', key: 'QUERIES_15_MIN_PLUS' }
    ];

    const chartData = performanceBuckets.map(bucket => ({
      name: bucket.name,
      value: data.reduce((sum, record) => sum + (parseInt(record[bucket.key]) || 0), 0)
    })).filter(item => item.value > 0);

    return (
      <div className="bg-white p-6 rounded-lg shadow-md">
        <h3 className="text-lg font-semibold mb-4">Query Performance Distribution</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="value" fill="#3498db" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  };

  // Render bad practices chart
  const renderBadPracticesChart = (data) => {
    if (!data || data.length === 0) return null;

    const badPractices = [
      { name: 'SELECT *', key: 'SELECT_STAR_QUERIES' },
      { name: 'Spilled', key: 'SPILLED_QUERIES' },
      { name: 'Failed', key: 'FAILED_QUERIES' },
      { name: 'Zero Results', key: 'ZERO_RESULT_QUERIES' },
      { name: 'High Compile', key: 'HIGH_COMPILE_TIME' },
      { name: 'Blocked Tx', key: 'BLOCKED_TRANSACTION_QUERIES' }
    ];

    const chartData = badPractices.map(practice => ({
      name: practice.name,
      value: data.reduce((sum, record) => sum + (parseInt(record[practice.key]) || 0), 0)
    })).filter(item => item.value > 0);

    return (
      <div className="bg-white p-6 rounded-lg shadow-md">
        <h3 className="text-lg font-semibold mb-4 flex items-center">
          <AlertTriangle className="mr-2 text-red-500" size={20} />
          Bad Practices Detection
        </h3>
        <ResponsiveContainer width="100%" height={300}>
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              outerRadius={100}
              fill="#8884d8"
              dataKey="value"
              label={({ name, value }) => `${name}: ${value}`}
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip />
          </PieChart>
        </ResponsiveContainer>
      </div>
    );
  };

  // Render data table
  const renderDataTable = (tableName) => {
    const data = tableData[tableName];
    if (!data || !data.data) return null;

    const records = data.data.slice(0, 100); // Limit to first 100 records
    if (records.length === 0) return <div>No data available</div>;

    const columns = data.columns || Object.keys(records[0]);
    const displayColumns = columns.slice(0, 8); // Limit columns for display

    return (
      <div className="bg-white rounded-lg shadow-md overflow-hidden">
        <div className="px-6 py-4 border-b">
          <h3 className="text-lg font-semibold">Data Table ({records.length} records)</h3>
          <p className="text-sm text-gray-600">{data.description}</p>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                {displayColumns.map(column => (
                  <th key={column} className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    {column.replace(/_/g, ' ')}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {records.map((record, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  {displayColumns.map(column => (
                    <td key={column} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {record[column] != null ? (
                        typeof record[column] === 'number' && column.includes('CREDIT') ? 
                          formatCurrency(record[column]) :
                        typeof record[column] === 'number' ? 
                          formatNumber(record[column]) :
                          record[column].toString()
                      ) : '-'}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  const currentData = tableData[activeTable];
  const stats = getTableStats(activeTable);

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center">
              <DollarSign className="h-8 w-8 text-blue-600 mr-3" />
              <div>
                <h1 className="text-2xl font-bold text-gray-900">FinOps Analytics Dashboard</h1>
                <p className="text-sm text-gray-500">Snowflake Cost Optimization & Performance Analytics</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              {drillDownPath.length > 0 && (
                <button
                  onClick={handleDrillBack}
                  className="px-4 py-2 bg-gray-500 text-white rounded-lg hover:bg-gray-600 transition-colors"
                >
                  ← Back
                </button>
              )}
              <button
                onClick={() => fetchTableData(activeTable, true)}
                disabled={loading}
                className="flex items-center px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                <RefreshCw className={`mr-2 h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                Refresh
              </button>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Navigation Tabs */}
        <div className="flex flex-wrap gap-2 mb-8">
          {Object.entries(tables).map(([key, config]) => {
            const Icon = config.icon;
            return (
              <button
                key={key}
                onClick={() => handleTableSwitch(key)}
                className={`flex items-center px-4 py-2 rounded-lg transition-colors ${
                  activeTable === key
                    ? 'bg-blue-600 text-white'
                    : 'bg-white text-gray-700 hover:bg-gray-50'
                }`}
              >
                <Icon className="mr-2 h-4 w-4" />
                {config.name}
              </button>
            );
          })}
        </div>

        {/* Error Message */}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-8">
            <div className="flex">
              <AlertTriangle className="h-5 w-5 text-red-400 mr-2" />
              <p className="text-red-800">{error}</p>
            </div>
          </div>
        )}

        {/* Loading State */}
        {loading && (
          <div className="text-center py-12">
            <RefreshCw className="h-8 w-8 animate-spin mx-auto text-blue-600" />
            <p className="mt-2 text-gray-600">Loading data...</p>
          </div>
        )}

        {/* Dashboard Content */}
        {currentData && !loading && (
          <>
            {/* Summary Stats */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
              {Object.entries(stats).map(([key, value]) => (
                <div key={key} className="bg-white p-6 rounded-lg shadow-md">
                  <div className="flex items-center">
                    <TrendingUp className="h-8 w-8 text-blue-600 mr-3" />
                    <div>
                      <p className="text-2xl font-bold text-gray-900">
                        {key.includes('Credits') || key.includes('credits') ? 
                          formatCurrency(value) : 
                          formatNumber(value)
                        }
                      </p>
                      <p className="text-sm text-gray-600 capitalize">
                        {key.replace(/([A-Z])/g, ' $1').trim()}
                      </p>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
              {renderPerformanceChart(currentData.data)}
              {renderBadPracticesChart(currentData.data)}
            </div>

            {/* Drill Down Actions */}
            {tables[activeTable].drillTargets.length > 0 && (
              <div className="bg-white p-6 rounded-lg shadow-md mb-8">
                <h3 className="text-lg font-semibold mb-4">Drill Down Options</h3>
                <div className="flex flex-wrap gap-3">
                  {tables[activeTable].drillTargets.map(target => {
                    const targetConfig = tables[target];
                    const Icon = targetConfig.icon;
                    return (
                      <button
                        key={target}
                        onClick={() => handleDrillDown(activeTable, target)}
                        className="flex items-center px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
                      >
                        <Icon className="mr-2 h-4 w-4" />
                        Drill to {targetConfig.name}
                      </button>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Data Table */}
            {renderDataTable(activeTable)}

            {/* Additional Insights for Warehouses */}
            {activeTable === 'warehouses' && currentData.data && (
              <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-8">
                {/* Top Credit Consumers */}
                <div className="bg-white p-6 rounded-lg shadow-md">
                  <h3 className="text-lg font-semibold mb-4">Top Credit Consumers</h3>
                  <div className="space-y-3">
                    {currentData.data
                      .sort((a, b) => (parseFloat(b.TOTAL_CREDITS_USED) || 0) - (parseFloat(a.TOTAL_CREDITS_USED) || 0))
                      .slice(0, 5)
                      .map((warehouse, index) => (
                        <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                          <span className="font-medium">{warehouse.WAREHOUSE_NAME}</span>
                          <span className="text-blue-600 font-semibold">
                            {formatCurrency(warehouse.TOTAL_CREDITS_USED)}
                          </span>
                        </div>
                    ))}
                  </div>
                </div>

                {/* Utilization Overview */}
                <div className="bg-white p-6 rounded-lg shadow-md">
                  <h3 className="text-lg font-semibold mb-4">Utilization Overview</h3>
                  <div className="space-y-3">
                    {currentData.data
                      .filter(w => w.AVG_UTILIZATION_PERCENT > 0)
                      .sort((a, b) => (parseFloat(b.AVG_UTILIZATION_PERCENT) || 0) - (parseFloat(a.AVG_UTILIZATION_PERCENT) || 0))
                      .slice(0, 5)
                      .map((warehouse, index) => (
                        <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                          <span className="font-medium">{warehouse.WAREHOUSE_NAME}</span>
                          <div className="flex items-center">
                            <div className="w-20 bg-gray-200 rounded-full h-2 mr-3">
                              <div 
                                className="bg-blue-600 h-2 rounded-full" 
                                style={{width: `${Math.min(parseFloat(warehouse.AVG_UTILIZATION_PERCENT) || 0, 100)}%`}}
                              ></div>
                            </div>
                            <span className="text-sm font-semibold">
                              {parseFloat(warehouse.AVG_UTILIZATION_PERCENT || 0).toFixed(1)}%
                            </span>
                          </div>
                        </div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Additional Insights for Users */}
            {activeTable === 'users' && currentData.data && (
              <div className="mt-8 grid grid-cols-1 lg:grid-cols-2 gap-8">
                {/* Most Active Users */}
                <div className="bg-white p-6 rounded-lg shadow-md">
                  <h3 className="text-lg font-semibold mb-4">Most Active Users</h3>
                  <div className="space-y-3">
                    {currentData.data
                      .sort((a, b) => (parseInt(b.TOTAL_QUERIES) || 0) - (parseInt(a.TOTAL_QUERIES) || 0))
                      .slice(0, 5)
                      .map((user, index) => (
                        <div key={index} className="flex justify-between items-center p-3 bg-gray-50 rounded">
                          <div>
                            <span className="font-medium">{user.USER_NAME}</span>
                            <p className="text-xs text-gray-500">{user.EMAIL}</p>
                          </div>
                          <span className="text-blue-600 font-semibold">
                            {formatNumber(user.TOTAL_QUERIES)} queries
                          </span>
                        </div>
                    ))}
                  </div>
                </div>

                {/* Users with Most Bad Practices */}
                <div className="bg-white p-6 rounded-lg shadow-md">
                  <h3 className="text-lg font-semibold mb-4 flex items-center">
                    <AlertTriangle className="mr-2 text-red-500" size={20} />
                    Users with Bad Practices
                  </h3>
                  <div className="space-y-3">
                    {currentData.data
                      .map(user => ({
                        ...user,
                        badPracticeCount: (parseInt(user.SELECT_STAR_QUERIES) || 0) + 
                                        (parseInt(user.SPILLED_QUERIES) || 0) + 
                                        (parseInt(user.FAILED_QUERIES) || 0) + 
                                        (parseInt(user.UNPARTITIONED_SCANS) || 0)
                      }))
                      .sort((a, b) => b.badPracticeCount - a.badPracticeCount)
                      .slice(0, 5)
                      .filter(user => user.badPracticeCount > 0)
                      .map((user, index) => (
                        <div key={index} className="flex justify-between items-center p-3 bg-red-50 rounded">
                          <span className="font-medium">{user.USER_NAME}</span>
                          <span className="text-red-600 font-semibold">
                            {user.badPracticeCount} issues
                          </span>
                        </div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Metadata */}
            <div className="mt-8 bg-gray-50 p-4 rounded-lg">
              <div className="flex justify-between items-center text-sm text-gray-600">
                <span>
                  Last Updated: {lastRefresh[activeTable] ? 
                    new Date(lastRefresh[activeTable]).toLocaleString() : 
                    'Never'
                  }
                </span>
                <span>
                  Total Records: {formatNumber(currentData.row_count)}
                </span>
                <span>
                  Data Range: Last 3 Days
                </span>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
};

export default FinOpsDashboard2;