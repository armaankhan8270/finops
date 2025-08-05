import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Search, Filter, X, Eye, TrendingUp, TrendingDown, AlertCircle, CheckCircle, Clock, Database, Zap, Users } from 'lucide-react';

const FinopsUserTable = () => {
  // State Management
  const [userData, setUserData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedFilters, setSelectedFilters] = useState({
    costStatus: 'all',
    scoreRange: 'all',
    queryRange: 'all'
  });
  const [sortConfig, setSortConfig] = useState({ key: 'WEIGHTED_SCORE', direction: 'desc' });
  const [modalData, setModalData] = useState({
    isOpen: false,
    user: null,
    queryType: null,
    queries: []
  });

  // Configuration Objects
  const queryTypeMapping = {
    'SPILLED_QUERIES': 'spilled',
    'OVER_PROVISIONED_QUERIES': 'over_provisioned',
    'PEAK_HOUR_LONG_RUNNING_QUERIES': 'peak_hour_long_running',
    'SELECT_STAR_QUERIES': 'select_star',
    'UNPARTITIONED_SCAN_QUERIES': 'unpartitioned_scan',
    'REPEATED_QUERIES': 'repeated_query',
    'COMPLEX_JOIN_QUERIES': 'complex_query',
    'ZERO_RESULT_QUERIES': 'zero_result_query',
    'HIGH_COMPILE_QUERIES': 'high_compile_time',
    'UNTAGGED_QUERIES': 'untagged_query',
    'UNLIMITED_ORDER_BY_QUERIES': 'unlimited_order_by',
    'LARGE_GROUP_BY_QUERIES': 'large_group_by',
    'SLOW_QUERIES': 'slow_query',
    'EXPENSIVE_DISTINCT_QUERIES': 'expensive_distinct',
    'INEFFICIENT_LIKE_QUERIES': 'inefficient_like',
    'NO_RESULTS_WITH_SCAN_QUERIES': 'no_results_with_scan',
    'CARTESIAN_JOIN_QUERIES': 'cartesian_join',
    'HIGH_COMPILE_RATIO_QUERIES': 'high_compile_ratio',
  };

  const tableConfig = [
    { key: 'USER_NAME', label: 'User', icon: Users, sortable: true, type: 'text' },
    { key: 'TOTAL_QUERIES', label: 'Total Queries', icon: Database, sortable: true, type: 'number' },
    { key: 'TOTAL_CREDITS', label: 'Credits', icon: TrendingUp, sortable: true, type: 'number' },
    { key: 'WEIGHTED_SCORE', label: 'Score', icon: Zap, sortable: true, type: 'score' },
    { key: 'COST_STATUS', label: 'Status', icon: AlertCircle, sortable: true, type: 'status' },
    { key: 'SPILLED_QUERIES', label: 'Spilled', sortable: true, type: 'clickable' },
    { key: 'OVER_PROVISIONED_QUERIES', label: 'Over-Provisioned', sortable: true, type: 'clickable' },
    { key: 'PEAK_HOUR_LONG_RUNNING_QUERIES', label: 'Long Running', sortable: true, type: 'clickable' },
    { key: 'SELECT_STAR_QUERIES', label: 'Select *', sortable: true, type: 'clickable' },
    { key: 'UNPARTITIONED_SCAN_QUERIES', label: 'Unpartitioned', sortable: true, type: 'clickable' },
    { key: 'ZERO_RESULT_QUERIES', label: 'Zero Result', sortable: true, type: 'clickable' },
    { key: 'HIGH_COMPILE_QUERIES', label: 'High Compile', sortable: true, type: 'clickable' },
    { key: 'UNLIMITED_ORDER_BY_QUERIES', label: 'Unlimited Order', sortable: true, type: 'clickable' },
    { key: 'CARTESIAN_JOIN_QUERIES', label: 'Cartesian Join', sortable: true, type: 'clickable' },
  ];

  // Utility Functions
  const formatters = {
    number: (num) => num === null || num === undefined ? 'N/A' : new Intl.NumberFormat().format(num),
    milliseconds: (ms) => ms === null || ms === undefined ? 'N/A' : `${(ms / 1000).toFixed(1)}s`,
    bytes: (bytes) => bytes === null || bytes === undefined ? 'N/A' : `${(bytes / Math.pow(1024, 3)).toFixed(2)} GB`,
    queryType: (key) => key.replace(/_queries|_query/g, '').replace(/_/g, ' ').toLowerCase().replace(/\b\w/g, l => l.toUpperCase())
  };

  // Data Fetching
  useEffect(() => {
    const fetchUserData = async () => {
      try {
        const response = await fetch('http://localhost:5000/api/users');
        if (!response.ok) throw new Error('Network response was not ok');
        const data = await response.json();
        setUserData(data);
      } catch (error) {
        setError(error);
      } finally {
        setLoading(false);
      }
    };
    fetchUserData();
  }, []);

  // Filtering and Sorting Logic
  const filteredAndSortedData = useMemo(() => {
    let filtered = userData.filter(user => {
      const matchesSearch = user.USER_NAME.toLowerCase().includes(searchTerm.toLowerCase());
      const matchesCostStatus = selectedFilters.costStatus === 'all' || user.COST_STATUS === selectedFilters.costStatus;
      
      const matchesScoreRange = selectedFilters.scoreRange === 'all' || (() => {
        const score = user.WEIGHTED_SCORE;
        switch (selectedFilters.scoreRange) {
          case 'high': return score >= 80;
          case 'medium': return score >= 50 && score < 80;
          case 'low': return score < 50;
          default: return true;
        }
      })();

      const matchesQueryRange = selectedFilters.queryRange === 'all' || (() => {
        const queries = user.TOTAL_QUERIES;
        switch (selectedFilters.queryRange) {
          case 'high': return queries >= 1000;
          case 'medium': return queries >= 100 && queries < 1000;
          case 'low': return queries < 100;
          default: return true;
        }
      })();

      return matchesSearch && matchesCostStatus && matchesScoreRange && matchesQueryRange;
    });

    if (sortConfig.key) {
      filtered.sort((a, b) => {
        const aVal = a[sortConfig.key];
        const bVal = b[sortConfig.key];
        const multiplier = sortConfig.direction === 'asc' ? 1 : -1;
        
        if (typeof aVal === 'string') return aVal.localeCompare(bVal) * multiplier;
        return (aVal - bVal) * multiplier;
      });
    }

    return filtered;
  }, [userData, searchTerm, selectedFilters, sortConfig]);

  // Event Handlers
  const handleSort = useCallback((key) => {
    setSortConfig(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
    }));
  }, []);

  const handleFilterChange = useCallback((filterType, value) => {
    setSelectedFilters(prev => ({ ...prev, [filterType]: value }));
  }, []);

  const openModal = useCallback((user, queryTypeKey) => {
    const mappedKey = queryTypeMapping[queryTypeKey];
    if (mappedKey && user.QUERY_SAMPLES?.[mappedKey]?.length > 0) {
      setModalData({
        isOpen: true,
        user,
        queryType: mappedKey,
        queries: user.QUERY_SAMPLES[mappedKey]
      });
    }
  }, []);

  const closeModal = useCallback(() => {
    setModalData({ isOpen: false, user: null, queryType: null, queries: [] });
  }, []);

  // Reusable Components
  const LoadingSpinner = () => (
    <div className="flex justify-center items-center h-screen bg-blue-50">
      <div className="relative">
        <div className="w-16 h-16 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin"></div>
        <div className="mt-4 text-blue-900 text-lg font-medium">Loading analytics...</div>
      </div>
    </div>
  );

  const ErrorDisplay = () => (
    <div className="flex justify-center items-center h-screen bg-white">
      <div className="bg-white rounded-2xl p-8 border border-red-500 shadow-lg">
        <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
        <p className="text-gray-800 text-center text-lg font-medium">
          {error.message}
        </p>
        <p className="text-gray-600 text-center mt-2">Please ensure the Flask server is running on port 5000</p>
      </div>
    </div>
  );

  const SearchAndFilter = () => (
    <div className="bg-white rounded-2xl p-6 border border-gray-200 mb-8 shadow-md">
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        <div className="lg:col-span-2">
          <div className="relative">
            <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 text-gray-400 w-5 h-5" />
            <input
              type="text"
              placeholder="Search users..."
              className="w-full pl-12 pr-4 py-3 bg-white border border-gray-300 rounded-xl text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all duration-300"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />
          </div>
        </div>
        
        <div>
          <select
            className="w-full px-4 py-3 bg-white border border-gray-300 rounded-xl text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all duration-300"
            value={selectedFilters.costStatus}
            onChange={(e) => handleFilterChange('costStatus', e.target.value)}
          >
            <option value="all">All Statuses</option>
            <option value="High Cost">High Cost</option>
            <option value="Low Cost">Low Cost</option>
          </select>
        </div>
        
        <div>
          <select
            className="w-full px-4 py-3 bg-white border border-gray-300 rounded-xl text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all duration-300"
            value={selectedFilters.scoreRange}
            onChange={(e) => handleFilterChange('scoreRange', e.target.value)}
          >
            <option value="all">All Scores</option>
            <option value="high">High (80+)</option>
            <option value="medium">Medium (50-79)</option>
            <option value="low">Low (&lt;50)</option>
          </select>
        </div>
      </div>
    </div>
  );

  const TableHeader = ({ config }) => {
    const Icon = config.icon;
    const isActive = sortConfig.key === config.key;
    
    return (
      <th
        className={`px-6 py-4 text-left text-xs font-bold text-gray-600 uppercase tracking-wider cursor-pointer hover:text-blue-600 transition-all duration-300 ${
          config.sortable ? 'select-none' : ''
        }`}
        onClick={() => config.sortable && handleSort(config.key)}
      >
        <div className="flex items-center space-x-2 group">
          {Icon && <Icon className="w-4 h-4 text-gray-500" />}
          <span>{config.label}</span>
          {config.sortable && (
            <div className="flex flex-col">
              <TrendingUp className={`w-3 h-3 ${isActive && sortConfig.direction === 'asc' ? 'text-blue-500' : 'text-gray-400'} group-hover:text-blue-400 transition-colors`} />
              <TrendingDown className={`w-3 h-3 -mt-1 ${isActive && sortConfig.direction === 'desc' ? 'text-blue-500' : 'text-gray-400'} group-hover:text-blue-400 transition-colors`} />
            </div>
          )}
        </div>
      </th>
    );
  };

  const TableCell = ({ user, config }) => {
    const value = user[config.key];
    const mappedKey = queryTypeMapping[config.key];
    const isClickable = config.type === 'clickable' && mappedKey && user.QUERY_SAMPLES?.[mappedKey]?.length > 0 && value > 0;

    const cellContent = () => {
      switch (config.type) {
        case 'status':
          return (
            <span className={`px-3 py-1 text-xs font-bold rounded-full flex items-center space-x-1 w-fit ${
              value === 'High Cost' 
                ? 'bg-red-100 text-red-700 border border-red-200' 
                : 'bg-blue-100 text-blue-700 border border-blue-200'
            }`}>
              {value === 'High Cost' ? <AlertCircle className="w-3 h-3" /> : <CheckCircle className="w-3 h-3" />}
              <span>{value}</span>
            </span>
          );
        case 'score':
          const score = value || 0;
          const scoreColor = score >= 80 ? 'text-blue-600' : score >= 50 ? 'text-yellow-600' : 'text-red-600';
          return (
            <div className="flex items-center space-x-2">
              <div className={`w-2 h-2 rounded-full ${score >= 80 ? 'bg-blue-600' : score >= 50 ? 'bg-yellow-600' : 'bg-red-600'}`}></div>
              <span className={`font-bold ${scoreColor}`}>{formatters.number(value)}</span>
            </div>
          );
        case 'clickable':
          return (
            <span
              className={`font-medium transition-all duration-300 ${
                isClickable 
                  ? 'text-blue-600 hover:text-blue-400 cursor-pointer hover:underline transform hover:scale-105' 
                  : 'text-gray-500'
              }`}
              onClick={() => isClickable && openModal(user, config.key)}
            >
              {formatters.number(value)}
            </span>
          );
        default:
          return <span className="text-gray-800 font-medium">{formatters.number(value)}</span>;
      }
    };

    return (
      <td className="px-6 py-4 whitespace-nowrap text-sm">
        {cellContent()}
      </td>
    );
  };

  const QueryModal = () => {
    if (!modalData.isOpen) return null;

    return (
      <div className="fixed inset-0 z-50 overflow-y-auto bg-black/50 backdrop-blur-sm flex items-center justify-center p-4">
        <div className="relative w-full max-w-6xl max-h-[90vh] bg-white rounded-2xl shadow-2xl border border-blue-500/30 flex flex-col">
          <div className="flex-shrink-0 px-8 py-6 border-b border-gray-200 flex justify-between items-center bg-blue-50 rounded-t-2xl">
            <div>
              <h3 className="text-2xl font-bold text-gray-900">
                <span className="text-blue-600">{modalData.user?.USER_NAME}</span>
              </h3>
              <p className="text-gray-600 mt-1">{formatters.queryType(modalData.queryType)} Queries</p>
            </div>
            <button 
              onClick={closeModal}
              className="text-gray-400 hover:text-gray-800 transition-colors p-2 hover:bg-gray-100 rounded-full"
            >
              <X className="w-6 h-6" />
            </button>
          </div>
          
          <div className="flex-grow overflow-y-auto p-8 space-y-6">
            {modalData.queries.map((query, index) => (
              <div key={index} className="bg-gray-50 rounded-xl p-6 border border-gray-200 hover:border-blue-500/50 transition-all duration-300">
                <div className="bg-gray-100 rounded-lg p-4 mb-4 border border-gray-200">
                  <pre className="overflow-x-auto text-sm text-gray-800 font-mono leading-relaxed">
                    <code>{query.query_text}</code>
                  </pre>
                </div>
                
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                  {[
                    { label: 'Query ID', value: query.query_id, icon: Database },
                    { label: 'Start Time', value: new Date(query.start_time).toLocaleString(), icon: Clock },
                    { label: 'Execution Time', value: formatters.milliseconds(query.execution_time_ms), icon: Zap },
                    { label: 'Bytes Scanned', value: formatters.bytes(query.bytes_scanned), icon: TrendingUp },
                    { label: 'Warehouse', value: query.warehouse_size, icon: Database },
                    { label: 'Status', value: query.execution_status, icon: CheckCircle },
                  ].filter(item => item.value).map((item, idx) => (
                    <div key={idx} className="bg-white rounded-lg p-3 border border-gray-200">
                      <div className="flex items-center space-x-2 mb-1">
                        <item.icon className="w-4 h-4 text-blue-500" />
                        <span className="text-gray-500 text-xs font-medium uppercase">{item.label}</span>
                      </div>
                      <span className="text-gray-800 font-medium">{item.value}</span>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
          
          <div className="flex-shrink-0 px-8 py-6 border-t border-gray-200 bg-gray-50 rounded-b-2xl">
            <button
              onClick={closeModal}
              className="px-6 py-3 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-xl transition-all duration-300 transform hover:scale-105 hover:shadow-lg hover:shadow-blue-500/25"
            >
              Close
            </button>
          </div>
        </div>
      </div>
    );
  };

  // Main Render Logic
  if (loading) return <LoadingSpinner />;
  if (error) return <ErrorDisplay />;

  return (
    <div className="min-h-screen bg-blue-50 p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-5xl font-bold text-gray-900 mb-4">
            FinOps Analytics Dashboard
          </h1>
          <p className="text-gray-600 text-lg max-w-2xl mx-auto">
            Advanced query performance analytics with intelligent insights and optimization recommendations
          </p>
        </div>

        {/* Search and Filters */}
        <SearchAndFilter />

        {/* Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
          {[
            { label: 'Total Users', value: userData.length, icon: Users, color: 'blue' },
            { label: 'Filtered Results', value: filteredAndSortedData.length, icon: Filter, color: 'blue' },
            { label: 'High Cost Users', value: userData.filter(u => u.COST_STATUS === 'High Cost').length, icon: AlertCircle, color: 'red' },
            { label: 'Avg Score', value: Math.round(userData.reduce((acc, u) => acc + (u.WEIGHTED_SCORE || 0), 0) / userData.length), icon: TrendingUp, color: 'blue' }
          ].map((stat, index) => (
            <div key={index} className="bg-white rounded-2xl p-6 border border-gray-200 hover:border-blue-500 transition-all duration-300 shadow-md">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-gray-600 text-sm font-medium">{stat.label}</p>
                  <p className="text-3xl font-bold text-gray-900 mt-1">{stat.value}</p>
                </div>
                <stat.icon className={`w-8 h-8 text-${stat.color}-500`} />
              </div>
            </div>
          ))}
        </div>

        {/* Main Table */}
        <div className="bg-white rounded-2xl border border-gray-200 overflow-hidden shadow-xl">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-blue-50">
                <tr>
                  {tableConfig.map(config => (
                    <TableHeader key={config.key} config={config} />
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {filteredAndSortedData.map((user, index) => (
                  <tr key={user.USER_NAME} className="hover:bg-blue-50 transition-all duration-300 group">
                    {tableConfig.map(config => (
                      <TableCell key={config.key} user={user} config={config} />
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        {filteredAndSortedData.length === 0 && (
          <div className="text-center py-12">
            <Filter className="w-16 h-16 text-gray-400 mx-auto mb-4" />
            <h3 className="text-xl font-medium text-gray-700 mb-2">No results found</h3>
            <p className="text-gray-500">Try adjusting your search terms or filters</p>
          </div>
        )}
      </div>

      <QueryModal />
    </div>
  );
};

export default FinopsUserTable;