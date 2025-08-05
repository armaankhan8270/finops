import React, { useState, useEffect, useMemo, useCallback } from 'react';
import {
  MagnifyingGlassIcon,
  XMarkIcon,
  FunnelIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  ExclamationTriangleIcon,
  ClockIcon,
  CurrencyDollarIcon,
  ChartBarIcon,
  CodeBracketIcon,
  EyeIcon,
  DocumentTextIcon,
  ArrowUpIcon,
  ArrowDownIcon,
  Bars3Icon,
} from '@heroicons/react/24/outline';

const FinopsUserTable = () => {
  // State Management
  const [userData, setUserData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'WEIGHTED_SCORE', direction: 'desc' });
  const [filters, setFilters] = useState({
    costStatus: 'all',
    minQueries: 0,
    maxQueries: Infinity,
    minScore: 0,
    maxScore: 100,
  });
  const [showFilters, setShowFilters] = useState(false);
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [modalData, setModalData] = useState({
    user: null,
    queryType: null,
    queries: [],
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

  const queryTypeIcons = {
    'spilled': ExclamationTriangleIcon,
    'over_provisioned': ChartBarIcon,
    'peak_hour_long_running': ClockIcon,
    'select_star': EyeIcon,
    'unpartitioned_scan': DocumentTextIcon,
    'repeated_query': ArrowUpIcon,
    'complex_query': CodeBracketIcon,
    'zero_result_query': XMarkIcon,
    'high_compile_time': ClockIcon,
    'untagged_query': Bars3Icon,
    'unlimited_order_by': ArrowDownIcon,
    'large_group_by': ChartBarIcon,
    'slow_query': ClockIcon,
    'expensive_distinct': CurrencyDollarIcon,
    'inefficient_like': MagnifyingGlassIcon,
    'no_results_with_scan': ExclamationTriangleIcon,
    'cartesian_join': CodeBracketIcon,
    'high_compile_ratio': ClockIcon,
  };

  const tableHeaders = [
    {
      key: 'USER_NAME',
      label: 'User',
      sortable: true,
      width: 'w-48',
      sticky: true,
    },
    {
      key: 'TOTAL_QUERIES',
      label: 'Total Queries',
      sortable: true,
      width: 'w-32',
      format: 'number',
    },
    {
      key: 'TOTAL_CREDITS',
      label: 'Credits',
      sortable: true,
      width: 'w-32',
      format: 'number',
    },
    {
      key: 'WEIGHTED_SCORE',
      label: 'Score',
      sortable: true,
      width: 'w-24',
      format: 'score',
    },
    {
      key: 'COST_STATUS',
      label: 'Status',
      sortable: true,
      width: 'w-32',
      format: 'status',
    },
    {
      key: 'SPILLED_QUERIES',
      label: 'Spilled',
      sortable: true,
      width: 'w-24',
      format: 'clickable',
      severity: 'high',
    },
    {
      key: 'OVER_PROVISIONED_QUERIES',
      label: 'Over-Provisioned',
      sortable: true,
      width: 'w-32',
      format: 'clickable',
      severity: 'medium',
    },
    {
      key: 'PEAK_HOUR_LONG_RUNNING_QUERIES',
      label: 'Long Running',
      sortable: true,
      width: 'w-28',
      format: 'clickable',
      severity: 'high',
    },
    {
      key: 'SELECT_STAR_QUERIES',
      label: 'Select *',
      sortable: true,
      width: 'w-24',
      format: 'clickable',
      severity: 'low',
    },
    {
      key: 'UNPARTITIONED_SCAN_QUERIES',
      label: 'Unpartitioned',
      sortable: true,
      width: 'w-28',
      format: 'clickable',
      severity: 'medium',
    },
    {
      key: 'ZERO_RESULT_QUERIES',
      label: 'Zero Results',
      sortable: true,
      width: 'w-28',
      format: 'clickable',
      severity: 'low',
    },
    {
      key: 'HIGH_COMPILE_QUERIES',
      label: 'High Compile',
      sortable: true,
      width: 'w-28',
      format: 'clickable',
      severity: 'medium',
    },
    {
      key: 'UNLIMITED_ORDER_BY_QUERIES',
      label: 'Unlimited Order',
      sortable: true,
      width: 'w-32',
      format: 'clickable',
      severity: 'medium',
    },
    {
      key: 'CARTESIAN_JOIN_QUERIES',
      label: 'Cartesian Join',
      sortable: true,
      width: 'w-32',
      format: 'clickable',
      severity: 'high',
    },
  ];

  // Utility Functions
  const formatters = {
    number: (num) => {
      if (num === null || num === undefined) return 'N/A';
      return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      }).format(num);
    },
    milliseconds: (ms) => {
      if (ms === null || ms === undefined) return 'N/A';
      const seconds = (ms / 1000).toFixed(1);
      return `${seconds}s`;
    },
    bytesToGB: (bytes) => {
      if (bytes === null || bytes === undefined) return 'N/A';
      const gb = bytes / Math.pow(1024, 3);
      return `${gb.toFixed(2)} GB`;
    },
    score: (score) => {
      if (score === null || score === undefined) return 'N/A';
      return `${score.toFixed(1)}`;
    },
  };

  const getSeverityColor = (severity, value) => {
    if (!value || value === 0) return 'text-gray-400';

    const colors = {
      high: 'text-red-600 hover:text-red-800',
      medium: 'text-amber-600 hover:text-amber-800',
      low: 'text-blue-600 hover:text-blue-800',
    };
    return colors[severity] || 'text-indigo-600 hover:text-indigo-800';
  };

  const getQueryTypeName = useCallback((key) => {
    return key
      .replace(/_queries|_query/g, '')
      .replace(/_/g, ' ')
      .toLowerCase()
      .split(' ')
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(' ');
  }, []);

  // Data Fetching
 // Data Fetching
useEffect(() => {
  const fetchUserData = async () => {
    try {
      const response = await fetch('http://localhost:5000/api/users');
      if (!response.ok) {
        throw new Error('Network response was not ok');
      }
      const data = await response.json();
      setUserData(data);
    } catch (error) {
      console.error('Fetch error:', error);
      setError(error);
    } finally {
      setLoading(false);
    }
  };
  fetchUserData();
}, []);
  // Filtering and Sorting Logic
  const filteredAndSortedData = useMemo(() => {
    let filtered = userData.filter((user) => {
      const matchesSearch = user.USER_NAME.toLowerCase().includes(
        searchTerm.toLowerCase()
      );
      const matchesCostStatus =
        filters.costStatus === 'all' || user.COST_STATUS === filters.costStatus;
      const matchesQueryRange =
        user.TOTAL_QUERIES >= filters.minQueries &&
        user.TOTAL_QUERIES <= filters.maxQueries;
      const matchesScoreRange =
        user.WEIGHTED_SCORE >= filters.minScore &&
        user.WEIGHTED_SCORE <= filters.maxScore;

      return matchesSearch && matchesCostStatus && matchesQueryRange && matchesScoreRange;
    });

    if (sortConfig.key) {
      filtered.sort((a, b) => {
        let aVal = a[sortConfig.key];
        let bVal = b[sortConfig.key];

        if (typeof aVal === 'string') {
          aVal = aVal.toLowerCase();
          bVal = bVal.toLowerCase();
        }

        if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
        if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
        return 0;
      });
    }

    return filtered;
  }, [userData, searchTerm, filters, sortConfig]);

  // Event Handlers
  const handleSort = useCallback((key) => {
    setSortConfig((prevConfig) => ({
      key,
      direction: prevConfig.key === key && prevConfig.direction === 'desc' ? 'asc' : 'desc',
    }));
  }, []);

  const openModal = useCallback(
    (user, queryTypeKey) => {
      const mappedKey = queryTypeMapping[queryTypeKey];
      const queries = user.QUERY_SAMPLES?.[mappedKey] ?? [];
      setModalData({
        user: user,
        queryType: mappedKey,
        queries: queries,
      });
      setIsModalOpen(true);
    },
    [queryTypeMapping]
  );

  const closeModal = useCallback(() => {
    setIsModalOpen(false);
    setModalData({ user: null, queryType: null, queries: [] });
  }, []);

  const resetFilters = useCallback(() => {
    setFilters({
      costStatus: 'all',
      minQueries: 0,
      maxQueries: Infinity,
      minScore: 0,
      maxScore: 100,
    });
    setSearchTerm('');
  }, []);

  // Component Renderers
  const renderLoadingState = () => (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-100 flex items-center justify-center">
      <div className="text-center space-y-4">
        <div className="relative">
          <div className="w-16 h-16 border-4 border-indigo-200 border-t-indigo-600 rounded-full animate-spin mx-auto"></div>
          <div className="absolute inset-0 w-16 h-16 border-4 border-transparent border-r-indigo-400 rounded-full animate-ping mx-auto"></div>
        </div>
        <p className="text-xl font-semibold text-gray-700">Loading FinOps Analytics...</p>
        <p className="text-sm text-gray-500">Analyzing query performance data</p>
      </div>
    </div>
  );

  const renderErrorState = () => (
    <div className="min-h-screen bg-gradient-to-br from-red-50 via-rose-50 to-pink-100 flex items-center justify-center p-4">
      <div className="bg-white rounded-2xl shadow-2xl p-8 max-w-md w-full text-center">
        <ExclamationTriangleIcon className="w-16 h-16 text-red-500 mx-auto mb-4" />
        <h2 className="text-2xl font-bold text-gray-800 mb-2">Connection Error</h2>
        <p className="text-gray-600 mb-4">{error.message}</p>
        <p className="text-sm text-gray-500">Please ensure the Flask server is running on port 5000.</p>
      </div>
    </div>
  );

  const renderTableCell = (user, header) => {
    const value = user[header.key];
    const mappedKey = queryTypeMapping[header.key];
    
    // The cell is clickable if the value is greater than 0
    const isClickable = value > 0;
    
    // Choose the icon based on the value
    const IconComponent = isClickable ? ClockIcon : queryTypeIcons[mappedKey];

    const cellClasses = `px-6 py-4 whitespace-nowrap text-sm ${header.width} ${
      header.sticky ? 'sticky left-0 bg-white z-20 border-r border-gray-200' : ''
    } ${isClickable ? 'cursor-pointer hover:bg-indigo-50 transition-colors' : ''}`;

    const cellContent = () => {
      switch (header.format) {
        case 'status':
          return (
            <span
              className={`px-3 py-1 inline-flex text-xs leading-5 font-semibold rounded-full ${
                value === 'High Cost' ? 'bg-red-100 text-red-800 border border-red-200' : 'bg-green-100 text-green-800 border border-green-200'
              }`}
            >
              {value}
            </span>
          );

        case 'score':
          const score = parseFloat(value);
          const scoreColor = score >= 80 ? 'text-green-600' : score >= 60 ? 'text-amber-600' : 'text-red-600';
          return (
            <div className="flex items-center space-x-2">
              <span className={`font-bold ${scoreColor}`}>{formatters.score(value)}</span>
              <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                <div
                  className={`h-full transition-all duration-300 ${
                    score >= 80 ? 'bg-green-500' : score >= 60 ? 'bg-amber-500' : 'bg-red-500'
                  }`}
                  style={{ width: `${Math.min(score, 100)}%` }}
                />
              </div>
            </div>
          );

        case 'clickable':
          const severity = header.severity;
          return (
            <div
              className={`flex items-center space-x-2 ${getSeverityColor(severity, value)} transition-all duration-200 ${
                isClickable ? 'hover:scale-105 font-semibold' : ''
              }`}
              onClick={() => isClickable && openModal(user, header.key)}
            >
              {IconComponent && <IconComponent className="w-4 h-4" />}
              <span>{formatters.number(value)}</span>
              {isClickable && (
                <ChevronDownIcon className="w-3 h-3 opacity-60" />
              )}
            </div>
          );

        case 'number':
          return <span className="font-medium text-gray-900">{formatters.number(value)}</span>;

        default:
          return <span className="font-medium text-gray-900">{value}</span>;
      }
    };

    return (
      <td key={header.key} className={cellClasses}>
        {cellContent()}
      </td>
    );
  };

  const renderQueryModal = () => (
    <div className="fixed inset-0 z-50 overflow-hidden bg-black bg-opacity-60 backdrop-blur-sm flex items-center justify-center p-4 transition-all duration-300">
      <div className="relative transform overflow-hidden rounded-2xl bg-white text-left shadow-2xl transition-all w-full max-w-6xl max-h-[95vh] flex flex-col">
        {/* Modal Header */}
        <div className="flex-shrink-0 bg-gradient-to-r from-indigo-600 to-purple-600 px-8 py-6 text-white">
          <div className="flex justify-between items-center">
            <div className="flex items-center space-x-4">
              {queryTypeIcons[modalData.queryType] && React.createElement(queryTypeIcons[modalData.queryType], { className: 'w-8 h-8' })}
              <div>
                <h3 className="text-2xl font-bold">{modalData.user?.USER_NAME}</h3>
                <p className="text-indigo-100 text-lg">
                  {getQueryTypeName(modalData.queryType)} Queries ({modalData.queries.length})
                </p>
              </div>
            </div>
            <button
              onClick={closeModal}
              className="text-white hover:text-indigo-200 transition-colors p-2 rounded-full hover:bg-white hover:bg-opacity-10"
            >
              <XMarkIcon className="h-6 w-6" />
            </button>
          </div>
        </div>

        {/* Modal Body */}
        <div className="flex-grow overflow-y-auto p-8 bg-gray-50">
          {modalData.queries.length > 0 ? (
            <div className="space-y-6">
              {modalData.queries.map((query, index) => (
                <div
                  key={index}
                  className="bg-white rounded-xl shadow-lg overflow-hidden border border-gray-200 hover:shadow-xl transition-shadow"
                >
                  {/* Query Header */}
                  <div className="bg-gray-800 px-6 py-4">
                    <div className="flex justify-between items-center mb-3">
                      <h4 className="text-lg font-semibold text-white flex items-center space-x-2">
                        <CodeBracketIcon className="w-5 h-5" />
                        <span>Query #{index + 1}</span>
                      </h4>
                      <span className="px-3 py-1 bg-green-600 text-white text-xs font-medium rounded-full">
                        {query.execution_status}
                      </span>
                    </div>
                    <div className="bg-gray-900 rounded-lg p-4 overflow-hidden">
                      <pre className="overflow-x-auto text-sm text-green-300 font-mono leading-relaxed whitespace-pre-wrap">
                        <code>{query.query_text}</code>
                      </pre>
                    </div>
                  </div>

                  {/* Query Metrics */}
                  <div className="p-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                      <div className="bg-blue-50 rounded-lg p-4 border border-blue-200">
                        <div className="flex items-center space-x-2 mb-2">
                          <DocumentTextIcon className="w-5 h-5 text-blue-600" />
                          <span className="font-semibold text-blue-900">Query ID</span>
                        </div>
                        <p className="text-blue-800 font-mono text-sm">{query.query_id}</p>
                      </div>

                      <div className="bg-purple-50 rounded-lg p-4 border border-purple-200">
                        <div className="flex items-center space-x-2 mb-2">
                          <ClockIcon className="w-5 h-5 text-purple-600" />
                          <span className="font-semibold text-purple-900">Execution Time</span>
                        </div>
                        <p className="text-purple-800 font-semibold">{formatters.milliseconds(query.execution_time_ms)}</p>
                      </div>

                      <div className="bg-green-50 rounded-lg p-4 border border-green-200">
                        <div className="flex items-center space-x-2 mb-2">
                          <ChartBarIcon className="w-5 h-5 text-green-600" />
                          <span className="font-semibold text-green-900">Start Time</span>
                        </div>
                        <p className="text-green-800 text-sm">{new Date(query.start_time).toLocaleString()}</p>
                      </div>

                      {query.bytes_scanned && (
                        <div className="bg-amber-50 rounded-lg p-4 border border-amber-200">
                          <div className="flex items-center space-x-2 mb-2">
                            <DocumentTextIcon className="w-5 h-5 text-amber-600" />
                            <span className="font-semibold text-amber-900">Data Scanned</span>
                          </div>
                          <p className="text-amber-800 font-semibold">{formatters.bytesToGB(query.bytes_scanned)}</p>
                        </div>
                      )}

                      {query.warehouse_size && (
                        <div className="bg-indigo-50 rounded-lg p-4 border border-indigo-200">
                          <div className="flex items-center space-x-2 mb-2">
                            <Bars3Icon className="w-5 h-5 text-indigo-600" />
                            <span className="font-semibold text-indigo-900">Warehouse</span>
                          </div>
                          <p className="text-indigo-800 font-semibold">{query.warehouse_size}</p>
                        </div>
                      )}

                      {query.rows_produced && (
                        <div className="bg-rose-50 rounded-lg p-4 border border-rose-200">
                          <div className="flex items-center space-x-2 mb-2">
                            <ChartBarIcon className="w-5 h-5 text-rose-600" />
                            <span className="font-semibold text-rose-900">Rows Produced</span>
                          </div>
                          <p className="text-rose-800 font-semibold">{formatters.number(query.rows_produced)}</p>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-12">
              <ExclamationTriangleIcon className="w-16 h-16 text-gray-300 mx-auto mb-4" />
              <p className="text-gray-500 text-lg">No sample queries available for this type.</p>
            </div>
          )}
        </div>

        {/* Modal Footer */}
        <div className="bg-white px-8 py-4 border-t border-gray-200 flex justify-end">
          <button
            type="button"
            className="inline-flex items-center justify-center rounded-xl border border-transparent bg-gradient-to-r from-indigo-600 to-purple-600 px-8 py-3 text-sm font-semibold text-white shadow-lg hover:from-indigo-700 hover:to-purple-700 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-all duration-200 hover:scale-105"
            onClick={closeModal}
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );

  // Loading and Error States
  if (loading) return renderLoadingState();
  if (error) return renderErrorState();

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-blue-50 to-indigo-100">
      {/* Header Section */}
      <div className="bg-white shadow-lg border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-8 py-8">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between space-y-4 lg:space-y-0">
            <div>
              <h1 className="text-4xl font-bold bg-gradient-to-r from-indigo-600 to-purple-600 bg-clip-text text-transparent">
                FinOps Analytics Dashboard
              </h1>
              <p className="text-gray-600 mt-2 text-lg">Advanced query performance insights and optimization opportunities</p>
            </div>

            <div className="flex items-center space-x-4">
              <div className="bg-gradient-to-r from-green-400 to-blue-500 text-white px-4 py-2 rounded-full text-sm font-medium">
                {filteredAndSortedData.length} Users
              </div>
              <button
                onClick={() => setShowFilters(!showFilters)}
                className="inline-flex items-center rounded-full border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 shadow-sm hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 transition-colors"
              >
                <FunnelIcon className="h-5 w-5 mr-2 text-gray-500" />
                Filters
                {showFilters ? <ChevronUpIcon className="ml-2 h-4 w-4" /> : <ChevronDownIcon className="ml-2 h-4 w-4" />}
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content Area */}
      <div className="max-w-7xl mx-auto py-8 px-4 sm:px-6 lg:px-8">
        {/* Filter Section */}
        {showFilters && (
          <div className="bg-white rounded-2xl shadow-xl p-6 mb-8 border border-gray-200 transition-all duration-300">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              {/* Search Bar */}
              <div className="col-span-full md:col-span-1">
                <label htmlFor="search" className="block text-sm font-medium text-gray-700">
                  Search User
                </label>
                <div className="mt-1 relative rounded-md shadow-sm">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
                    <MagnifyingGlassIcon className="h-5 w-5 text-gray-400" />
                  </div>
                  <input
                    type="text"
                    name="search"
                    id="search"
                    className="block w-full rounded-md border-gray-300 pl-10 focus:border-indigo-500 focus:ring-indigo-500 text-sm"
                    placeholder="Search by user name..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                  />
                </div>
              </div>

              {/* Cost Status Filter */}
              <div>
                <label htmlFor="costStatus" className="block text-sm font-medium text-gray-700">
                  Cost Status
                </label>
                <select
                  id="costStatus"
                  name="costStatus"
                  className="mt-1 block w-full rounded-md border-gray-300 py-2 pl-3 pr-10 text-sm focus:border-indigo-500 focus:ring-indigo-500"
                  value={filters.costStatus}
                  onChange={(e) => setFilters({ ...filters, costStatus: e.target.value })}
                >
                  <option value="all">All</option>
                  <option value="High Cost">High Cost</option>
                  <option value="Optimal">Optimal</option>
                </select>
              </div>

              {/* Score Range Filter */}
              <div>
                <label htmlFor="minScore" className="block text-sm font-medium text-gray-700">
                  Score Range
                </label>
                <div className="mt-1 flex items-center space-x-2">
                  <input
                    type="number"
                    id="minScore"
                    className="block w-1/2 rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 text-sm"
                    value={filters.minScore}
                    onChange={(e) => setFilters({ ...filters, minScore: Number(e.target.value) })}
                  />
                  <span>-</span>
                  <input
                    type="number"
                    id="maxScore"
                    className="block w-1/2 rounded-md border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 text-sm"
                    value={filters.maxScore === Infinity ? '' : filters.maxScore}
                    onChange={(e) =>
                      setFilters({
                        ...filters,
                        maxScore: e.target.value === '' ? Infinity : Number(e.target.value),
                      })
                    }
                  />
                </div>
              </div>
            </div>
            <div className="mt-6 flex justify-end space-x-3">
              <button
                onClick={resetFilters}
                className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none"
              >
                Reset
              </button>
            </div>
          </div>
        )}

        {/* Table Section */}
        <div className="bg-white shadow-2xl rounded-2xl overflow-hidden border border-gray-200">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50 sticky top-0 z-10 shadow-md">
                <tr>
                  {tableHeaders.map((header) => (
                    <th
                      key={header.key}
                      scope="col"
                      className={`px-6 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wider ${header.width} ${
                        header.sticky ? 'sticky left-0 bg-gray-50 z-20 border-r border-gray-200' : ''
                      } ${header.sortable ? 'cursor-pointer hover:bg-gray-100 transition-colors' : ''}`}
                      onClick={() => header.sortable && handleSort(header.key)}
                    >
                      <div className="flex items-center space-x-2">
                        <span>{header.label}</span>
                        {header.sortable &&
                          (sortConfig.key === header.key ? (
                            sortConfig.direction === 'asc' ? (
                              <ChevronUpIcon className="h-4 w-4 text-gray-400" />
                            ) : (
                              <ChevronDownIcon className="h-4 w-4 text-gray-400" />
                            )
                          ) : (
                            <ChevronDownIcon className="h-4 w-4 text-gray-300 opacity-0 group-hover:opacity-100 transition-opacity" />
                          ))}
                      </div>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {filteredAndSortedData.length > 0 ? (
                  filteredAndSortedData.map((user) => (
                    <tr key={user.USER_NAME} className="hover:bg-gray-50 transition-colors duration-150">
                      {tableHeaders.map((header) => renderTableCell(user, header))}
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={tableHeaders.length} className="px-6 py-12 text-center text-gray-500 text-lg">
                      No users found matching the current filters.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
      {isModalOpen && modalData.user && renderQueryModal()}
    </div>
  );
};

export default FinopsUserTable;