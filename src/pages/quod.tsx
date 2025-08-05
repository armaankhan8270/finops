import React, { useState, useEffect, useContext, createContext, useCallback, useMemo } from 'react';
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
  Home,
  ArrowLeft,
  Eye,
  BarChart3,
  Settings,
  RefreshCw,
  FileText,
  Zap,
  Shield,
  Table as TableIcon,
  ChevronDown,
  ChevronUp,
  ExternalLink,
  Copy,
  CheckCircle,
  XCircle,
  AlertCircle,
  Loader2
} from 'lucide-react';

// Types
interface DrillDownContext {
  level: 'dashboard' | 'warehouse' | 'user' | 'query' | 'details';
  entityType: 'warehouse' | 'database' | 'table' | 'user' | 'serverless' | 'role' | null;
  entityId: string | null;
  entityName: string | null;
  metricCategory: string | null;
  filterContext: any;
  breadcrumb: BreadcrumbItem[];
  data: any;
}

interface BreadcrumbItem {
  label: string;
  type: string;
  value?: any;
  onClick?: () => void;
}

interface WarehouseMetrics {
  warehouse_id: string;
  warehouse_name: string;
  total_queries: number;
  unique_users: number;
  total_credits: number;
  avg_credits_per_query: number;
  active_days: number;
  avg_execution_time_sec: number;
  total_gb_scanned: number;
  queries_0_to_1_sec: number;
  queries_1_to_10_sec: number;
  queries_10_to_30_sec: number;
  queries_30_to_60_sec: number;
  queries_1_to_5_min: number;
  queries_5_min_plus: number;
  select_star_on_large_tables: number;
  unpartitioned_scan_queries: number;
  cartesian_join_queries: number;
  zero_result_expensive_queries: number;
  failed_cancelled_queries: number;
  high_compile_time_queries: number;
  spilled_to_local_queries: number;
  spilled_to_remote_queries: number;
  weekend_credits: number;
  off_hours_credits: number;
  performance_recommendation: string;
  cost_recommendation: string;
}

interface UserMetrics {
  user_warehouse_id: string;
  user_name: string;
  warehouse_name: string;
  warehouse_id: string;
  user_display_name: string;
  user_email: string;
  total_queries: number;
  total_credits: number;
  percentage_of_warehouse_credits: number;
  select_star_queries: number;
  unpartitioned_scan_queries: number;
  spilled_queries: number;
  long_running_queries: number;
  failed_queries: number;
  cost_category: string;
  optimization_status: string;
}

interface QueryHistory {
  query_id: string;
  query_text_preview: string;
  user_name: string;
  warehouse_name: string;
  start_time: string;
  execution_time_ms: number;
  total_credits_used: number;
  gb_scanned: number;
  rows_produced: number;
  execution_status: string;
  performance_bucket: string;
  cost_category: string;
  is_select_star_large: boolean;
  is_unpartitioned_scan: boolean;
  is_spilled_remote: boolean;
  is_long_running: boolean;
  is_failed: boolean;
}

interface QueryDetails {
  query_id: string;
  query_text: string;
  execution_time_ms: number;
  compilation_time_ms: number;
  total_credits: number;
  estimated_cost_usd: number;
  gb_scanned: number;
  rows_produced: number;
  partition_scan_percentage: number;
  optimization_recommendations: string[];
  cost_impact: string;
  performance_impact: string;
}

// API Service
class FinOpsAPI {
  private baseURL = process.env.REACT_APP_API_URL || 'http://localhost:5000/api';

  async request(endpoint: string, options: RequestInit = {}): Promise<any> {
    const response = await fetch(`${this.baseURL}${endpoint}`, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      throw new Error(`API Error: ${response.statusText}`);
    }

    return response.json();
  }

  async getWarehouses(): Promise<WarehouseMetrics[]> {
    return this.request('/warehouses');
  }

  async getUsers(filters: any = {}): Promise<UserMetrics[]> {
    const params = new URLSearchParams(filters).toString();
    return this.request(`/users${params ? `?${params}` : ''}`);
  }

  async getQueries(filters: any = {}): Promise<QueryHistory[]> {
    const params = new URLSearchParams(filters).toString();
    return this.request(`/queries${params ? `?${params}` : ''}`);
  }

  async getQueryDetails(queryId: string): Promise<QueryDetails> {
    return this.request(`/query-details/${queryId}`);
  }

  async getDatabases(): Promise<any[]> {
    return this.request('/databases');
  }

  async getTables(filters: any = {}): Promise<any[]> {
    const params = new URLSearchParams(filters).toString();
    return this.request(`/tables${params ? `?${params}` : ''}`);
  }

  async getServerless(filters: any = {}): Promise<any[]> {
    const params = new URLSearchParams(filters).toString();
    return this.request(`/serverless${params ? `?${params}` : ''}`);
  }

  async getRoles(): Promise<any[]> {
    return this.request('/roles');
  }

  async exportData(endpoint: string): Promise<void> {
    window.open(`${this.baseURL}${endpoint}?export=csv`, '_blank');
  }

  async initializeTables(daysFilter: number = 30): Promise<any> {
    return this.request('/initialize', {
      method: 'POST',
      body: JSON.stringify({ days_filter: daysFilter }),
    });
  }
}

const api = new FinOpsAPI();

// Context
const DrillDownContext = createContext<{
  context: DrillDownContext;
  setContext: (context: Partial<DrillDownContext>) => void;
  resetContext: () => void;
  goBack: () => void;
}>({
  context: {
    level: 'dashboard',
    entityType: null,
    entityId: null,
    entityName: null,
    metricCategory: null,
    filterContext: null,
    breadcrumb: [],
    data: null
  },
  setContext: () => {},
  resetContext: () => {},
  goBack: () => {}
});

// Context Provider
const DrillDownProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [context, setContextState] = useState<DrillDownContext>({
    level: 'dashboard',
    entityType: null,
    entityId: null,
    entityName: null,
    metricCategory: null,
    filterContext: null,
    breadcrumb: [],
    data: null
  });

  const setContext = useCallback((newContext: Partial<DrillDownContext>) => {
    setContextState(prev => ({ ...prev, ...newContext }));
  }, []);

  const resetContext = useCallback(() => {
    setContextState({
      level: 'dashboard',
      entityType: null,
      entityId: null,
      entityName: null,
      metricCategory: null,
      filterContext: null,
      breadcrumb: [],
      data: null
    });
  }, []);

  const goBack = useCallback(() => {
    setContextState(prev => {
      const newBreadcrumb = [...prev.breadcrumb];
      newBreadcrumb.pop();
      
      if (newBreadcrumb.length === 0) {
        return {
          level: 'dashboard',
          entityType: null,
          entityId: null,
          entityName: null,
          metricCategory: null,
          filterContext: null,
          breadcrumb: [],
          data: null
        };
      }

      const lastItem = newBreadcrumb[newBreadcrumb.length - 1];
      return {
        ...prev,
        breadcrumb: newBreadcrumb,
        level: newBreadcrumb.length === 1 ? 'warehouse' : 
               newBreadcrumb.length === 2 ? 'user' : 'query'
      };
    });
  }, []);

  return (
    <DrillDownContext.Provider value={{ context, setContext, resetContext, goBack }}>
      {children}
    </DrillDownContext.Provider>
  );
};

// Utility Components
const LoadingSpinner: React.FC<{ size?: 'sm' | 'md' | 'lg' }> = ({ size = 'md' }) => {
  const sizeClasses = {
    sm: 'w-4 h-4',
    md: 'w-6 h-6',
    lg: 'w-8 h-8'
  };

  return (
    <div className="flex items-center justify-center p-4">
      <Loader2 className={`${sizeClasses[size]} animate-spin text-blue-500`} />
    </div>
  );
};

const ErrorMessage: React.FC<{ message: string; onRetry?: () => void }> = ({ message, onRetry }) => (
  <div className="flex items-center justify-center p-8">
    <div className="text-center">
      <AlertCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
      <p className="text-gray-600 mb-4">{message}</p>
      {onRetry && (
        <button
          onClick={onRetry}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          Try Again
        </button>
      )}
    </div>
  </div>
);

const MetricCard: React.FC<{
  title: string;
  value: number | string;
  icon: React.ReactNode;
  onClick?: () => void;
  variant?: 'default' | 'warning' | 'danger' | 'success';
  subtitle?: string;
  trend?: 'up' | 'down' | 'neutral';
}> = ({ title, value, icon, onClick, variant = 'default', subtitle, trend }) => {
  const variantClasses = {
    default: 'bg-white border-gray-200 hover:border-blue-300',
    warning: 'bg-orange-50 border-orange-200 hover:border-orange-300',
    danger: 'bg-red-50 border-red-200 hover:border-red-300',
    success: 'bg-green-50 border-green-200 hover:border-green-300'
  };

  const textClasses = {
    default: 'text-gray-900',
    warning: 'text-orange-900',
    danger: 'text-red-900',
    success: 'text-green-900'
  };

  const iconClasses = {
    default: 'text-blue-500',
    warning: 'text-orange-500',
    danger: 'text-red-500',
    success: 'text-green-500'
  };

  return (
    <div
      className={`p-6 rounded-lg border-2 transition-all duration-200 ${variantClasses[variant]} ${
        onClick ? 'cursor-pointer hover:shadow-lg transform hover:-translate-y-1' : ''
      }`}
      onClick={onClick}
    >
      <div className="flex items-center justify-between">
        <div className="flex-1">
          <div className="flex items-center space-x-3">
            <div className={iconClasses[variant]}>{icon}</div>
            <div>
              <p className="text-sm font-medium text-gray-600">{title}</p>
              <p className={`text-2xl font-bold ${textClasses[variant]}`}>
                {typeof value === 'number' ? value.toLocaleString() : value}
              </p>
              {subtitle && (
                <p className="text-sm text-gray-500 mt-1">{subtitle}</p>
              )}
            </div>
          </div>
        </div>
        {onClick && (
          <ChevronRight className="w-5 h-5 text-gray-400 group-hover:text-gray-600" />
        )}
      </div>
    </div>
  );
};

const DataTable: React.FC<{
  data: any[];
  columns: Array<{
    key: string;
    label: string;
    render?: (value: any, row: any) => React.ReactNode;
    sortable?: boolean;
    width?: string;
  }>;
  onRowClick?: (row: any) => void;
  loading?: boolean;
  searchable?: boolean;
  exportable?: boolean;
  onExport?: () => void;
}> = ({ data, columns, onRowClick, loading, searchable = true, exportable = true, onExport }) => {
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState<{ key: string; direction: 'asc' | 'desc' } | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 50;

  const filteredData = useMemo(() => {
    if (!searchTerm) return data;
    
    return data.filter(row =>
      Object.values(row).some(value =>
        String(value).toLowerCase().includes(searchTerm.toLowerCase())
      )
    );
  }, [data, searchTerm]);

  const sortedData = useMemo(() => {
    if (!sortConfig) return filteredData;

    return [...filteredData].sort((a, b) => {
      const aValue = a[sortConfig.key];
      const bValue = b[sortConfig.key];

      if (aValue < bValue) return sortConfig.direction === 'asc' ? -1 : 1;
      if (aValue > bValue) return sortConfig.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [filteredData, sortConfig]);

  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * itemsPerPage;
    return sortedData.slice(startIndex, startIndex + itemsPerPage);
  }, [sortedData, currentPage]);

  const totalPages = Math.ceil(sortedData.length / itemsPerPage);

  const handleSort = (key: string) => {
    setSortConfig(current => ({
      key,
      direction: current?.key === key && current.direction === 'asc' ? 'desc' : 'asc'
    }));
  };

  if (loading) return <LoadingSpinner size="lg" />;

  return (
    <div className="space-y-4">
      {/* Table Controls */}
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-4">
          {searchable && (
            <div className="relative">
              <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              />
            </div>
          )}
          <span className="text-sm text-gray-600">
            {sortedData.length} items
          </span>
        </div>
        
        {exportable && onExport && (
          <button
            onClick={onExport}
            className="flex items-center space-x-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
          >
            <Download className="w-4 h-4" />
            <span>Export CSV</span>
          </button>
        )}
      </div>

      {/* Table */}
      <div className="bg-white rounded-lg shadow overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                {columns.map((column) => (
                  <th
                    key={column.key}
                    className={`px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider ${
                      column.sortable ? 'cursor-pointer hover:bg-gray-100' : ''
                    } ${column.width || ''}`}
                    onClick={() => column.sortable && handleSort(column.key)}
                  >
                    <div className="flex items-center space-x-1">
                      <span>{column.label}</span>
                      {column.sortable && sortConfig?.key === column.key && (
                        sortConfig.direction === 'asc' ? 
                        <ChevronUp className="w-4 h-4" /> : 
                        <ChevronDown className="w-4 h-4" />
                      )}
                    </div>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {paginatedData.map((row, index) => (
                <tr
                  key={index}
                  className={`${onRowClick ? 'cursor-pointer hover:bg-gray-50' : ''}`}
                  onClick={() => onRowClick?.(row)}
                >
                  {columns.map((column) => (
                    <td key={column.key} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {column.render ? column.render(row[column.key], row) : row[column.key]}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="bg-white px-4 py-3 flex items-center justify-between border-t border-gray-200">
            <div className="flex-1 flex justify-between sm:hidden">
              <button
                onClick={() => setCurrentPage(prev => Math.max(prev - 1, 1))}
                disabled={currentPage === 1}
                className="relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
              >
                Previous
              </button>
              <button
                onClick={() => setCurrentPage(prev => Math.min(prev + 1, totalPages))}
                disabled={currentPage === totalPages}
                className="ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 disabled:opacity-50"
              >
                Next
              </button>
            </div>
            <div className="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
              <div>
                <p className="text-sm text-gray-700">
                  Showing <span className="font-medium">{(currentPage - 1) * itemsPerPage + 1}</span> to{' '}
                  <span className="font-medium">
                    {Math.min(currentPage * itemsPerPage, sortedData.length)}
                  </span>{' '}
                  of <span className="font-medium">{sortedData.length}</span> results
                </p>
              </div>
              <div>
                <nav className="relative z-0 inline-flex rounded-md shadow-sm -space-x-px">
                  {Array.from({ length: totalPages }, (_, i) => i + 1).map((page) => (
                    <button
                      key={page}
                      onClick={() => setCurrentPage(page)}
                      className={`relative inline-flex items-center px-4 py-2 border text-sm font-medium ${
                        page === currentPage
                          ? 'z-10 bg-blue-50 border-blue-500 text-blue-600'
                          : 'bg-white border-gray-300 text-gray-500 hover:bg-gray-50'
                      }`}
                    >
                      {page}
                    </button>
                  ))}
                </nav>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

const BreadcrumbTrail: React.FC = () => {
  const { context, resetContext, goBack } = useContext(DrillDownContext);

  if (context.breadcrumb.length === 0) return null;

  return (
    <div className="flex items-center space-x-2 mb-6 p-4 bg-gray-50 rounded-lg">
      <button
        onClick={resetContext}
        className="flex items-center space-x-1 text-gray-600 hover:text-blue-600 transition-colors"
      >
        <Home className="w-4 h-4" />
        <span>Dashboard</span>
      </button>
      
      {context.breadcrumb.map((item, index) => (
        <React.Fragment key={index}>
          <ChevronRight className="w-4 h-4 text-gray-400" />
          <button
            onClick={item.onClick || (() => {})}
            className="text-gray-600 hover:text-blue-600 transition-colors"
          >
            {item.label}
          </button>
        </React.Fragment>
      ))}
      
      {context.breadcrumb.length > 0 && (
        <button
          onClick={goBack}
          className="ml-4 flex items-center space-x-1 text-blue-600 hover:text-blue-700 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          <span>Back</span>
        </button>
      )}
    </div>
  );
};

// Main Components
const WarehouseMetricsView: React.FC = () => {
  const [warehouses, setWarehouses] = useState<WarehouseMetrics[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { setContext } = useContext(DrillDownContext);

  useEffect(() => {
    loadWarehouses();
  }, []);

  const loadWarehouses = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await api.getWarehouses();
      setWarehouses(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load warehouses');
    } finally {
      setLoading(false);
    }
  };

  const handleMetricClick = (warehouse: WarehouseMetrics, metricKey: string, metricValue: number) => {
    setContext({
      level: 'user',
      entityType: 'warehouse',
      entityId: warehouse.warehouse_id,
      entityName: warehouse.warehouse_name,
      metricCategory: metricKey,
      filterContext: { metricValue, warehouse },
      breadcrumb: [
        { label: 'Warehouses', type: 'entity' },
        { label: warehouse.warehouse_name, type: 'name' },
        { label: metricKey.replace(/_/g, ' ').toUpperCase(), type: 'metric' }
      ]
    });
  };

  const columns = [
    {
      key: 'warehouse_name',
      label: 'Warehouse',
      render: (value: string) => (
        <div className="font-medium text-gray-900">{value}</div>
      ),
      sortable: true
    },
    {
      key: 'total_queries',
      label: 'Total Queries',
      render: (value: number, row: WarehouseMetrics) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            handleMetricClick(row, 'total_queries', value);
          }}
          className="text-blue-600 hover:text-blue-800 font-medium"
        >
          {value.toLocaleString()}
        </button>
      ),
      sortable: true
    },
    {
      key: 'total_credits',
      label: 'Total Credits',
      render: (value: number, row: WarehouseMetrics) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            handleMetricClick(row, 'total_credits', value);
          }}
          className="text-green-600 hover:text-green-800 font-medium"
        >
          ${value.toFixed(2)}
        </button>
      ),
      sortable: true
    },
    {
      key: 'unique_users',
      label: 'Users',
      render: (value: number, row: WarehouseMetrics) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            handleMetricClick(row, 'unique_users', value);
          }}
          className="text-purple-600 hover:text-purple-800 font-medium"
        >
          {value}
        </button>
      ),
      sortable: true
    },
    {
      key: 'unpartitioned_scan_queries',
      label: 'Unpartitioned Scans',
      render: (value: number, row: WarehouseMetrics) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            handleMetricClick(row, 'unpartitioned_scan_queries', value);
          }}
          className={`font-medium ${value > 0 ? 'text-red-600 hover:text-red-800' : 'text-gray-400'}`}
        >
          {value}
        </button>
      ),
      sortable: true
    },
    {
      key: 'select_star_on_large_tables',
      label: 'SELECT * Queries',
      render: (value: number, row: WarehouseMetrics) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            handleMetricClick(row, 'select_star_on_large_tables', value);
          }}
          className={`font-medium ${value > 0 ? 'text-orange-600 hover:text-orange-800' : 'text-gray-400'}`}
        >
          {value}
        </button>
      ),
      sortable: true
    },
    {
      key: 'spilled_to_remote_queries',
      label: 'Remote Spill',
      render: (value: number, row: WarehouseMetrics) => (
        <button
          onClick={(e) => {
            e.stopPropagation();
            handleMetricClick(row, 'spilled_to_remote_queries', value);
          }}
          className={`font-medium ${value > 0 ? 'text-red-600 hover:text-red-800' : 'text-gray-400'}`}
        >
          {value}
        </button>
      ),
      sortable: true
    },
    {
      key: 'performance_recommendation',
      label: 'Recommendation',
      render: (value: string) => (
        <div className="text-sm text-gray-600 max-w-xs truncate" title={value}>
          {value}
        </div>
      )
    }
  ];

  if (error) {
    return <ErrorMessage message={error} onRetry={loadWarehouses} />;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Warehouse Analytics</h1>
          <p className="text-gray-600 mt-2">Monitor warehouse performance, costs, and optimization opportunities</p>
        </div>
        <button
          onClick={loadWarehouses}
          className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <RefreshCw className="w-4 h-4" />
          <span>Refresh</span>
        </button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard
          title="Total Warehouses"
          value={warehouses.length}
          icon={<Server className="w-6 h-6" />}
          variant="default"
        />
        <MetricCard
          title="Total Credits"
          value={`$${warehouses.reduce((sum, w) => sum + w.total_credits, 0).toFixed(2)}`}
          icon={<DollarSign className="w-6 h-6" />}
          variant="success"
        />
        <MetricCard
          title="Total Queries"
          value={warehouses.reduce((sum, w) => sum + w.total_queries, 0).toLocaleString()}
          icon={<Activity className="w-6 h-6" />}
          variant="default"
        />
        <MetricCard
          title="Problem Queries"
          value={warehouses.reduce((sum, w) => sum + w.unpartitioned_scan_queries + w.spilled_to_remote_queries, 0)}
          icon={<AlertTriangle className="w-6 h-6" />}
          variant="danger"
        />
      </div>

      {/* Warehouses Table */}
      <DataTable
        data={warehouses}
        columns={columns}
        loading={loading}
        onExport={() => api.exportData('/warehouses')}
      />
    </div>
  );
};

const UserMetricsView: React.FC = () => {
  const [users, setUsers] = useState<UserMetrics[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { context, setContext } = useContext(DrillDownContext);

  useEffect(() => {
    loadUsers();
  }, [context]);

  const loadUsers = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const filters: any = {};
      if (context.entityName) {
        filters.warehouse_name = context.entityName;
      }
      if (context.metricCategory) {
        // Add specific metric filtering based on drill-down context
        if (context.metricCategory === 'select_star_on_large_tables') {
          filters.select_star_queries = 'true';
        } else if (context.metricCategory === 'unpartitioned_scan_queries') {
          filters.unpartitioned_scan_queries = 'true';
        }
      }
      
      const data = await api.getUsers(filters);
      setUsers(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load users');
    } finally {
      setLoading(false);
    }
  };

  const handleUserClick = (user: UserMetrics) => {
    setContext({
      level: 'query',
      entityType: 'user',
      entityId: user.user_warehouse_id,
      entityName: user.user_name,
      filterContext: { ...context.filterContext, user },
      breadcrumb: [
        ...context.breadcrumb,
        { label: `User: ${user.user_display_name}`, type: 'user' }
      ]
    });
  };

  const columns = [
    {
      key: 'user_display_name',
      label: 'User',
      render: (value: string, row: UserMetrics) => (
        <div>
          <div className="font-medium text-gray-900">{value}</div>
          <div className="text-sm text-gray-500">{row.user_email}</div>
        </div>
      ),
      sortable: true
    },
    {
      key: 'total_queries',
      label: 'Queries',
      render: (value: number) => (
        <span className="font-medium text-blue-600">{value.toLocaleString()}</span>
      ),
      sortable: true
    },
    {
      key: 'total_credits',
      label: 'Credits',
      render: (value: number) => (
        <span className="font-medium text-green-600">${value.toFixed(2)}</span>
      ),
      sortable: true
    },
    {
      key: 'percentage_of_warehouse_credits',
      label: '% of Warehouse',
      render: (value: number) => (
        <div className="flex items-center">
          <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
            <div
              className="bg-blue-600 h-2 rounded-full"
              style={{ width: `${Math.min(value, 100)}%` }}
            ></div>
          </div>
          <span className="text-sm font-medium">{value.toFixed(1)}%</span>
        </div>
      ),
      sortable: true
    },
    {
      key: 'cost_category',
      label: 'Cost Category',
      render: (value: string) => {
        const colors = {
          'High Cost User': 'bg-red-100 text-red-800',
          'Medium Cost User': 'bg-yellow-100 text-yellow-800',
          'Low Cost User': 'bg-green-100 text-green-800'
        };
        return (
          <span className={`px-2 py-1 rounded-full text-xs font-medium ${colors[value as keyof typeof colors] || 'bg-gray-100 text-gray-800'}`}>
            {value}
          </span>
        );
      },
      sortable: true
    },
    {
      key: 'optimization_status',
      label: 'Status',
      render: (value: string) => {
        const colors = {
          'Needs Optimization Training': 'bg-red-100 text-red-800',
          'Needs Query Review': 'bg-yellow-100 text-yellow-800',
          'Good Practices': 'bg-green-100 text-green-800'
        };
        return (
          <span className={`px-2 py-1 rounded-full text-xs font-medium ${colors[value as keyof typeof colors] || 'bg-gray-100 text-gray-800'}`}>
            {value}
          </span>
        );
      },
      sortable: true
    }
  ];

  if (error) {
    return <ErrorMessage message={error} onRetry={loadUsers} />;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">
            User Analytics
            {context.entityName && (
              <span className="text-blue-600"> - {context.entityName}</span>
            )}
          </h1>
          <p className="text-gray-600 mt-2">
            {context.metricCategory 
              ? `Users contributing to ${context.metricCategory.replace(/_/g, ' ')}`
              : 'User performance and cost analysis'
            }
          </p>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard
          title="Total Users"
          value={users.length}
          icon={<Users className="w-6 h-6" />}
          variant="default"
        />
        <MetricCard
          title="High Cost Users"
          value={users.filter(u => u.cost_category === 'High Cost User').length}
          icon={<DollarSign className="w-6 h-6" />}
          variant="danger"
        />
        <MetricCard
          title="Need Training"
          value={users.filter(u => u.optimization_status === 'Needs Optimization Training').length}
          icon={<AlertTriangle className="w-6 h-6" />}
          variant="warning"
        />
        <MetricCard
          title="Good Practices"
          value={users.filter(u => u.optimization_status === 'Good Practices').length}
          icon={<CheckCircle className="w-6 h-6" />}
          variant="success"
        />
      </div>

      {/* Users Table */}
      <DataTable
        data={users}
        columns={columns}
        loading={loading}
        onRowClick={handleUserClick}
        onExport={() => api.exportData('/users')}
      />
    </div>
  );
};

const QueryHistoryView: React.FC = () => {
  const [queries, setQueries] = useState<QueryHistory[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { context, setContext } = useContext(DrillDownContext);

  useEffect(() => {
    loadQueries();
  }, [context]);

  const loadQueries = async () => {
    try {
      setLoading(true);
      setError(null);
      
      const filters: any = {};
      
      // Add context-based filters
      if (context.filterContext?.warehouse?.warehouse_name) {
        filters.warehouse_name = context.filterContext.warehouse.warehouse_name;
      }
      if (context.filterContext?.user?.user_name) {
        filters.user_name = context.filterContext.user.user_name;
      }
      
      // Add metric-specific filters
      if (context.metricCategory) {
        if (context.metricCategory === 'select_star_on_large_tables') {
          filters.is_select_star_large = 'true';
        } else if (context.metricCategory === 'unpartitioned_scan_queries') {
          filters.is_unpartitioned_scan = 'true';
        } else if (context.metricCategory === 'spilled_to_remote_queries') {
          filters.is_spilled_remote = 'true';
        } else if (context.metricCategory === 'queries_5_min_plus') {
          filters.is_long_running = 'true';
        }
      }
      
      const data = await api.getQueries(filters);
      setQueries(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load queries');
    } finally {
      setLoading(false);
    }
  };

  const handleQueryClick = (query: QueryHistory) => {
    setContext({
      level: 'details',
      entityType: 'query',
      entityId: query.query_id,
      entityName: query.query_id.substring(0, 8),
      filterContext: { ...context.filterContext, query },
      breadcrumb: [
        ...context.breadcrumb,
        { label: `Query: ${query.query_id.substring(0, 8)}...`, type: 'query' }
      ]
    });
  };

  const columns = [
    {
      key: 'query_id',
      label: 'Query ID',
      render: (value: string) => (
        <div className="font-mono text-sm text-gray-600">
          {value.substring(0, 8)}...
        </div>
      )
    },
    {
      key: 'query_text_preview',
      label: 'Query Preview',
      render: (value: string) => (
        <div className="max-w-md truncate text-sm" title={value}>
          {value}
        </div>
      )
    },
    {
      key: 'start_time',
      label: 'Start Time',
      render: (value: string) => (
        <div className="text-sm text-gray-600">
          {new Date(value).toLocaleString()}
        </div>
      ),
      sortable: true
    },
    {
      key: 'execution_time_ms',
      label: 'Duration',
      render: (value: number) => {
        const seconds = value / 1000;
        const color = seconds > 300 ? 'text-red-600' : seconds > 60 ? 'text-yellow-600' : 'text-green-600';
        return (
          <span className={`font-medium ${color}`}>
            {seconds > 60 ? `${(seconds / 60).toFixed(1)}m` : `${seconds.toFixed(1)}s`}
          </span>
        );
      },
      sortable: true
    },
    {
      key: 'total_credits_used',
      label: 'Credits',
      render: (value: number) => (
        <span className="font-medium text-green-600">${value.toFixed(3)}</span>
      ),
      sortable: true
    },
    {
      key: 'gb_scanned',
      label: 'Data Scanned',
      render: (value: number) => (
        <span className="font-medium text-blue-600">{value.toFixed(2)} GB</span>
      ),
      sortable: true
    },
    {
      key: 'execution_status',
      label: 'Status',
      render: (value: string) => {
        const colors = {
          'SUCCESS': 'bg-green-100 text-green-800',
          'FAIL': 'bg-red-100 text-red-800',
          'CANCELLED': 'bg-yellow-100 text-yellow-800'
        };
        return (
          <span className={`px-2 py-1 rounded-full text-xs font-medium ${colors[value as keyof typeof colors] || 'bg-gray-100 text-gray-800'}`}>
            {value}
          </span>
        );
      },
      sortable: true
    },
    {
      key: 'issues',
      label: 'Issues',
      render: (value: any, row: QueryHistory) => {
        const issues = [];
        if (row.is_select_star_large) issues.push('SELECT *');
        if (row.is_unpartitioned_scan) issues.push('Unpartitioned');
        if (row.is_spilled_remote) issues.push('Remote Spill');
        if (row.is_long_running) issues.push('Long Running');
        if (row.is_failed) issues.push('Failed');
        
        return (
          <div className="flex flex-wrap gap-1">
            {issues.map((issue, index) => (
              <span
                key={index}
                className="px-1 py-0.5 bg-red-100 text-red-700 text-xs rounded"
              >
                {issue}
              </span>
            ))}
          </div>
        );
      }
    }
  ];

  if (error) {
    return <ErrorMessage message={error} onRetry={loadQueries} />;
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Query History</h1>
          <p className="text-gray-600 mt-2">
            {context.metricCategory 
              ? `Queries matching: ${context.metricCategory.replace(/_/g, ' ')}`
              : 'Detailed query execution history'
            }
          </p>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard
          title="Total Queries"
          value={queries.length}
          icon={<Activity className="w-6 h-6" />}
          variant="default"
        />
        <MetricCard
          title="Failed Queries"
          value={queries.filter(q => q.is_failed).length}
          icon={<XCircle className="w-6 h-6" />}
          variant="danger"
        />
        <MetricCard
          title="Long Running"
          value={queries.filter(q => q.is_long_running).length}
          icon={<Clock className="w-6 h-6" />}
          variant="warning"
        />
        <MetricCard
          title="Total Cost"
          value={`$${queries.reduce((sum, q) => sum + q.total_credits_used, 0).toFixed(2)}`}
          icon={<DollarSign className="w-6 h-6" />}
          variant="success"
        />
      </div>

      {/* Queries Table */}
      <DataTable
        data={queries}
        columns={columns}
        loading={loading}
        onRowClick={handleQueryClick}
        onExport={() => api.exportData('/queries')}
      />
    </div>
  );
};

const QueryDetailsView: React.FC = () => {
  const [queryDetails, setQueryDetails] = useState<QueryDetails | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showFullQuery, setShowFullQuery] = useState(false);
  const { context } = useContext(DrillDownContext);

  useEffect(() => {
    if (context.entityId) {
      loadQueryDetails();
    }
  }, [context.entityId]);

  const loadQueryDetails = async () => {
    try {
      setLoading(true);
      setError(null);
      const data = await api.getQueryDetails(context.entityId!);
      setQueryDetails(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load query details');
    } finally {
      setLoading(false);
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  if (loading) return <LoadingSpinner size="lg" />;
  if (error) return <ErrorMessage message={error} onRetry={loadQueryDetails} />;
  if (!queryDetails) return <ErrorMessage message="Query details not found" />;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-gray-900">Query Analysis</h1>
          <p className="text-gray-600 mt-2">Detailed performance and cost analysis</p>
        </div>
        <div className="flex space-x-2">
          <button
            onClick={() => copyToClipboard(queryDetails.query_text)}
            className="flex items-center space-x-2 px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors"
          >
            <Copy className="w-4 h-4" />
            <span>Copy Query</span>
          </button>
        </div>
      </div>

      {/* Performance Overview */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard
          title="Execution Time"
          value={`${(queryDetails.execution_time_ms / 1000).toFixed(2)}s`}
          icon={<Clock className="w-6 h-6" />}
          variant={queryDetails.performance_impact === 'CRITICAL' ? 'danger' : 
                  queryDetails.performance_impact === 'HIGH' ? 'warning' : 'default'}
        />
        <MetricCard
          title="Total Cost"
          value={`$${queryDetails.estimated_cost_usd.toFixed(3)}`}
          icon={<DollarSign className="w-6 h-6" />}
          variant={queryDetails.cost_impact === 'CRITICAL' ? 'danger' : 
                  queryDetails.cost_impact === 'HIGH' ? 'warning' : 'success'}
        />
        <MetricCard
          title="Data Scanned"
          value={`${queryDetails.gb_scanned.toFixed(2)} GB`}
          icon={<Database className="w-6 h-6" />}
          variant="default"
        />
        <MetricCard
          title="Rows Produced"
          value={queryDetails.rows_produced.toLocaleString()}
          icon={<BarChart3 className="w-6 h-6" />}
          variant="default"
        />
      </div>

      {/* Query Text */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-semibold text-gray-900">Query Text</h2>
          <button
            onClick={() => setShowFullQuery(!showFullQuery)}
            className="flex items-center space-x-2 text-blue-600 hover:text-blue-700"
          >
            <Eye className="w-4 h-4" />
            <span>{showFullQuery ? 'Collapse' : 'Expand'}</span>
          </button>
        </div>
        <div className="bg-gray-50 rounded-lg p-4">
          <pre className={`text-sm text-gray-800 whitespace-pre-wrap ${showFullQuery ? '' : 'max-h-32 overflow-hidden'}`}>
            {queryDetails.query_text}
          </pre>
        </div>
      </div>

      {/* Performance Metrics */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Performance Metrics</h2>
          <div className="space-y-4">
            <div className="flex justify-between">
              <span className="text-gray-600">Compilation Time:</span>
              <span className="font-medium">{(queryDetails.compilation_time_ms / 1000).toFixed(2)}s</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Credits Used:</span>
              <span className="font-medium">{queryDetails.total_credits.toFixed(4)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-600">Partition Scan %:</span>
              <span className={`font-medium ${queryDetails.partition_scan_percentage > 80 ? 'text-red-600' : 'text-green-600'}`}>
                {queryDetails.partition_scan_percentage.toFixed(1)}%
              </span>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-gray-900 mb-4">Impact Assessment</h2>
          <div className="space-y-4">
            <div className="flex justify-between items-center">
              <span className="text-gray-600">Performance Impact:</span>
              <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                queryDetails.performance_impact === 'CRITICAL' ? 'bg-red-100 text-red-800' :
                queryDetails.performance_impact === 'HIGH' ? 'bg-orange-100 text-orange-800' :
                queryDetails.performance_impact === 'MEDIUM' ? 'bg-yellow-100 text-yellow-800' :
                'bg-green-100 text-green-800'
              }`}>
                {queryDetails.performance_impact}
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-gray-600">Cost Impact:</span>
              <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                queryDetails.cost_impact === 'CRITICAL' ? 'bg-red-100 text-red-800' :
                queryDetails.cost_impact === 'HIGH' ? 'bg-orange-100 text-orange-800' :
                queryDetails.cost_impact === 'MEDIUM' ? 'bg-yellow-100 text-yellow-800' :
                'bg-green-100 text-green-800'
              }`}>
                {queryDetails.cost_impact}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Optimization Recommendations */}
      {queryDetails.optimization_recommendations && queryDetails.optimization_recommendations.length > 0 && (
        <div className="bg-blue-50 rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold text-blue-900 mb-4 flex items-center">
            <Zap className="w-5 h-5 mr-2" />
            Optimization Recommendations
          </h2>
          <div className="space-y-3">
            {queryDetails.optimization_recommendations.map((recommendation, index) => (
              <div key={index} className="flex items-start space-x-3">
                <div className="flex-shrink-0 w-6 h-6 bg-blue-600 text-white rounded-full flex items-center justify-center text-sm font-medium">
                  {index + 1}
                </div>
                <p className="text-blue-800">{recommendation}</p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

// Main Dashboard Component
const FinOpsDashboard: React.FC = () => {
  const [activeTab, setActiveTab] = useState<'warehouses' | 'databases' | 'tables' | 'users' | 'serverless' | 'roles'>('warehouses');
  const [isInitializing, setIsInitializing] = useState(false);
  const { context, resetContext } = useContext(DrillDownContext);

  const tabs = [
    { id: 'warehouses', label: 'Warehouses', icon: <Server className="w-5 h-5" /> },
    { id: 'databases', label: 'Databases', icon: <Database className="w-5 h-5" /> },
    { id: 'tables', label: 'Tables', icon: <TableIcon className="w-5 h-5" /> },
    { id: 'users', label: 'Users', icon: <Users className="w-5 h-5" /> },
    { id: 'serverless', label: 'Serverless', icon: <Zap className="w-5 h-5" /> },
    { id: 'roles', label: 'Roles', icon: <Shield className="w-5 h-5" /> }
  ];

  const handleInitialize = async () => {
    try {
      setIsInitializing(true);
      await api.initializeTables(30);
      // Refresh current view
      window.location.reload();
    } catch (error) {
      console.error('Failed to initialize tables:', error);
    } finally {
      setIsInitializing(false);
    }
  };

  const renderContent = () => {
    // Handle drill-down views
    if (context.level === 'details') {
      return <QueryDetailsView />;
    }
    if (context.level === 'query') {
      return <QueryHistoryView />;
    }
    if (context.level === 'user') {
      return <UserMetricsView />;
    }
    if (context.level === 'warehouse' || context.entityType === 'warehouse') {
      return <UserMetricsView />;
    }

    // Handle main dashboard views
    switch (activeTab) {
      case 'warehouses':
        return <WarehouseMetricsView />;
      case 'databases':
        return <div className="text-center py-12 text-gray-500">Database metrics view coming soon...</div>;
      case 'tables':
        return <div className="text-center py-12 text-gray-500">Table metrics view coming soon...</div>;
      case 'users':
        return <UserMetricsView />;
      case 'serverless':
        return <div className="text-center py-12 text-gray-500">Serverless metrics view coming soon...</div>;
      case 'roles':
        return <div className="text-center py-12 text-gray-500">Roles metrics view coming soon...</div>;
      default:
        return <WarehouseMetricsView />;
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                <BarChart3 className="w-8 h-8 text-blue-600" />
                <h1 className="text-xl font-bold text-gray-900">FinOps Analytics</h1>
              </div>
              {context.level !== 'dashboard' && (
                <button
                  onClick={resetContext}
                  className="flex items-center space-x-1 text-gray-600 hover:text-blue-600 transition-colors"
                >
                  <Home className="w-4 h-4" />
                  <span>Dashboard</span>
                </button>
              )}
            </div>
            <div className="flex items-center space-x-4">
              <button
                onClick={handleInitialize}
                disabled={isInitializing}
                className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors disabled:opacity-50"
              >
                {isInitializing ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <RefreshCw className="w-4 h-4" />
                )}
                <span>{isInitializing ? 'Initializing...' : 'Initialize Data'}</span>
              </button>
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <BreadcrumbTrail />

        {/* Navigation Tabs - Only show on main dashboard */}
        {context.level === 'dashboard' && (
          <nav className="mb-8">
            <div className="flex space-x-1 bg-white p-1 rounded-lg shadow">
              {tabs.map((tab) => (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id as any)}
                  className={`flex items-center space-x-2 px-4 py-2 rounded-md transition-colors ${
                    activeTab === tab.id
                      ? 'bg-blue-600 text-white shadow-sm'
                      : 'text-gray-600 hover:text-gray-900 hover:bg-gray-50'
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

export default FinOpsDashboard;