from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import snowflake.connector
import pandas as pd
import os
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Any
import io

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Snowflake connection configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER'),
    'password': os.getenv('SNOWFLAKE_PASSWORD'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
    'database': os.getenv('SNOWFLAKE_DATABASE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA')
}

class SnowflakeConnection:
    def __init__(self):
        self.connection = None
    
    def connect(self):
        try:
            self.connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logger.info("Successfully connected to Snowflake")
            return self.connection
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {str(e)}")
            raise
    
    def execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch results and column names
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Create DataFrame
            df = pd.DataFrame(results, columns=columns)
            cursor.close()
            return df
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_procedure(self, procedure_name: str, params: List = None):
        if not self.connection:
            self.connect()
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.callproc(procedure_name, params)
            else:
                cursor.callproc(procedure_name)
            cursor.close()
            logger.info(f"Successfully executed procedure: {procedure_name}")
        except Exception as e:
            logger.error(f"Procedure execution failed: {str(e)}")
            raise

# Initialize Snowflake connection
sf_conn = SnowflakeConnection()

# Stored Procedures Creation
STORED_PROCEDURES = {
    'warehouse_metrics': """
    CREATE OR REPLACE PROCEDURE REFRESH_WAREHOUSE_METRICS()
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    BEGIN
        -- Create or replace warehouse metrics table
        CREATE OR REPLACE TABLE FINOPS_WAREHOUSE_METRICS AS
        WITH warehouse_base AS (
            SELECT 
                warehouse_name,
                COUNT(*) as total_queries,
                SUM(credits_used_cloud_services + credits_used_compute) as total_credits,
                COUNT(DISTINCT DATE(start_time)) as active_days,
                AVG(CASE WHEN execution_status = 'SUCCESS' 
                    THEN DATEDIFF('second', start_time, end_time) END) as avg_execution_time
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            GROUP BY warehouse_name
        ),
        performance_buckets AS (
            SELECT 
                warehouse_name,
                SUM(CASE WHEN execution_time_ms BETWEEN 0 AND 1000 THEN 1 ELSE 0 END) as queries_0_to_1_sec,
                SUM(CASE WHEN execution_time_ms BETWEEN 1001 AND 10000 THEN 1 ELSE 0 END) as queries_1_to_10_sec,
                SUM(CASE WHEN execution_time_ms BETWEEN 10001 AND 30000 THEN 1 ELSE 0 END) as queries_10_to_30_sec,
                SUM(CASE WHEN execution_time_ms BETWEEN 30001 AND 60000 THEN 1 ELSE 0 END) as queries_30_to_60_sec,
                SUM(CASE WHEN execution_time_ms BETWEEN 60001 AND 300000 THEN 1 ELSE 0 END) as queries_1_to_5_min,
                SUM(CASE WHEN execution_time_ms > 300000 THEN 1 ELSE 0 END) as queries_5_min_plus
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        ),
        bad_practices AS (
            SELECT 
                warehouse_name,
                SUM(CASE WHEN query_text ILIKE '%SELECT *%' THEN 1 ELSE 0 END) as select_star_queries,
                SUM(CASE WHEN partitions_scanned > partitions_total * 0.8 THEN 1 ELSE 0 END) as unpartitioned_scan_queries,
                SUM(CASE WHEN query_text ILIKE '%CROSS JOIN%' OR query_text ILIKE '%CARTESIAN%' THEN 1 ELSE 0 END) as cartesian_join_queries,
                SUM(CASE WHEN rows_produced = 0 THEN 1 ELSE 0 END) as zero_result_queries,
                SUM(CASE WHEN execution_status IN ('FAIL', 'CANCELLED') THEN 1 ELSE 0 END) as failed_cancelled_queries,
                SUM(CASE WHEN compilation_time_ms > 5000 THEN 1 ELSE 0 END) as high_compile_time_queries,
                SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) as spilled_to_local_queries,
                SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) as spilled_to_remote_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        ),
        cost_efficiency AS (
            SELECT 
                warehouse_name,
                SUM(CASE WHEN DAYOFWEEK(start_time) IN (1, 7) 
                    THEN credits_used_cloud_services + credits_used_compute ELSE 0 END) as weekend_idle_credits,
                AVG(queue_time_ms) as queue_wait_time_avg_ms
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        )
        SELECT 
            wb.warehouse_name,
            wb.total_queries,
            wb.total_credits,
            (wb.active_days * 24.0 / 30) as active_hours_per_day,
            COALESCE(pb.queries_0_to_1_sec, 0) as queries_0_to_1_sec,
            COALESCE(pb.queries_1_to_10_sec, 0) as queries_1_to_10_sec,
            COALESCE(pb.queries_10_to_30_sec, 0) as queries_10_to_30_sec,
            COALESCE(pb.queries_30_to_60_sec, 0) as queries_30_to_60_sec,
            COALESCE(pb.queries_1_to_5_min, 0) as queries_1_to_5_min,
            COALESCE(pb.queries_5_min_plus, 0) as queries_5_min_plus,
            COALESCE(bp.select_star_queries, 0) as select_star_queries,
            COALESCE(bp.unpartitioned_scan_queries, 0) as unpartitioned_scan_queries,
            COALESCE(bp.cartesian_join_queries, 0) as cartesian_join_queries,
            COALESCE(bp.zero_result_queries, 0) as zero_result_queries,
            COALESCE(bp.failed_cancelled_queries, 0) as failed_cancelled_queries,
            COALESCE(bp.high_compile_time_queries, 0) as high_compile_time_queries,
            COALESCE(bp.spilled_to_local_queries, 0) as spilled_to_local_queries,
            COALESCE(bp.spilled_to_remote_queries, 0) as spilled_to_remote_queries,
            COALESCE(ce.weekend_idle_credits, 0) as weekend_idle_credits,
            COALESCE(ce.queue_wait_time_avg_ms, 0) as queue_wait_time_avg_ms,
            CURRENT_TIMESTAMP() as last_updated
        FROM warehouse_base wb
        LEFT JOIN performance_buckets pb ON wb.warehouse_name = pb.warehouse_name
        LEFT JOIN bad_practices bp ON wb.warehouse_name = bp.warehouse_name
        LEFT JOIN cost_efficiency ce ON wb.warehouse_name = ce.warehouse_name;
        
        RETURN 'Warehouse metrics refreshed successfully';
    END;
    $$;
    """,
    
    'database_metrics': """
    CREATE OR REPLACE PROCEDURE REFRESH_DATABASE_METRICS()
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    BEGIN
        CREATE OR REPLACE TABLE FINOPS_DATABASE_METRICS AS
        WITH database_storage AS (
            SELECT 
                database_name,
                SUM(storage_bytes) / (1024*1024*1024) as total_storage_gb,
                COUNT(DISTINCT table_name) as table_count,
                SUM(time_travel_bytes) / (1024*1024*1024) as time_travel_storage_gb
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE deleted IS NULL
            GROUP BY database_name
        ),
        query_patterns AS (
            SELECT 
                database_name,
                SUM(CASE WHEN query_text ILIKE '%SELECT *%' THEN 1 ELSE 0 END) as select_star_on_large_tables,
                SUM(CASE WHEN partitions_scanned > partitions_total * 0.8 THEN 1 ELSE 0 END) as unpartitioned_scans_count,
                SUM(CASE WHEN partitions_scanned = partitions_total THEN 1 ELSE 0 END) as full_table_scan_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            AND database_name IS NOT NULL
            GROUP BY database_name
        )
        SELECT 
            ds.database_name,
            ds.total_storage_gb,
            ds.table_count,
            ds.time_travel_storage_gb,
            COALESCE(qp.select_star_on_large_tables, 0) as select_star_on_large_tables,
            COALESCE(qp.unpartitioned_scans_count, 0) as unpartitioned_scans_count,
            COALESCE(qp.full_table_scan_queries, 0) as full_table_scan_queries,
            CURRENT_TIMESTAMP() as last_updated
        FROM database_storage ds
        LEFT JOIN query_patterns qp ON ds.database_name = qp.database_name;
        
        RETURN 'Database metrics refreshed successfully';
    END;
    $$;
    """,
    
    'user_metrics': """
    CREATE OR REPLACE PROCEDURE REFRESH_USER_METRICS()
    RETURNS STRING
    LANGUAGE SQL
    AS
    $$
    BEGIN
        CREATE OR REPLACE TABLE FINOPS_USER_METRICS AS
        WITH user_base AS (
            SELECT 
                user_name,
                warehouse_name,
                COUNT(*) as total_queries,
                SUM(credits_used_cloud_services + credits_used_compute) as total_credits,
                SUM(CASE WHEN query_text ILIKE '%SELECT *%' THEN 1 ELSE 0 END) as select_star_queries,
                SUM(CASE WHEN partitions_scanned > partitions_total * 0.8 THEN 1 ELSE 0 END) as unpartitioned_scan_queries,
                SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) as spilled_queries,
                SUM(CASE WHEN execution_time_ms > 300000 THEN 1 ELSE 0 END) as long_running_queries,
                SUM(CASE WHEN rows_produced = 0 THEN 1 ELSE 0 END) as zero_result_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            AND user_name IS NOT NULL
            AND warehouse_name IS NOT NULL
            GROUP BY user_name, warehouse_name
        ),
        warehouse_totals AS (
            SELECT 
                warehouse_name,
                SUM(total_credits) as warehouse_total_credits
            FROM user_base
            GROUP BY warehouse_name
        )
        SELECT 
            ub.user_name,
            ub.warehouse_name,
            ub.total_queries,
            ub.total_credits,
            (ub.total_credits / wt.warehouse_total_credits * 100) as percentage_of_warehouse_usage,
            ub.select_star_queries,
            ub.unpartitioned_scan_queries,
            ub.spilled_queries,
            ub.long_running_queries,
            ub.zero_result_queries,
            CASE 
                WHEN ub.total_credits > 1000 THEN 'High Cost'
                ELSE 'Low Cost'
            END as cost_status,
            CURRENT_TIMESTAMP() as last_updated
        FROM user_base ub
        JOIN warehouse_totals wt ON ub.warehouse_name = wt.warehouse_name;
        
        RETURN 'User metrics refreshed successfully';
    END;
    $$;
    """
}

def create_stored_procedures():
    """Create all stored procedures in Snowflake"""
    try:
        for proc_name, proc_sql in STORED_PROCEDURES.items():
            sf_conn.execute_query(proc_sql)
            logger.info(f"Created stored procedure: {proc_name}")
    except Exception as e:
        logger.error(f"Failed to create stored procedures: {str(e)}")
        raise

def refresh_all_metrics():
    """Refresh all metric tables using stored procedures"""
    procedures = [
        'REFRESH_WAREHOUSE_METRICS',
        'REFRESH_DATABASE_METRICS', 
        'REFRESH_USER_METRICS'
    ]
    
    for proc in procedures:
        try:
            sf_conn.execute_procedure(proc)
            logger.info(f"Refreshed metrics using: {proc}")
        except Exception as e:
            logger.error(f"Failed to refresh {proc}: {str(e)}")
            raise

# API Routes

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

@app.route('/api/refresh-metrics', methods=['POST'])
def refresh_metrics():
    """Refresh all metrics tables"""
    try:
        refresh_all_metrics()
        return jsonify({
            'status': 'success',
            'message': 'All metrics refreshed successfully',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/warehouses', methods=['GET'])
def get_warehouse_metrics():
    """Get warehouse metrics data"""
    try:
        query = """
        SELECT * FROM FINOPS_WAREHOUSE_METRICS
        ORDER BY total_credits DESC
        """
        df = sf_conn.execute_query(query)
        
        # Convert to JSON format expected by frontend
        data = df.to_dict('records')
        
        # Save to CSV if requested
        if request.args.get('export') == 'csv':
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return send_file(
                io.BytesIO(csv_buffer.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'warehouse_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error fetching warehouse metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases', methods=['GET'])
def get_database_metrics():
    """Get database metrics data"""
    try:
        query = """
        SELECT * FROM FINOPS_DATABASE_METRICS
        ORDER BY total_storage_gb DESC
        """
        df = sf_conn.execute_query(query)
        data = df.to_dict('records')
        
        if request.args.get('export') == 'csv':
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return send_file(
                io.BytesIO(csv_buffer.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'database_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error fetching database metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/users', methods=['GET'])
def get_user_metrics():
    """Get user metrics data with optional filtering"""
    try:
        warehouse_filter = request.args.get('warehouse')
        metric_filter = request.args.get('metric')
        
        query = "SELECT * FROM FINOPS_USER_METRICS"
        conditions = []
        
        if warehouse_filter:
            conditions.append(f"warehouse_name = '{warehouse_filter}'")
        
        if metric_filter:
            # Add specific metric filtering logic based on drill-down context
            if metric_filter == 'select_star_queries':
                conditions.append("select_star_queries > 0")
            elif metric_filter == 'unpartitioned_scan_queries':
                conditions.append("unpartitioned_scan_queries > 0")
            elif metric_filter == 'spilled_queries':
                conditions.append("spilled_queries > 0")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY total_credits DESC"
        
        df = sf_conn.execute_query(query)
        data = df.to_dict('records')
        
        if request.args.get('export') == 'csv':
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return send_file(
                io.BytesIO(csv_buffer.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'user_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error fetching user metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/queries', methods=['GET'])
def get_query_history():
    """Get query history for specific user/warehouse/metric combination"""
    try:
        user_name = request.args.get('user')
        warehouse_name = request.args.get('warehouse')
        metric_type = request.args.get('metric_type')
        limit = request.args.get('limit', 100)
        
        query = """
        SELECT 
            query_id,
            user_name,
            warehouse_name,
            LEFT(query_text, 100) as query_text_preview,
            start_time,
            execution_time_ms,
            bytes_scanned,
            credits_used_cloud_services + credits_used_compute as credits_used,
            error_code,
            execution_status
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        """
        
        conditions = []
        if user_name:
            conditions.append(f"user_name = '{user_name}'")
        if warehouse_name:
            conditions.append(f"warehouse_name = '{warehouse_name}'")
        
        # Add metric-specific filtering
        if metric_type:
            if metric_type == 'select_star_queries':
                conditions.append("query_text ILIKE '%SELECT *%'")
            elif metric_type == 'unpartitioned_scan_queries':
                conditions.append("partitions_scanned > partitions_total * 0.8")
            elif metric_type == 'spilled_queries':
                conditions.append("bytes_spilled_to_local_storage > 0")
            elif metric_type == 'long_running_queries':
                conditions.append("execution_time_ms > 300000")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += f" ORDER BY start_time DESC LIMIT {limit}"
        
        df = sf_conn.execute_query(query)
        data = df.to_dict('records')
        
        if request.args.get('export') == 'csv':
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            return send_file(
                io.BytesIO(csv_buffer.getvalue().encode()),
                mimetype='text/csv',
                as_attachment=True,
                download_name=f'query_history_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            )
        
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error fetching query history: {str(e)}")
        return jsonify({'error': str(e)}), 500

# @app.route('/api/query/```


# ```text
# Flask==2.3.3
# Flask-CORS==4.0.0
# snowflake-connector-python==3.4.0
# pandas==2.1.1
# python-dotenv==1.0.0
# gunicorn==21.2.0