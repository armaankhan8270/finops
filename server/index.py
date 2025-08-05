from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import io
import uuid

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FinOpsAnalytics:
    def __init__(self, snowflake_cursor):
        self.cursor = snowflake_cursor
        self.days_filter = 30  # Default to 30 days
    
    def set_time_filter(self, days: int):
        """Set the time filter for data extraction"""
        self.days_filter = days
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            columns = [desc[0] for desc in self.cursor.description]
            return pd.DataFrame(results, columns=columns)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def create_warehouse_metrics_table(self):
        """Create comprehensive warehouse metrics with drill-down IDs"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_WAREHOUSE_METRICS AS
        WITH warehouse_base AS (
            SELECT 
                warehouse_name,
                warehouse_id,
                COUNT(*) as total_queries,
                COUNT(DISTINCT user_name) as unique_users,
                SUM(credits_used_cloud_services + credits_used_compute) as total_credits,
                AVG(credits_used_cloud_services + credits_used_compute) as avg_credits_per_query,
                COUNT(DISTINCT DATE(start_time)) as active_days,
                AVG(CASE WHEN execution_status = 'SUCCESS' 
                    THEN DATEDIFF('second', start_time, end_time) END) as avg_execution_time_sec,
                SUM(bytes_scanned) / (1024*1024*1024) as total_gb_scanned,
                SUM(rows_produced) as total_rows_produced
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name, warehouse_id
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
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        ),
        bad_practices AS (
            SELECT 
                warehouse_name,
                SUM(CASE WHEN query_text ILIKE '%SELECT *%' AND bytes_scanned > 1073741824 THEN 1 ELSE 0 END) as select_star_on_large_tables,
                SUM(CASE WHEN partitions_scanned > partitions_total * 0.8 AND partitions_total > 10 THEN 1 ELSE 0 END) as unpartitioned_scan_queries,
                SUM(CASE WHEN query_text ILIKE '%CROSS JOIN%' OR query_text ILIKE '%CARTESIAN%' THEN 1 ELSE 0 END) as cartesian_join_queries,
                SUM(CASE WHEN rows_produced = 0 AND execution_time_ms > 5000 THEN 1 ELSE 0 END) as zero_result_expensive_queries,
                SUM(CASE WHEN execution_status IN ('FAIL', 'CANCELLED') THEN 1 ELSE 0 END) as failed_cancelled_queries,
                SUM(CASE WHEN compilation_time_ms > 10000 THEN 1 ELSE 0 END) as high_compile_time_queries,
                SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) as spilled_to_local_queries,
                SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) as spilled_to_remote_queries,
                SUM(CASE WHEN query_text NOT ILIKE '%WHERE%' AND query_text ILIKE '%SELECT%' 
                    AND bytes_scanned > 1073741824 THEN 1 ELSE 0 END) as missing_where_clause_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        ),
        cost_efficiency AS (
            SELECT 
                warehouse_name,
                SUM(CASE WHEN DAYOFWEEK(start_time) IN (1, 7) 
                    THEN credits_used_cloud_services + credits_used_compute ELSE 0 END) as weekend_credits,
                SUM(CASE WHEN HOUR(start_time) BETWEEN 22 AND 6 
                    THEN credits_used_cloud_services + credits_used_compute ELSE 0 END) as off_hours_credits,
                AVG(queue_time_ms) as avg_queue_wait_time_ms,
                SUM(CASE WHEN queue_time_ms > 30000 THEN 1 ELSE 0 END) as high_queue_time_queries,
                COUNT(DISTINCT CASE WHEN credits_used_cloud_services + credits_used_compute = 0 
                    THEN query_id END) as zero_credit_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        ),
        recommendations AS (
            SELECT 
                warehouse_name,
                CASE 
                    WHEN AVG(queue_time_ms) > 10000 THEN 'Consider increasing warehouse size or using multi-cluster'
                    WHEN SUM(CASE WHEN execution_time_ms > 300000 THEN 1 ELSE 0 END) > 50 THEN 'Review long-running queries for optimization'
                    WHEN SUM(CASE WHEN bytes_spilled_to_remote_storage > 0 THEN 1 ELSE 0 END) > 20 THEN 'Increase warehouse size to reduce spilling'
                    ELSE 'Performance looks good'
                END as performance_recommendation,
                CASE 
                    WHEN SUM(CASE WHEN DAYOFWEEK(start_time) IN (1, 7) THEN credits_used_cloud_services + credits_used_compute ELSE 0 END) > 
                         SUM(credits_used_cloud_services + credits_used_compute) * 0.3 THEN 'High weekend usage - consider auto-suspend'
                    WHEN AVG(credits_used_cloud_services + credits_used_compute) < 0.1 THEN 'Consider using smaller warehouse size'
                    ELSE 'Cost efficiency looks reasonable'
                END as cost_recommendation
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND warehouse_name IS NOT NULL
            GROUP BY warehouse_name
        )
        SELECT 
            COALESCE(wb.warehouse_id, HASH(wb.warehouse_name)) as warehouse_id,
            wb.warehouse_name,
            wb.total_queries,
            wb.unique_users,
            wb.total_credits,
            wb.avg_credits_per_query,
            wb.active_days,
            wb.avg_execution_time_sec,
            wb.total_gb_scanned,
            wb.total_rows_produced,
            COALESCE(pb.queries_0_to_1_sec, 0) as queries_0_to_1_sec,
            COALESCE(pb.queries_1_to_10_sec, 0) as queries_1_to_10_sec,
            COALESCE(pb.queries_10_to_30_sec, 0) as queries_10_to_30_sec,
            COALESCE(pb.queries_30_to_60_sec, 0) as queries_30_to_60_sec,
            COALESCE(pb.queries_1_to_5_min, 0) as queries_1_to_5_min,
            COALESCE(pb.queries_5_min_plus, 0) as queries_5_min_plus,
            COALESCE(bp.select_star_on_large_tables, 0) as select_star_on_large_tables,
            COALESCE(bp.unpartitioned_scan_queries, 0) as unpartitioned_scan_queries,
            COALESCE(bp.cartesian_join_queries, 0) as cartesian_join_queries,
            COALESCE(bp.zero_result_expensive_queries, 0) as zero_result_expensive_queries,
            COALESCE(bp.failed_cancelled_queries, 0) as failed_cancelled_queries,
            COALESCE(bp.high_compile_time_queries, 0) as high_compile_time_queries,
            COALESCE(bp.spilled_to_local_queries, 0) as spilled_to_local_queries,
            COALESCE(bp.spilled_to_remote_queries, 0) as spilled_to_remote_queries,
            COALESCE(bp.missing_where_clause_queries, 0) as missing_where_clause_queries,
            COALESCE(ce.weekend_credits, 0) as weekend_credits,
            COALESCE(ce.off_hours_credits, 0) as off_hours_credits,
            COALESCE(ce.avg_queue_wait_time_ms, 0) as avg_queue_wait_time_ms,
            COALESCE(ce.high_queue_time_queries, 0) as high_queue_time_queries,
            COALESCE(ce.zero_credit_queries, 0) as zero_credit_queries,
            r.performance_recommendation,
            r.cost_recommendation,
            CURRENT_TIMESTAMP() as last_updated
        FROM warehouse_base wb
        LEFT JOIN performance_buckets pb ON wb.warehouse_name = pb.warehouse_name
        LEFT JOIN bad_practices bp ON wb.warehouse_name = bp.warehouse_name
        LEFT JOIN cost_efficiency ce ON wb.warehouse_name = ce.warehouse_name
        LEFT JOIN recommendations r ON wb.warehouse_name = r.warehouse_name
        """
        self.execute_query(query)
        logger.info("Created FINOPS_WAREHOUSE_METRICS table")
    
    def create_user_warehouse_usage_table(self):
        """Create user-warehouse usage table for drill-down"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_USER_WAREHOUSE_USAGE AS
        WITH user_warehouse_base AS (
            SELECT 
                COALESCE(qh.user_name, 'UNKNOWN') as user_name,
                COALESCE(qh.warehouse_name, 'UNKNOWN') as warehouse_name,
                COALESCE(qh.warehouse_id, HASH(qh.warehouse_name)) as warehouse_id,
                COALESCE(u.name, qh.user_name) as user_display_name,
                COALESCE(u.email, qh.user_name || '@company.com') as user_email,
                COUNT(*) as total_queries,
                SUM(credits_used_cloud_services + credits_used_compute) as total_credits,
                AVG(credits_used_cloud_services + credits_used_compute) as avg_credits_per_query,
                SUM(bytes_scanned) / (1024*1024*1024) as total_gb_scanned,
                AVG(execution_time_ms) as avg_execution_time_ms,
                COUNT(DISTINCT DATE(start_time)) as active_days
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON qh.user_name = u.name
            WHERE qh.start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND qh.user_name IS NOT NULL
            AND qh.warehouse_name IS NOT NULL
            GROUP BY qh.user_name, qh.warehouse_name, qh.warehouse_id, u.name, u.email
        ),
        user_bad_practices AS (
            SELECT 
                user_name,
                warehouse_name,
                SUM(CASE WHEN query_text ILIKE '%SELECT *%' AND bytes_scanned > 1073741824 THEN 1 ELSE 0 END) as select_star_queries,
                SUM(CASE WHEN partitions_scanned > partitions_total * 0.8 AND partitions_total > 10 THEN 1 ELSE 0 END) as unpartitioned_scan_queries,
                SUM(CASE WHEN bytes_spilled_to_local_storage > 0 THEN 1 ELSE 0 END) as spilled_queries,
                SUM(CASE WHEN execution_time_ms > 300000 THEN 1 ELSE 0 END) as long_running_queries,
                SUM(CASE WHEN rows_produced = 0 AND execution_time_ms > 5000 THEN 1 ELSE 0 END) as zero_result_expensive_queries,
                SUM(CASE WHEN compilation_time_ms > 10000 THEN 1 ELSE 0 END) as high_compile_time_queries,
                SUM(CASE WHEN execution_status IN ('FAIL', 'CANCELLED') THEN 1 ELSE 0 END) as failed_queries,
                SUM(CASE WHEN query_text NOT ILIKE '%WHERE%' AND query_text ILIKE '%SELECT%' 
                    AND bytes_scanned > 1073741824 THEN 1 ELSE 0 END) as missing_where_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND user_name IS NOT NULL
            AND warehouse_name IS NOT NULL
            GROUP BY user_name, warehouse_name
        ),
        user_cost_patterns AS (
            SELECT 
                user_name,
                warehouse_name,
                SUM(CASE WHEN DAYOFWEEK(start_time) IN (1, 7) 
                    THEN credits_used_cloud_services + credits_used_compute ELSE 0 END) as weekend_credits,
                SUM(CASE WHEN HOUR(start_time) BETWEEN 22 AND 6 
                    THEN credits_used_cloud_services + credits_used_compute ELSE 0 END) as off_hours_credits,
                COUNT(CASE WHEN credits_used_cloud_services + credits_used_compute > 1 THEN 1 END) as expensive_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND user_name IS NOT NULL
            AND warehouse_name IS NOT NULL
            GROUP BY user_name, warehouse_name
        ),
        warehouse_totals AS (
            SELECT 
                warehouse_name,
                SUM(total_credits) as warehouse_total_credits,
                SUM(total_queries) as warehouse_total_queries
            FROM user_warehouse_base
            GROUP BY warehouse_name
        )
        SELECT 
            HASH(uwb.user_name || uwb.warehouse_name) as user_warehouse_id,
            uwb.user_name,
            uwb.warehouse_name,
            uwb.warehouse_id,
            uwb.user_display_name,
            uwb.user_email,
            uwb.total_queries,
            uwb.total_credits,
            uwb.avg_credits_per_query,
            uwb.total_gb_scanned,
            uwb.avg_execution_time_ms,
            uwb.active_days,
            ROUND((uwb.total_credits / NULLIF(wt.warehouse_total_credits, 0) * 100), 2) as percentage_of_warehouse_credits,
            ROUND((uwb.total_queries / NULLIF(wt.warehouse_total_queries, 0) * 100), 2) as percentage_of_warehouse_queries,
            COALESCE(ubp.select_star_queries, 0) as select_star_queries,
            COALESCE(ubp.unpartitioned_scan_queries, 0) as unpartitioned_scan_queries,
            COALESCE(ubp.spilled_queries, 0) as spilled_queries,
            COALESCE(ubp.long_running_queries, 0) as long_running_queries,
            COALESCE(ubp.zero_result_expensive_queries, 0) as zero_result_expensive_queries,
            COALESCE(ubp.high_compile_time_queries, 0) as high_compile_time_queries,
            COALESCE(ubp.failed_queries, 0) as failed_queries,
            COALESCE(ubp.missing_where_queries, 0) as missing_where_queries,
            COALESCE(ucp.weekend_credits, 0) as weekend_credits,
            COALESCE(ucp.off_hours_credits, 0) as off_hours_credits,
            COALESCE(ucp.expensive_queries, 0) as expensive_queries,
            CASE 
                WHEN uwb.total_credits > 100 THEN 'High Cost User'
                WHEN uwb.total_credits > 50 THEN 'Medium Cost User'
                ELSE 'Low Cost User'
            END as cost_category,
            CASE 
                WHEN COALESCE(ubp.select_star_queries, 0) + COALESCE(ubp.unpartitioned_scan_queries, 0) + 
                     COALESCE(ubp.spilled_queries, 0) > 10 THEN 'Needs Optimization Training'
                WHEN COALESCE(ubp.failed_queries, 0) > 5 THEN 'Needs Query Review'
                ELSE 'Good Practices'
            END as optimization_status,
            CURRENT_TIMESTAMP() as last_updated
        FROM user_warehouse_base uwb
        LEFT JOIN user_bad_practices ubp ON uwb.user_name = ubp.user_name AND uwb.warehouse_name = ubp.warehouse_name
        LEFT JOIN user_cost_patterns ucp ON uwb.user_name = ucp.user_name AND uwb.warehouse_name = ucp.warehouse_name
        LEFT JOIN warehouse_totals wt ON uwb.warehouse_name = wt.warehouse_name
        """
        self.execute_query(query)
        logger.info("Created FINOPS_USER_WAREHOUSE_USAGE table")
    
    def create_database_metrics_table(self):
        """Create database metrics table"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_DATABASE_METRICS AS
        WITH database_storage AS (
            SELECT 
                database_name,
                database_id,
                SUM(storage_bytes) / (1024*1024*1024) as total_storage_gb,
                SUM(failsafe_bytes) / (1024*1024*1024) as failsafe_storage_gb,
                SUM(time_travel_bytes) / (1024*1024*1024) as time_travel_storage_gb,
                COUNT(DISTINCT table_name) as table_count,
                COUNT(DISTINCT schema_name) as schema_count,
                AVG(storage_bytes) / (1024*1024*1024) as avg_table_size_gb
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE deleted IS NULL
            GROUP BY database_name, database_id
        ),
        database_query_patterns AS (
            SELECT 
                database_name,
                COUNT(*) as total_queries,
                COUNT(DISTINCT user_name) as unique_users,
                SUM(credits_used_cloud_services + credits_used_compute) as total_credits,
                SUM(CASE WHEN query_text ILIKE '%SELECT *%' AND bytes_scanned > 1073741824 THEN 1 ELSE 0 END) as select_star_on_large_tables,
                SUM(CASE WHEN partitions_scanned > partitions_total * 0.8 AND partitions_total > 10 THEN 1 ELSE 0 END) as unpartitioned_scans_count,
                SUM(CASE WHEN partitions_scanned = partitions_total AND partitions_total > 1 THEN 1 ELSE 0 END) as full_table_scan_queries,
                SUM(CASE WHEN query_text ILIKE '%JOIN%' AND query_text NOT ILIKE '%ON%' THEN 1 ELSE 0 END) as missing_join_conditions,
                SUM(CASE WHEN execution_time_ms > 300000 THEN 1 ELSE 0 END) as long_running_queries,
                AVG(bytes_scanned) / (1024*1024*1024) as avg_gb_scanned_per_query
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND database_name IS NOT NULL
            GROUP BY database_name
        ),
        database_recommendations AS (
            SELECT 
                database_name,
                CASE 
                    WHEN SUM(time_travel_bytes) / SUM(storage_bytes) > 0.5 THEN 'Consider reducing time travel retention'
                    WHEN COUNT(DISTINCT table_name) > 1000 THEN 'Consider database partitioning or archival'
                    WHEN AVG(storage_bytes) / (1024*1024*1024) > 100 THEN 'Review large tables for optimization'
                    ELSE 'Storage optimization looks good'
                END as storage_recommendation
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE deleted IS NULL
            GROUP BY database_name
        )
        SELECT 
            COALESCE(ds.database_id, HASH(ds.database_name)) as database_id,
            ds.database_name,
            ds.total_storage_gb,
            ds.failsafe_storage_gb,
            ds.time_travel_storage_gb,
            ds.table_count,
            ds.schema_count,
            ds.avg_table_size_gb,
            COALESCE(dqp.total_queries, 0) as total_queries,
            COALESCE(dqp.unique_users, 0) as unique_users,
            COALESCE(dqp.total_credits, 0) as total_credits,
            COALESCE(dqp.select_star_on_large_tables, 0) as select_star_on_large_tables,
            COALESCE(dqp.unpartitioned_scans_count, 0) as unpartitioned_scans_count,
            COALESCE(dqp.full_table_scan_queries, 0) as full_table_scan_queries,
            COALESCE(dqp.missing_join_conditions, 0) as missing_join_conditions,
            COALESCE(dqp.long_running_queries, 0) as long_running_queries,
            COALESCE(dqp.avg_gb_scanned_per_query, 0) as avg_gb_scanned_per_query,
            dr.storage_recommendation,
            CURRENT_TIMESTAMP() as last_updated
        FROM database_storage ds
        LEFT JOIN database_query_patterns dqp ON ds.database_name = dqp.database_name
        LEFT JOIN database_recommendations dr ON ds.database_name = dr.database_name
        """
        self.execute_query(query)
        logger.info("Created FINOPS_DATABASE_METRICS table")
    
    def create_table_metrics_table(self):
        """Create table-level metrics"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_TABLE_METRICS AS
        WITH table_storage AS (
            SELECT 
                database_name,
                schema_name,
                table_name,
                table_id,
                storage_bytes / (1024*1024*1024) as storage_gb,
                time_travel_bytes / (1024*1024*1024) as time_travel_gb,
                failsafe_bytes / (1024*1024*1024) as failsafe_gb,
                row_count,
                comment as table_comment
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE deleted IS NULL
        ),
        table_query_patterns AS (
            SELECT 
                database_name,
                REGEXP_SUBSTR(query_text, 'FROM\\s+([^\\s]+)', 1, 1, 'i', 1) as table_reference,
                COUNT(*) as query_count,
                SUM(CASE WHEN partitions_scanned = partitions_total AND partitions_total > 1 THEN 1 ELSE 0 END) as full_table_scans_count,
                SUM(CASE WHEN query_text ILIKE '%SELECT *%' THEN 1 ELSE 0 END) as select_star_count,
                SUM(bytes_scanned) / (1024*1024*1024) as total_gb_scanned,
                AVG(execution_time_ms) as avg_execution_time_ms,
                COUNT(DISTINCT user_name) as unique_users_accessing
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND database_name IS NOT NULL
            AND query_text ILIKE '%FROM%'
            GROUP BY database_name, REGEXP_SUBSTR(query_text, 'FROM\\s+([^\\s]+)', 1, 1, 'i', 1)
        )
        SELECT 
            COALESCE(ts.table_id, HASH(ts.database_name || '.' || ts.schema_name || '.' || ts.table_name)) as table_id,
            ts.database_name,
            ts.schema_name,
            ts.table_name,
            ts.storage_gb,
            ts.time_travel_gb,
            ts.failsafe_gb,
            ts.row_count,
            ts.table_comment,
            COALESCE(tqp.query_count, 0) as query_count,
            COALESCE(tqp.full_table_scans_count, 0) as full_table_scans_count,
            COALESCE(tqp.select_star_count, 0) as select_star_count,
            COALESCE(tqp.total_gb_scanned, 0) as total_gb_scanned,
            COALESCE(tqp.avg_execution_time_ms, 0) as avg_execution_time_ms,
            COALESCE(tqp.unique_users_accessing, 0) as unique_users_accessing,
            CASE 
                WHEN ts.storage_gb > 100 AND COALESCE(tqp.query_count, 0) = 0 THEN 'Consider archiving - unused large table'
                WHEN COALESCE(tqp.full_table_scans_count, 0) > 10 THEN 'Add clustering keys or partitioning'
                WHEN ts.time_travel_gb / NULLIF(ts.storage_gb, 0) > 0.5 THEN 'Reduce time travel retention'
                ELSE 'Table optimization looks good'
            END as optimization_recommendation,
            CURRENT_TIMESTAMP() as last_updated
        FROM table_storage ts
        LEFT JOIN table_query_patterns tqp ON ts.database_name = tqp.database_name 
            AND (tqp.table_reference ILIKE '%' || ts.table_name || '%')
        """
        self.execute_query(query)
        logger.info("Created FINOPS_TABLE_METRICS table")
    
    def create_serverless_metrics_table(self):
        """Create serverless services metrics"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_SERVERLESS_METRICS AS
        WITH snowpipe_metrics AS (
            SELECT 
                'SNOWPIPE' as service_type,
                pipe_name as service_name,
                HASH(pipe_name) as service_id,
                COUNT(*) as executions_count,
                SUM(credits_used) as total_credits,
                SUM(files_inserted) as files_processed_count,
                SUM(rows_inserted) as rows_processed,
                AVG(credits_used) as avg_credits_per_execution,
                COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) as error_count,
                ROUND(COUNT(CASE WHEN error_message IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as error_rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
            WHERE last_load_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND pipe_name IS NOT NULL
            GROUP BY pipe_name
            
            UNION ALL
            
            SELECT 
                'TASK' as service_type,
                name as service_name,
                HASH(name) as service_id,
                COUNT(*) as executions_count,
                SUM(credits_used) as total_credits,
                0 as files_processed_count,
                0 as rows_processed,
                AVG(credits_used) as avg_credits_per_execution,
                COUNT(CASE WHEN state = 'FAILED' THEN 1 END) as error_count,
                ROUND(COUNT(CASE WHEN state = 'FAILED' THEN 1 END) * 100.0 / COUNT(*), 2) as error_rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY
            WHERE scheduled_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            GROUP BY name
            
            UNION ALL
            
            SELECT 
                'STREAM' as service_type,
                stream_name as service_name,
                HASH(stream_name) as service_id,
                COUNT(*) as executions_count,
                0 as total_credits,
                0 as files_processed_count,
                SUM(rows_inserted + rows_deleted) as rows_processed,
                0 as avg_credits_per_execution,
                0 as error_count,
                0 as error_rate_pct
            FROM SNOWFLAKE.ACCOUNT_USAGE.STREAMS
            WHERE created >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            GROUP BY stream_name
        ),
        serverless_recommendations AS (
            SELECT 
                service_name,
                service_type,
                CASE 
                    WHEN service_type = 'SNOWPIPE' AND error_rate_pct > 5 THEN 'Review pipe configuration and source data quality'
                    WHEN service_type = 'TASK' AND error_rate_pct > 10 THEN 'Review task logic and dependencies'
                    WHEN service_type = 'SNOWPIPE' AND avg_credits_per_execution > 1 THEN 'Consider batching smaller files'
                    WHEN service_type = 'TASK' AND avg_credits_per_execution > 5 THEN 'Optimize task queries'
                    ELSE 'Service performance looks good'
                END as optimization_recommendation
            FROM snowpipe_metrics
        )
        SELECT 
            sm.*,
            sr.optimization_recommendation,
            CURRENT_TIMESTAMP() as last_updated
        FROM snowpipe_metrics sm
        LEFT JOIN serverless_recommendations sr ON sm.service_name = sr.service_name AND sm.service_type = sr.service_type
        """
        self.execute_query(query)
        logger.info("Created FINOPS_SERVERLESS_METRICS table")
    
    def create_roles_metrics_table(self):
        """Create roles and permissions metrics"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_ROLES_METRICS AS
        WITH role_usage AS (
            SELECT 
                role_name,
                COUNT(DISTINCT user_name) as unique_users,
                COUNT(*) as total_queries,
                SUM(credits_used_cloud_services + credits_used_compute) as total_credits,
                COUNT(DISTINCT warehouse_name) as warehouses_used,
                COUNT(DISTINCT database_name) as databases_accessed,
                AVG(execution_time_ms) as avg_execution_time_ms
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
            AND role_name IS NOT NULL
            GROUP BY role_name
        ),
        role_grants AS (
            SELECT 
                role_name,
                COUNT(*) as total_grants,
                COUNT(CASE WHEN privilege = 'USAGE' THEN 1 END) as usage_grants,
                COUNT(CASE WHEN privilege = 'SELECT' THEN 1 END) as select_grants,
                COUNT(CASE WHEN privilege = 'INSERT' THEN 1 END) as insert_grants,
                COUNT(CASE WHEN privilege = 'DELETE' THEN 1 END) as delete_grants,
                COUNT(CASE WHEN privilege = 'CREATE' THEN 1 END) as create_grants
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES
            WHERE deleted_on IS NULL
            GROUP BY role_name
        ),
        role_recommendations AS (
            SELECT 
                ru.role_name,
                CASE 
                    WHEN ru.unique_users = 0 THEN 'Unused role - consider removal'
                    WHEN ru.total_credits > 1000 AND ru.unique_users = 1 THEN 'High-cost single user role - review necessity'
                    WHEN rg.total_grants > 100 THEN 'Role has many privileges - review for least privilege'
                    ELSE 'Role usage looks appropriate'
                END as security_recommendation
            FROM role_usage ru
            LEFT JOIN role_grants rg ON ru.role_name = rg.role_name
        )
        SELECT 
            HASH(ru.role_name) as role_id,
            ru.role_name,
            ru.unique_users,
            ru.total_queries,
            ru.total_credits,
            ru.warehouses_used,
            ru.databases_accessed,
            ru.avg_execution_time_ms,
            COALESCE(rg.total_grants, 0) as total_grants,
            COALESCE(rg.usage_grants, 0) as usage_grants,
            COALESCE(rg.select_grants, 0) as select_grants,
            COALESCE(rg.insert_grants, 0) as insert_grants,
            COALESCE(rg.delete_grants, 0) as delete_grants,
            COALESCE(rg.create_grants, 0) as create_grants,
            rr.security_recommendation,
            CURRENT_TIMESTAMP() as last_updated
        FROM role_usage ru
        LEFT JOIN role_grants rg ON ru.role_name = rg.role_name
        LEFT JOIN role_recommendations rr ON ru.role_name = rr.role_name
        """
        self.execute_query(query)
        logger.info("Created FINOPS_ROLES_METRICS table")
    
    def create_comprehensive_query_history_table(self):
        """Create comprehensive query history with all drill-down relationships"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_QUERY_HISTORY AS
        SELECT 
            qh.query_id,
            qh.query_text,
            LEFT(qh.query_text, 200) as query_text_preview,
            qh.user_name,
            qh.role_name,
            qh.warehouse_name,
            COALESCE(qh.warehouse_id, HASH(qh.warehouse_name)) as warehouse_id,
            qh.database_name,
            qh.schema_name,
            qh.start_time,
            qh.end_time,
            qh.execution_time_ms,
            qh.compilation_time_ms,
            qh.queue_time_ms,
            qh.execution_status,
            qh.error_code,
            qh.error_message,
            qh.bytes_scanned / (1024*1024*1024) as gb_scanned,
            qh.rows_produced,
            qh.rows_inserted,
            qh.rows_updated,
            qh.rows_deleted,
            qh.credits_used_cloud_services,
            qh.credits_used_compute,
            (qh.credits_used_cloud_services + qh.credits_used_compute) as total_credits_used,
            qh.partitions_scanned,
            qh.partitions_total,
            qh.bytes_spilled_to_local_storage / (1024*1024*1024) as gb_spilled_local,
            qh.bytes_spilled_to_remote_storage / (1024*1024*1024) as gb_spilled_remote,
            
            -- Bad Practice Flags
            CASE WHEN qh.query_text ILIKE '%SELECT *%' AND qh.bytes_scanned > 1073741824 THEN TRUE ELSE FALSE END as is_select_star_large,
            CASE WHEN qh.partitions_scanned > qh.partitions_total * 0.8 AND qh.partitions_total > 10 THEN TRUE ELSE FALSE END as is_unpartitioned_scan,
            CASE WHEN qh.query_text ILIKE '%CROSS JOIN%' OR qh.query_text ILIKE '%CARTESIAN%' THEN TRUE ELSE FALSE END as is_cartesian_join,
            CASE WHEN qh.rows_produced = 0 AND qh.execution_time_ms > 5000 THEN TRUE ELSE FALSE END as is_zero_result_expensive,
            CASE WHEN qh.execution_status IN ('FAIL', 'CANCELLED') THEN TRUE ELSE FALSE END as is_failed,
            CASE WHEN qh.compilation_time_ms > 10000 THEN TRUE ELSE FALSE END as is_high_compile_time,
            CASE WHEN qh.bytes_spilled_to_local_storage > 0 THEN TRUE ELSE FALSE END as is_spilled_local,
            CASE WHEN qh.bytes_spilled_to_remote_storage > 0 THEN TRUE ELSE FALSE END as is_spilled_remote,
            CASE WHEN qh.execution_time_ms > 300000 THEN TRUE ELSE FALSE END as is_long_running,
            CASE WHEN qh.queue_time_ms > 30000 THEN TRUE ELSE FALSE END as is_high_queue_time,
            CASE WHEN qh.query_text NOT ILIKE '%WHERE%' AND qh.query_text ILIKE '%SELECT%' 
                AND qh.bytes_scanned > 1073741824 THEN TRUE ELSE FALSE END as is_missing_where_clause,
            
            -- Performance Categories
            CASE 
                WHEN qh.execution_time_ms BETWEEN 0 AND 1000 THEN '0-1 sec'
                WHEN qh.execution_time_ms BETWEEN 1001 AND 10000 THEN '1-10 sec'
                WHEN qh.execution_time_ms BETWEEN 10001 AND 30000 THEN '10-30 sec'
                WHEN qh.execution_time_ms BETWEEN 30001 AND 60000 THEN '30-60 sec'
                WHEN qh.execution_time_ms BETWEEN 60001 AND 300000 THEN '1-5 min'
                ELSE '5+ min'
            END as performance_bucket,
            
            -- Cost Categories
            CASE 
                WHEN (qh.credits_used_cloud_services + qh.credits_used_compute) = 0 THEN 'Zero Cost'
                WHEN (qh.credits_used_cloud_services + qh.credits_used_compute) <= 0.1 THEN 'Low Cost'
                WHEN (qh.credits_used_cloud_services + qh.credits_used_compute) <= 1 THEN 'Medium Cost'
                ELSE 'High Cost'
            END as cost_category,
            
            -- Time Categories
            CASE 
                WHEN DAYOFWEEK(qh.start_time) IN (1, 7) THEN 'Weekend'
                WHEN HOUR(qh.start_time) BETWEEN 22 AND 6 THEN 'Off Hours'
                ELSE 'Business Hours'
            END as time_category,
            
            -- Query Type Detection
            CASE 
                WHEN qh.query_text ILIKE 'SELECT%' THEN 'SELECT'
                WHEN qh.query_text ILIKE 'INSERT%' THEN 'INSERT'
                WHEN qh.query_text ILIKE 'UPDATE%' THEN 'UPDATE'
                WHEN qh.query_text ILIKE 'DELETE%' THEN 'DELETE'
                WHEN qh.query_text ILIKE 'CREATE%' THEN 'CREATE'
                WHEN qh.query_text ILIKE 'DROP%' THEN 'DROP'
                WHEN qh.query_text ILIKE 'ALTER%' THEN 'ALTER'
                ELSE 'OTHER'
            END as query_type,
            
            -- Foreign Key References for Drill-Down
            HASH(qh.user_name || qh.warehouse_name) as user_warehouse_id,
            COALESCE(db.database_id, HASH(qh.database_name)) as database_id,
            HASH(qh.role_name) as role_id,
            
            CURRENT_TIMESTAMP() as last_updated
            
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        LEFT JOIN (SELECT DISTINCT database_name, database_id FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASES) db 
            ON qh.database_name = db.database_name
        WHERE qh.start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
        """
        self.execute_query(query)
        logger.info("Created FINOPS_QUERY_HISTORY table")
    
    def create_query_details_table(self):
        """Create detailed query analysis with recommendations"""
        query = f"""
        CREATE OR REPLACE TABLE FINOPS_QUERY_DETAILS AS
        WITH query_analysis AS (
            SELECT 
                qh.query_id,
                qh.query_text,
                qh.execution_time_ms,
                qh.compilation_time_ms,
                qh.bytes_scanned,
                qh.rows_produced,
                qh.credits_used_cloud_services + qh.credits_used_compute as total_credits,
                qh.partitions_scanned,
                qh.partitions_total,
                qh.bytes_spilled_to_local_storage,
                qh.bytes_spilled_to_remote_storage,
                
                -- Cost Analysis
                (qh.credits_used_cloud_services + qh.credits_used_compute) * 3.0 as estimated_cost_usd,
                CASE WHEN qh.bytes_scanned > 0 THEN 
                    (qh.credits_used_cloud_services + qh.credits_used_compute) / (qh.bytes_scanned / (1024*1024*1024))
                    ELSE 0 END as cost_per_gb_scanned,
                
                -- Efficiency Metrics
                CASE WHEN qh.execution_time_ms > 0 THEN 
                    qh.rows_produced / (qh.execution_time_ms / 1000.0) 
                    ELSE 0 END as rows_per_second,
                CASE WHEN qh.bytes_scanned > 0 THEN 
                    qh.rows_produced / (qh.bytes_scanned / (1024*1024*1024))
                    ELSE 0 END as rows_per_gb_scanned,
                
                -- Problem Detection
                CASE WHEN qh.partitions_total > 0 THEN 
                    qh.partitions_scanned / qh.partitions_total * 100 
                    ELSE 0 END as partition_scan_percentage
                    
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
            WHERE qh.start_time >= DATEADD('day', -{self.days_filter}, CURRENT_TIMESTAMP())
        ),
        query_recommendations AS (
            SELECT 
                query_id,
                ARRAY_CONSTRUCT(
                    CASE WHEN partition_scan_percentage > 80 AND partitions_total > 10 
                        THEN 'Add WHERE clause to filter partitions' END,
                    CASE WHEN query_text ILIKE '%SELECT *%' AND bytes_scanned > 1073741824 
                        THEN 'Replace SELECT * with specific columns' END,
                    CASE WHEN bytes_spilled_to_local_storage > 0 
                        THEN 'Increase warehouse size to reduce spilling' END,
                    CASE WHEN bytes_spilled_to_remote_storage > 0 
                        THEN 'Significantly increase warehouse size - remote spilling detected' END,
                    CASE WHEN compilation_time_ms > 10000 
                        THEN 'Query compilation is slow - consider simplifying' END,
                    CASE WHEN rows_produced = 0 AND execution_time_ms > 5000 
                        THEN 'Query returns no results but takes time - check logic' END,
                    CASE WHEN cost_per_gb_scanned > 1 
                        THEN 'High cost per GB scanned - optimize data access patterns' END,
                    CASE WHEN query_text ILIKE '%CROSS JOIN%' 
                        THEN 'Cartesian join detected - add proper join conditions' END,
                    CASE WHEN query_text NOT ILIKE '%WHERE%' AND query_text ILIKE '%SELECT%' AND bytes_scanned > 1073741824 
                        THEN 'Large scan without WHERE clause - add filters' END
                ) as optimization_recommendations,
                
                CASE 
                    WHEN total_credits > 10 THEN 'CRITICAL'
                    WHEN total_credits > 1 THEN 'HIGH'
                    WHEN total_credits > 0.1 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as cost_impact,
                
                CASE 
                    WHEN execution_time_ms > 300000 THEN 'CRITICAL'
                    WHEN execution_time_ms > 60000 THEN 'HIGH'
                    WHEN execution_time_ms > 10000 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as performance_impact
                
            FROM query_analysis
        )
        SELECT 
            qa.*,
            qr.optimization_recommendations,
            qr.cost_impact,
            qr.performance_impact,
            CURRENT_TIMESTAMP() as last_updated
        FROM query_analysis qa
        LEFT JOIN query_recommendations qr ON qa.query_id = qr.query_id
        """
        self.execute_query(query)
        logger.info("Created FINOPS_QUERY_DETAILS table")
    
    def create_all_tables(self):
        """Create all FinOps tables"""
        logger.info(f"Creating all FinOps tables with {self.days_filter} days filter")
        
        self.create_warehouse_metrics_table()
        self.create_user_warehouse_usage_table()
        self.create_database_metrics_table()
        self.create_table_metrics_table()
        self.create_serverless_metrics_table()
        self.create_roles_metrics_table()
        self.create_comprehensive_query_history_table()
        self.create_query_details_table()
        
        logger.info("All FinOps tables created successfully")
    
    def get_table_data(self, table_name: str, filters: Dict = None, limit: int = 1000) -> pd.DataFrame:
        """Get data from any FinOps table with optional filtering"""
        query = f"SELECT * FROM {table_name}"
        
        if filters:
            conditions = []
            for key, value in filters.items():
                if isinstance(value, str):
                    conditions.append(f"{key} = '{value}'")
                else:
                    conditions.append(f"{key} = {value}")
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
        
        query += f" LIMIT {limit}"
        
        return self.execute_query(query)

# Initialize Flask app with FinOps analytics
finops = None

def initialize_finops(snowflake_cursor, days_filter=30):
    """Initialize FinOps analytics with Snowflake cursor"""
    global finops
    finops = FinOpsAnalytics(snowflake_cursor)
    finops.set_time_filter(days_filter)
    return finops

# API Routes
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0.0'
    })

@app.route('/api/initialize', methods=['POST'])
def initialize_tables():
    """Initialize all FinOps tables"""
    try:
        days_filter = request.json.get('days_filter', 30)
        finops.set_time_filter(days_filter)
        finops.create_all_tables()
        
        return jsonify({
            'status': 'success',
            'message': f'All FinOps tables created with {days_filter} days filter',
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/warehouses', methods=['GET'])
def get_warehouses():
    """Get warehouse metrics with drill-down support"""
    try:
        df = finops.get_table_data('FINOPS_WAREHOUSE_METRICS')
        
        if request.args.get('export') == 'csv':
            return export_to_csv(df, 'warehouse_metrics')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/users', methods=['GET'])
def get_users():
    """Get user metrics with filtering support"""
    try:
        filters = {}
        if request.args.get('warehouse_name'):
            filters['warehouse_name'] = request.args.get('warehouse_name')
        if request.args.get('cost_category'):
            filters['cost_category'] = request.args.get('cost_category')
        
        df = finops.get_table_data('FINOPS_USER_WAREHOUSE_USAGE', filters)
        
        if request.args.get('export') == 'csv':
            return export_to_csv(df, 'user_metrics')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """Get database metrics"""
    try:
        df = finops.get_table_data('FINOPS_DATABASE_METRICS')
        
        if request.args.get('export') == 'csv':
            return export_to_csv(df, 'database_metrics')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tables', methods=['GET'])
def get_tables():
    """Get table metrics with filtering"""
    try:
        filters = {}
        if request.args.get('database_name'):
            filters['database_name'] = request.args.get('database_name')
        
        df = finops.get_table_data('FINOPS_TABLE_METRICS', filters)
        
        if request.args.get('export') == 'csv':
            return export_to_csv(df, 'table_metrics')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500
import pandas as pd
import os

def export_to_csv(df: pd.DataFrame, filename: str, output_dir: str = "./exports") -> str:
    """
    Exports a DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): The DataFrame to export.
        filename (str): The name of the CSV file (without .csv extension).
        output_dir (str): Directory to save the file (default is './exports').

    Returns:
        str: The full path to the saved CSV file.
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{filename}.csv")
    df.to_csv(file_path, index=False)
    print(f"[✓] Exported to {file_path}")
    return file_path

@app.route('/api/serverless', methods=['GET'])
def get_serverless():
    """Get serverless metrics"""
    try:
        filters = {}
        if request.args.get('service_type'):
            filters['service_type'] = request.args.get('service_type')
        
        df = finops.get_table_data('FINOPS_SERVERLESS_METRICS', filters)
        
        if request.args.get('export') == 'csv':
            return export_to_csv(df, 'serverless_metrics')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/roles', methods=['GET'])
def get_roles():
    """Get roles metrics"""
    try:
        df = finops.get_table_data('FINOPS_ROLES_METRICS')
        
        if request.args.get('export') == 'csv':
            return export_to_csv(df, 'roles_metrics')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/queries', methods=['GET'])
def get_queries():
    """Get query history with comprehensive filtering"""
    try:
        filters = {}
        
        # Add all possible filters
        filter_params = [
            'user_name', 'warehouse_name', 'database_name', 'role_name',
            'query_type', 'performance_bucket', 'cost_category', 'time_category'
        ]
        
        for param in filter_params:
            if request.args.get(param):
                filters[param] = request.args.get(param)
        
        # Add boolean filters for bad practices
        boolean_filters = [
            'is_select_star_large', 'is_unpartitioned_scan', 'is_cartesian_join',
            'is_zero_result_expensive', 'is_failed', 'is_high_compile_time',
            'is_spilled_local', 'is_spilled_remote', 'is_long_running'
        ]
        
        for param in boolean_filters:
            if request.args.get(param) == 'true':
                filters[param] = True
        
        limit = int(request.args.get('limit', 1000))
        df = finops.get_table_data('FINOPS_QUERY_HISTORY', filters, limit)
        
        if request.args.get('export') == 'csv':
            return export_to_csv(df, 'query_history')
        
        return jsonify(df.to_dict('records'))
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/query-details/<query_id>', methods=['GET'])
def get_query_details(query_id: str):
    """Get detailed query analysis"""
    try:
        filters = {'query_id': query_id}
        df = finops.get_table_data('FINOPS_QUERY_DETAILS', filters, limit=1)
        
        if df.empty:
            return jsonify({'error': 'Query not found'}), 404
            
        if request.args.get('export') == 'csv':
            return export_to_csv(df, f'query_details_{query_id}')
        
        return jsonify(df.to_dict('records')[0])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/summary', methods=['GET'])
def get_summary():
    """Get high-level summary metrics"""
    try:
        warehouse_df = finops.get_table_data('FINOPS_WAREHOUSE_METRICS', limit=1000)
        user_df = finops.get_table_data('FINOPS_USER_WAREHOUSE_USAGE', limit=1000)
        db_df = finops.get_table_data('FINOPS_DATABASE_METRICS', limit=1000)
        serverless_df = finops.get_table_data('FINOPS_SERVERLESS_METRICS', limit=1000)
        
        summary = {
            'total_warehouses': len(warehouse_df),
            'total_credits_used': float(warehouse_df['total_credits'].sum()),
            'active_users': len(user_df['user_name'].unique()),
            'databases_count': len(db_df),
            'serverless_services_count': len(serverless_df),
            'average_credits_per_user': float(user_df['total_credits'].sum() / len(user_df['user_name'].unique())) if len(user_df['user_name'].unique()) > 0 else 0,
            'top_warehouse': warehouse_df.loc[warehouse_df['total_credits'].idxmax()]['warehouse_name'] if not warehouse_df.empty else None,
            'highest_cost_user': user_df.loc[user_df['total_credits'].idxmax()]['user_name'] if not user_df.empty else None,
            'largest_database': db_df.loc[db_df['total_storage_gb'].idxmax()]['database_name'] if not db_df.empty else None,
            'timestamp': datetime.now().isoformat()
        }
        
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def export_to_csv_response(df: pd.DataFrame, filename: str):
    """Helper function to send CSV file response"""
    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)
    
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'{filename}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    )

if __name__ == '__main__':
    # Note: Snowflake connection should be initialized before running
    # Example: finops = initialize_finops(snowflake_cursor)
    app.run(debug=True, host='0.0.0.0', port=5000)