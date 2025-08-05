from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import os
from dataclasses import dataclass
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv('SNOWFLAKE_USER', 'your_user'),
    'password': os.getenv('SNOWFLAKE_PASSWORD', 'your_password'),
    'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your_account'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
    'database': os.getenv('SNOWFLAKE_DATABASE', 'SNOWFLAKE'),
    'schema': os.getenv('SNOWFLAKE_SCHEMA', 'ACCOUNT_USAGE'),
    'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
}

@dataclass
class QueryConfig:
    name: str
    sql: str
    description: str

class SnowflakeConnector:
    def __init__(self, config: Dict[str, str]):
        self.config = config
        self.connection = None
    
    def connect(self):
        try:
            self.connection = snowflake.connector.connect(**self.config)
            logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def execute_query(self, query: str) -> pd.DataFrame:
        if not self.connection:
            if not self.connect():
                raise Exception("Cannot establish Snowflake connection")
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            
            # Fetch column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            cursor.close()
            return df
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise e
    
    def close(self):
        if self.connection:
            self.connection.close()

# Initialize Snowflake connector
sf_connector = SnowflakeConnector(SNOWFLAKE_CONFIG)

# Query Definitions
QUERIES = {
    'warehouses': QueryConfig(
        name='Warehouse Analytics',
        sql="""
        WITH warehouse_metrics AS (
            SELECT 
                wm.WAREHOUSE_NAME,
                wm.WAREHOUSE_ID,
                wh.WAREHOUSE_SIZE,
                wh.AUTO_SUSPEND,
                wh.AUTO_RESUME,
                wh.MIN_CLUSTER_COUNT,
                wh.MAX_CLUSTER_COUNT,
                wh.SCALING_POLICY,
                wh.RESOURCE_MONITOR,
                -- Credit Usage (Last 3 Days)
                ROUND(SUM(wm.CREDITS_USED), 2) as TOTAL_CREDITS_USED,
                ROUND(SUM(wm.CREDITS_USED_COMPUTE), 2) as CREDITS_USED_COMPUTE,
                ROUND(SUM(wm.CREDITS_USED_CLOUD_SERVICES), 2) as CREDITS_USED_CLOUD_SERVICES,
                
                -- Query Performance Buckets
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME <= 1000 THEN 1 END) as QUERIES_0_TO_1_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 1000 AND qh.TOTAL_ELAPSED_TIME <= 10000 THEN 1 END) as QUERIES_1_TO_10_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 10000 AND qh.TOTAL_ELAPSED_TIME <= 30000 THEN 1 END) as QUERIES_10_TO_30_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 30000 AND qh.TOTAL_ELAPSED_TIME <= 60000 THEN 1 END) as QUERIES_30_TO_60_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 60000 AND qh.TOTAL_ELAPSED_TIME <= 300000 THEN 1 END) as QUERIES_1_TO_5_MIN,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 300000 AND qh.TOTAL_ELAPSED_TIME <= 900000 THEN 1 END) as QUERIES_5_TO_15_MIN,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 900000 THEN 1 END) as QUERIES_15_MIN_PLUS,
                
                -- Bad Practice Categories
                COUNT(CASE WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 1 END) as SPILLED_QUERIES,
                COUNT(CASE WHEN qh.EXECUTION_STATUS = 'FAIL' THEN 1 END) as FAILED_QUERIES,
                COUNT(CASE WHEN qh.ROWS_PRODUCED = 0 AND qh.EXECUTION_STATUS = 'SUCCESS' THEN 1 END) as ZERO_RESULT_QUERIES,
                COUNT(CASE WHEN qh.COMPILATION_TIME > 5000 THEN 1 END) as HIGH_COMPILE_TIME,
                COUNT(CASE WHEN qh.TRANSACTION_BLOCKED_TIME > 0 THEN 1 END) as BLOCKED_TRANSACTION_QUERIES,
                
                -- Utilization Metrics
                ROUND(AVG(CASE WHEN wl.AVG_RUNNING > 0 THEN (wl.AVG_RUNNING / GREATEST(wh.MAX_CLUSTER_COUNT, 1)) * 100 ELSE 0 END), 2) as AVG_UTILIZATION_PERCENT,
                
                -- Core Metrics
                COUNT(qh.QUERY_ID) as TOTAL_QUERIES,
                COUNT(DISTINCT qh.DATABASE_NAME) as UNIQUE_DATABASES_ACCESSED,
                COUNT(DISTINCT qh.USER_NAME) as UNIQUE_USERS,
                COUNT(DISTINCT DATE(qh.START_TIME)) as ACTIVE_DAYS_COUNT,
                ROUND(AVG(qh.TOTAL_ELAPSED_TIME), 2) as AVG_EXECUTION_TIME_MS,
                ROUND(SUM(qh.BYTES_SCANNED) / (1024*1024*1024), 2) as TOTAL_DATA_SCANNED_GB
                
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY wm
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES wh ON wm.WAREHOUSE_ID = wh.WAREHOUSE_ID
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON wm.WAREHOUSE_ID = qh.WAREHOUSE_ID 
                AND qh.START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY wl ON wm.WAREHOUSE_ID = wl.WAREHOUSE_ID
                AND wl.START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
            WHERE wm.START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
                AND wh.DELETED IS NULL
            GROUP BY 1,2,3,4,5,6,7,8,9
            ORDER BY TOTAL_CREDITS_USED DESC
        """,
        description='Comprehensive warehouse analytics with performance metrics and bad practices'
    ),
    
    'users': QueryConfig(
        name='User Analytics',
        sql="""
        WITH user_metrics AS (
            SELECT 
                u.NAME as USER_NAME,
                u.USER_ID,
                u.LOGIN_NAME,
                u.EMAIL,
                u.DEFAULT_WAREHOUSE,
                u.DEFAULT_ROLE,
                u.DISABLED,
                u.LAST_SUCCESS_LOGIN,
                u.TYPE as USER_TYPE,
                
                -- Credit Usage (Last 3 Days) - Aggregated from Query Level
                ROUND(SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)), 2) as TOTAL_CREDITS_CONSUMED,
                
                -- Query Performance Buckets
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME <= 1000 THEN 1 END) as QUERIES_0_TO_1_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 1000 AND qh.TOTAL_ELAPSED_TIME <= 10000 THEN 1 END) as QUERIES_1_TO_10_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 10000 AND qh.TOTAL_ELAPSED_TIME <= 30000 THEN 1 END) as QUERIES_10_TO_30_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 30000 AND qh.TOTAL_ELAPSED_TIME <= 60000 THEN 1 END) as QUERIES_30_TO_60_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 60000 AND qh.TOTAL_ELAPSED_TIME <= 300000 THEN 1 END) as QUERIES_1_TO_5_MIN,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 300000 AND qh.TOTAL_ELAPSED_TIME <= 900000 THEN 1 END) as QUERIES_5_TO_15_MIN,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 900000 THEN 1 END) as QUERIES_15_MIN_PLUS,
                
                -- Bad Practice Categories
                COUNT(CASE WHEN UPPER(qh.QUERY_TEXT) LIKE '%SELECT *%' THEN 1 END) as SELECT_STAR_QUERIES,
                COUNT(CASE WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 1 END) as SPILLED_QUERIES,
                COUNT(CASE WHEN qh.EXECUTION_STATUS = 'FAIL' THEN 1 END) as FAILED_QUERIES,
                COUNT(CASE WHEN qh.ROWS_PRODUCED = 0 AND qh.EXECUTION_STATUS = 'SUCCESS' THEN 1 END) as ZERO_RESULT_QUERIES,
                COUNT(CASE WHEN qh.COMPILATION_TIME > 5000 THEN 1 END) as HIGH_COMPILE_TIME,
                COUNT(CASE WHEN qh.PARTITIONS_SCANNED = qh.PARTITIONS_TOTAL AND qh.PARTITIONS_TOTAL > 100 THEN 1 END) as UNPARTITIONED_SCANS,
                COUNT(CASE WHEN qh.TRANSACTION_BLOCKED_TIME > 0 THEN 1 END) as BLOCKED_TRANSACTION_QUERIES,
                
                -- Core Metrics
                COUNT(qh.QUERY_ID) as TOTAL_QUERIES,
                COUNT(DISTINCT qh.WAREHOUSE_NAME) as UNIQUE_WAREHOUSES_USED,
                COUNT(DISTINCT qh.DATABASE_NAME) as UNIQUE_DATABASES_ACCESSED,
                COUNT(DISTINCT DATE(qh.START_TIME)) as ACTIVE_DAYS_COUNT,
                ROUND(AVG(qh.TOTAL_ELAPSED_TIME), 2) as AVG_EXECUTION_TIME_MS,
                ROUND(SUM(qh.BYTES_SCANNED) / (1024*1024*1024), 2) as TOTAL_DATA_SCANNED_GB,
                
                -- Repeated Queries Detection
                COUNT(CASE WHEN repeat_check.QUERY_COUNT > 1 THEN 1 END) as REPEATED_QUERIES
                
            FROM SNOWFLAKE.ACCOUNT_USAGE.USERS u
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON u.NAME = qh.USER_NAME 
                AND qh.START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
            LEFT JOIN (
                SELECT QUERY_HASH, USER_NAME, COUNT(*) as QUERY_COUNT
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
                    AND QUERY_HASH IS NOT NULL
                GROUP BY QUERY_HASH, USER_NAME
            ) repeat_check ON qh.QUERY_HASH = repeat_check.QUERY_HASH AND qh.USER_NAME = repeat_check.USER_NAME
            WHERE u.DELETED_ON IS NULL
            GROUP BY 1,2,3,4,5,6,7,8,9
            ORDER BY TOTAL_QUERIES DESC
        """,
        description='User analytics with bad practices and performance metrics'
    ),
    
    'queries': QueryConfig(
        name='Query Analytics',
        sql="""
        SELECT 
            qh.QUERY_ID,
            qh.QUERY_TEXT,
            qh.USER_NAME,
            qh.ROLE_NAME,
            qh.WAREHOUSE_NAME,
            qh.WAREHOUSE_SIZE,
            qh.DATABASE_NAME,
            qh.SCHEMA_NAME,
            qh.QUERY_TYPE,
            qh.EXECUTION_STATUS,
            qh.ERROR_CODE,
            qh.ERROR_MESSAGE,
            qh.START_TIME,
            qh.END_TIME,
            qh.TOTAL_ELAPSED_TIME,
            qh.COMPILATION_TIME,
            qh.EXECUTION_TIME,
            qh.QUEUED_PROVISIONING_TIME,
            qh.QUEUED_REPAIR_TIME,
            qh.QUEUED_OVERLOAD_TIME,
            qh.TRANSACTION_BLOCKED_TIME,
            
            -- Performance Metrics
            ROUND(qh.BYTES_SCANNED / (1024*1024*1024), 4) as BYTES_SCANNED_GB,
            qh.PERCENTAGE_SCANNED_FROM_CACHE,
            ROUND(qh.BYTES_WRITTEN / (1024*1024*1024), 4) as BYTES_WRITTEN_GB,
            qh.ROWS_PRODUCED,
            qh.ROWS_INSERTED,
            qh.ROWS_UPDATED,
            qh.ROWS_DELETED,
            qh.PARTITIONS_SCANNED,
            qh.PARTITIONS_TOTAL,
            
            -- Spill Detection
            ROUND(qh.BYTES_SPILLED_TO_LOCAL_STORAGE / (1024*1024*1024), 4) as BYTES_SPILLED_LOCAL_GB,
            ROUND(qh.BYTES_SPILLED_TO_REMOTE_STORAGE / (1024*1024*1024), 4) as BYTES_SPILLED_REMOTE_GB,
            
            -- Credits
            ROUND(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0), 4) as CREDITS_USED_CLOUD_SERVICES,
            
            -- Performance Categories
            CASE 
                WHEN qh.TOTAL_ELAPSED_TIME <= 1000 THEN '0-1 SEC'
                WHEN qh.TOTAL_ELAPSED_TIME <= 10000 THEN '1-10 SEC'
                WHEN qh.TOTAL_ELAPSED_TIME <= 30000 THEN '10-30 SEC'
                WHEN qh.TOTAL_ELAPSED_TIME <= 60000 THEN '30-60 SEC'
                WHEN qh.TOTAL_ELAPSED_TIME <= 300000 THEN '1-5 MIN'
                WHEN qh.TOTAL_ELAPSED_TIME <= 900000 THEN '5-15 MIN'
                ELSE '15+ MIN'
            END as PERFORMANCE_BUCKET,
            
            -- Bad Practice Flags
            CASE WHEN UPPER(qh.QUERY_TEXT) LIKE '%SELECT *%' THEN 1 ELSE 0 END as IS_SELECT_STAR,
            CASE WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 1 ELSE 0 END as HAS_SPILL,
            CASE WHEN qh.PARTITIONS_SCANNED = qh.PARTITIONS_TOTAL AND qh.PARTITIONS_TOTAL > 100 THEN 1 ELSE 0 END as IS_FULL_TABLE_SCAN,
            CASE WHEN qh.COMPILATION_TIME > 5000 THEN 1 ELSE 0 END as HAS_HIGH_COMPILE_TIME,
            CASE WHEN qh.ROWS_PRODUCED = 0 AND qh.EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END as IS_ZERO_RESULT,
            
            qh.QUERY_HASH,
            qh.QUERY_HASH_VERSION
            
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
        WHERE qh.START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
        ORDER BY qh.START_TIME DESC
        LIMIT 10000
        """,
        description='Detailed query analytics with performance categorization'
    ),
    
    'query_details': QueryConfig(
        name='Query Detail Analytics',
        sql="""
        WITH query_performance AS (
            SELECT 
                qh.QUERY_ID,
                qh.QUERY_TEXT,
                qh.USER_NAME,
                qh.WAREHOUSE_NAME,
                qh.DATABASE_NAME,
                qh.SCHEMA_NAME,
                qh.START_TIME,
                qh.END_TIME,
                qh.TOTAL_ELAPSED_TIME,
                qh.EXECUTION_STATUS,
                
                -- Detailed Performance Breakdown
                qh.COMPILATION_TIME,
                qh.EXECUTION_TIME,
                qh.QUEUED_PROVISIONING_TIME,
                qh.QUEUED_REPAIR_TIME,
                qh.QUEUED_OVERLOAD_TIME,
                qh.TRANSACTION_BLOCKED_TIME,
                qh.LIST_EXTERNAL_FILES_TIME,
                
                -- Data Movement
                ROUND(qh.BYTES_SCANNED / (1024*1024*1024), 4) as BYTES_SCANNED_GB,
                ROUND(qh.BYTES_WRITTEN / (1024*1024*1024), 4) as BYTES_WRITTEN_GB,
                ROUND(qh.BYTES_SPILLED_TO_LOCAL_STORAGE / (1024*1024*1024), 4) as BYTES_SPILLED_LOCAL_GB,
                ROUND(qh.BYTES_SPILLED_TO_REMOTE_STORAGE / (1024*1024*1024), 4) as BYTES_SPILLED_REMOTE_GB,
                ROUND(qh.BYTES_SENT_OVER_THE_NETWORK / (1024*1024*1024), 4) as BYTES_NETWORK_GB,
                
                -- Row Operations
                qh.ROWS_PRODUCED,
                qh.ROWS_INSERTED,
                qh.ROWS_UPDATED,
                qh.ROWS_DELETED,
                qh.ROWS_UNLOADED,
                
                -- Partition Information
                qh.PARTITIONS_SCANNED,
                qh.PARTITIONS_TOTAL,
                CASE 
                    WHEN qh.PARTITIONS_TOTAL > 0 
                    THEN ROUND((qh.PARTITIONS_SCANNED::FLOAT / qh.PARTITIONS_TOTAL::FLOAT) * 100, 2)
                    ELSE 0 
                END as PARTITION_SCAN_PERCENTAGE,
                
                -- Cache Performance
                qh.PERCENTAGE_SCANNED_FROM_CACHE,
                
                -- Credits and Cost
                ROUND(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0), 6) as CREDITS_USED_CLOUD_SERVICES,
                
                -- Query Categorization
                qh.QUERY_TYPE,
                qh.QUERY_TAG,
                
                -- Error Information
                qh.ERROR_CODE,
                qh.ERROR_MESSAGE,
                
                -- Optimization Indicators
                CASE 
                    WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 'MEMORY_SPILL'
                    WHEN qh.PARTITIONS_SCANNED = qh.PARTITIONS_TOTAL AND qh.PARTITIONS_TOTAL > 100 THEN 'FULL_TABLE_SCAN'
                    WHEN qh.COMPILATION_TIME > 5000 THEN 'HIGH_COMPILE_TIME'
                    WHEN qh.PERCENTAGE_SCANNED_FROM_CACHE < 10 AND qh.BYTES_SCANNED > 1073741824 THEN 'POOR_CACHE_USAGE'
                    WHEN qh.TRANSACTION_BLOCKED_TIME > 10000 THEN 'TRANSACTION_BLOCKED'
                    ELSE 'NORMAL'
                END as OPTIMIZATION_FLAG,
                
                -- Access Pattern
                ah.DIRECT_OBJECTS_ACCESSED,
                ah.BASE_OBJECTS_ACCESSED,
                ah.OBJECTS_MODIFIED
                
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah ON qh.QUERY_ID = ah.QUERY_ID
            WHERE qh.START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
        )
        SELECT * FROM query_performance
        ORDER BY START_TIME DESC
        LIMIT 5000
        """,
        description='Deep dive query performance analysis with optimization recommendations'
    ),
    
    'databases': QueryConfig(
        name='Database Analytics',
        sql="""
        WITH database_metrics AS (
            SELECT 
                qh.DATABASE_NAME,
                qh.DATABASE_ID,
                
                -- Query Activity
                COUNT(qh.QUERY_ID) as TOTAL_QUERIES,
                COUNT(DISTINCT qh.USER_NAME) as UNIQUE_USERS,
                COUNT(DISTINCT qh.WAREHOUSE_NAME) as UNIQUE_WAREHOUSES_USED,
                COUNT(DISTINCT qh.SCHEMA_NAME) as UNIQUE_SCHEMAS_ACCESSED,
                COUNT(DISTINCT DATE(qh.START_TIME)) as ACTIVE_DAYS_COUNT,
                
                -- Performance Metrics
                ROUND(AVG(qh.TOTAL_ELAPSED_TIME), 2) as AVG_EXECUTION_TIME_MS,
                ROUND(SUM(qh.BYTES_SCANNED) / (1024*1024*1024), 2) as TOTAL_DATA_SCANNED_GB,
                ROUND(SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)), 4) as TOTAL_CREDITS_CONSUMED,
                
                -- Query Type Distribution
                COUNT(CASE WHEN qh.QUERY_TYPE = 'SELECT' THEN 1 END) as SELECT_QUERIES,
                COUNT(CASE WHEN qh.QUERY_TYPE = 'INSERT' THEN 1 END) as INSERT_QUERIES,
                COUNT(CASE WHEN qh.QUERY_TYPE = 'UPDATE' THEN 1 END) as UPDATE_QUERIES,
                COUNT(CASE WHEN qh.QUERY_TYPE = 'DELETE' THEN 1 END) as DELETE_QUERIES,
                COUNT(CASE WHEN qh.QUERY_TYPE = 'CREATE' THEN 1 END) as CREATE_QUERIES,
                COUNT(CASE WHEN qh.QUERY_TYPE = 'DROP' THEN 1 END) as DROP_QUERIES,
                
                -- Performance Buckets
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME <= 1000 THEN 1 END) as QUERIES_0_TO_1_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 1000 AND qh.TOTAL_ELAPSED_TIME <= 10000 THEN 1 END) as QUERIES_1_TO_10_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 10000 AND qh.TOTAL_ELAPSED_TIME <= 30000 THEN 1 END) as QUERIES_10_TO_30_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 30000 AND qh.TOTAL_ELAPSED_TIME <= 60000 THEN 1 END) as QUERIES_30_TO_60_SEC,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 60000 AND qh.TOTAL_ELAPSED_TIME <= 300000 THEN 1 END) as QUERIES_1_TO_5_MIN,
                COUNT(CASE WHEN qh.TOTAL_ELAPSED_TIME > 300000 THEN 1 END) as QUERIES_5_MIN_PLUS,
                
                -- Bad Practices
                COUNT(CASE WHEN UPPER(qh.QUERY_TEXT) LIKE '%SELECT *%' THEN 1 END) as SELECT_STAR_QUERIES,
                COUNT(CASE WHEN qh.BYTES_SPILLED_TO_LOCAL_STORAGE > 0 OR qh.BYTES_SPILLED_TO_REMOTE_STORAGE > 0 THEN 1 END) as SPILLED_QUERIES,
                COUNT(CASE WHEN qh.EXECUTION_STATUS = 'FAIL' THEN 1 END) as FAILED_QUERIES,
                COUNT(CASE WHEN qh.ROWS_PRODUCED = 0 AND qh.EXECUTION_STATUS = 'SUCCESS' THEN 1 END) as ZERO_RESULT_QUERIES,
                COUNT(CASE WHEN qh.PARTITIONS_SCANNED = qh.PARTITIONS_TOTAL AND qh.PARTITIONS_TOTAL > 100 THEN 1 END) as UNPARTITIONED_SCANS
                
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
            WHERE qh.START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
                AND qh.DATABASE_NAME IS NOT NULL
            GROUP BY qh.DATABASE_NAME, qh.DATABASE_ID
        ),
        database_storage AS (
            SELECT 
                ds.DATABASE_NAME,
                ds.DATABASE_ID,
                ROUND(AVG(ds.AVERAGE_DATABASE_BYTES) / (1024*1024*1024), 2) as AVG_STORAGE_GB,
                ROUND(AVG(ds.AVERAGE_FAILSAFE_BYTES) / (1024*1024*1024), 2) as AVG_FAILSAFE_GB
            FROM SNOWFLAKE.ACCOUNT_USAGE.DATABASE_STORAGE_USAGE_HISTORY ds
            WHERE ds.USAGE_DATE >= DATEADD(day, -3, CURRENT_DATE())
            GROUP BY ds.DATABASE_NAME, ds.DATABASE_ID
        )
        SELECT 
            dm.*,
            COALESCE(ds.AVG_STORAGE_GB, 0) as AVG_STORAGE_GB,
            COALESCE(ds.AVG_FAILSAFE_GB, 0) as AVG_FAILSAFE_GB
        FROM database_metrics dm
        LEFT JOIN database_storage ds ON dm.DATABASE_NAME = ds.DATABASE_NAME
        ORDER BY dm.TOTAL_QUERIES DESC
        """,
        description='Database-level analytics with storage and query patterns'
    ),
    
    'tables': QueryConfig(
        name='Table Analytics',
        sql="""
        WITH table_metrics AS (
            SELECT 
                t.TABLE_NAME,
                t.TABLE_SCHEMA,
                t.TABLE_CATALOG as DATABASE_NAME,
                t.TABLE_TYPE,
                t.IS_TRANSIENT,
                t.CLUSTERING_KEY,
                t.ROW_COUNT,
                ROUND(t.BYTES / (1024*1024*1024), 4) as SIZE_GB,
                t.RETENTION_TIME,
                t.AUTO_CLUSTERING_ON,
                t.CREATED,
                t.LAST_ALTERED,
                t.LAST_DDL,
                
                -- Query Activity (from ACCESS_HISTORY)
                COUNT(DISTINCT ah.QUERY_ID) as QUERIES_ACCESSING_TABLE,
                COUNT(DISTINCT ah.USER_NAME) as UNIQUE_USERS_ACCESSING,
                
                -- Access Patterns
                COUNT(CASE WHEN ah.DIRECT_OBJECTS_ACCESSED IS NOT NULL THEN 1 END) as DIRECT_ACCESS_COUNT,
                COUNT(CASE WHEN ah.BASE_OBJECTS_ACCESSED IS NOT NULL THEN 1 END) as BASE_ACCESS_COUNT,
                COUNT(CASE WHEN ah.OBJECTS_MODIFIED IS NOT NULL THEN 1 END) as MODIFICATION_COUNT,
                
                -- Performance Impact
                AVG(qh.TOTAL_ELAPSED_TIME) as AVG_QUERY_TIME_MS,
                SUM(qh.BYTES_SCANNED) / (1024*1024*1024) as TOTAL_SCANNED_GB,
                AVG(qh.PERCENTAGE_SCANNED_FROM_CACHE) as AVG_CACHE_HIT_RATE
                
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah ON 
                UPPER(ah.BASE_OBJECTS_ACCESSED) LIKE '%' || UPPER(t.TABLE_CATALOG) || '.' || UPPER(t.TABLE_SCHEMA) || '.' || UPPER(t.TABLE_NAME) || '%'
                AND ah.QUERY_START_TIME >= DATEADD(day, -3, CURRENT_TIMESTAMP())
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON ah.QUERY_ID = qh.QUERY_ID
            WHERE t.DELETED IS NULL
                AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
        )
        SELECT 
            tm.*,
            -- Utilization Categories
            CASE 
                WHEN tm.QUERIES_ACCESSING_TABLE = 0 THEN 'UNUSED'
                WHEN tm.QUERIES_ACCESSING_TABLE <= 10 THEN 'LOW_USAGE'
                WHEN tm.QUERIES_ACCESSING_TABLE <= 100 THEN 'MEDIUM_USAGE'
                ELSE 'HIGH_USAGE'
            END as USAGE_CATEGORY,
            
            -- Storage Efficiency
            CASE 
                WHEN tm.SIZE_GB = 0 THEN 'EMPTY'
                WHEN tm.SIZE_GB < 0.1 THEN 'VERY_SMALL'
                WHEN tm.SIZE_GB < 1 THEN 'SMALL'
                WHEN tm.SIZE_GB < 10 THEN 'MEDIUM'
                WHEN tm.SIZE_GB < 100 THEN 'LARGE'
                ELSE 'VERY_LARGE'
            END as SIZE_CATEGORY
            
        FROM table_metrics tm
        ORDER BY tm.QUERIES_ACCESSING_TABLE DESC, tm.SIZE_GB DESC
        LIMIT 1000
        """,
        description='Table-level analytics with access patterns and storage metrics'
    )
}

# Data storage for tables
table_data = {}
last_refresh = {}

def refresh_table_data(table_name: str) -> Dict[str, Any]:
    """Refresh data for a specific table"""
    try:
        if table_name not in QUERIES:
            return {"error": f"Table {table_name} not found"}
        
        query_config = QUERIES[table_name]
        logger.info(f"Executing query for {table_name}")
        
        df = sf_connector.execute_query(query_config.sql)
        
        # Convert DataFrame to dict for JSON serialization
        data = {
            "data": df.to_dict('records'),
            "columns": df.columns.tolist(),
            "row_count": len(df),
            "description": query_config.description,
            "last_updated": datetime.now().isoformat()
        }
        
        # Store in memory
        table_data[table_name] = data
        last_refresh[table_name] = datetime.now()
        
        logger.info(f"Successfully refreshed {table_name} with {len(df)} rows")
        return data
        
    except Exception as e:
        logger.error(f"Error refreshing {table_name}: {e}")
        return {"error": str(e)}

def get_table_data(table_name: str, force_refresh: bool = False) -> Dict[str, Any]:
    """Get table data, refresh if needed"""
    
    # Check if we need to refresh
    needs_refresh = (
        force_refresh or 
        table_name not in table_data or 
        table_name not in last_refresh or
        (datetime.now() - last_refresh[table_name]).total_seconds() > 3600  # 1 hour cache
    )
    
    if needs_refresh:
        return refresh_table_data(table_name)
    
    return table_data[table_name]

# Flask Routes

@app.route('/')
def index():
    """Main dashboard page"""
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>FinOps Analytics Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .card { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .table-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
            .table-card { border-left: 4px solid #3498db; }
            .table-card h3 { margin-top: 0; color: #2c3e50; }
            .btn { background: #3498db; color: white; padding: 10px 20px; text-decoration: none; border-radius: 4px; display: inline-block; margin: 5px; }
            .btn:hover { background: #2980b9; }
            .status { padding: 5px 10px; border-radius: 4px; font-size: 12px; }
            .status.success { background: #2ecc71; color: white; }
            .status.error { background: #e74c3c; color: white; }
            .metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }
            .metric { background: #ecf0f1; padding: 15px; border-radius: 4px; text-align: center; }
            .metric-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
            .metric-label { font-size: 12px; color: #7f8c8d; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🏢 FinOps Analytics Dashboard</h1>
                <p>Snowflake Cost Optimization & Performance Analytics</p>
            </div>
            
            <div class="card">
                <h2>📊 Available Analytics Tables</h2>
                <div class="table-grid">
                    <div class="card table-card">
                        <h3>🏭 Warehouses</h3>
                        <p>Comprehensive warehouse analytics with performance metrics, utilization, and bad practices detection.</p>
                        <a href="/api/tables/warehouses" class="btn">View Data</a>
                        <a href="/api/tables/warehouses/refresh" class="btn">Refresh</a>
                    </div>
                    
                    <div class="card table-card">
                        <h3>👥 Users</h3>
                        <p>User analytics with bad practices, credit consumption, and query patterns.</p>
                        <a href="/api/tables/users" class="btn">View Data</a>
                        <a href="/api/tables/users/refresh" class="btn">Refresh</a>
                    </div>
                    
                    <div class="card table-card">
                        <h3>🔍 Queries</h3>
                        <p>Detailed query analytics with performance categorization and optimization flags.</p>
                        <a href="/api/tables/queries" class="btn">View Data</a>
                        <a href="/api/tables/queries/refresh" class="btn">Refresh</a>
                    </div>
                    
                    <div class="card table-card">
                        <h3>📋 Query Details</h3>
                        <p>Deep dive query performance analysis with optimization recommendations.</p>
                        <a href="/api/tables/query_details" class="btn">View Data</a>
                        <a href="/api/tables/query_details/refresh" class="btn">Refresh</a>
                    </div>
                    
                    <div class="card table-card">
                        <h3>🗄️ Databases</h3>
                        <p>Database-level analytics with storage metrics and query patterns.</p>
                        <a href="/api/tables/databases" class="btn">View Data</a>
                        <a href="/api/tables/databases/refresh" class="btn">Refresh</a>
                    </div>
                    
                    <div class="card table-card">
                        <h3>📊 Tables</h3>
                        <p>Table-level analytics with access patterns and storage efficiency metrics.</p>
                        <a href="/api/tables/tables" class="btn">View Data</a>
                        <a href="/api/tables/tables/refresh" class="btn">Refresh</a>
                    </div>
                </div>
            </div>
            
            <div class="card">
                <h2>🔄 Bulk Operations</h2>
                <a href="/api/refresh-all" class="btn">Refresh All Tables</a>
                <a href="/api/status" class="btn">System Status</a>
            </div>
            
            <div class="card">
                <h2>📖 API Endpoints</h2>
                <ul>
                    <li><strong>GET /api/tables/{table_name}</strong> - Get table data</li>
                    <li><strong>GET /api/tables/{table_name}/refresh</strong> - Refresh specific table</li>
                    <li><strong>GET /api/refresh-all</strong> - Refresh all tables</li>
                    <li><strong>GET /api/status</strong> - System status</li>
                    <li><strong>GET /api/tables</strong> - List all available tables</li>
                </ul>
            </div>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_template)

@app.route('/api/tables')
def list_tables():
    """List all available tables"""
    tables = []
    for name, config in QUERIES.items():
        table_info = {
            "name": name,
            "description": config.description,
            "last_refresh": last_refresh.get(name, None),
            "cached": name in table_data,
            "row_count": len(table_data[name]["data"]) if name in table_data else 0
        }
        tables.append(table_info)
    
    return jsonify({
        "tables": tables,
        "total_tables": len(tables)
    })

@app.route('/api/tables/<table_name>')
def get_table(table_name: str):
    """Get data for a specific table"""
    data = get_table_data(table_name)
    return jsonify(data)

@app.route('/api/tables/<table_name>/refresh')
def refresh_table(table_name: str):
    """Force refresh a specific table"""
    data = refresh_table_data(table_name)
    return jsonify(data)

@app.route('/api/refresh-all')
def refresh_all_tables():
    """Refresh all tables"""
    results = {}
    for table_name in QUERIES.keys():
        try:
            results[table_name] = refresh_table_data(table_name)
            results[table_name]["status"] = "success"
        except Exception as e:
            results[table_name] = {"status": "error", "error": str(e)}
    
    return jsonify({
        "message": "Bulk refresh completed",
        "results": results,
        "total_tables": len(QUERIES),
        "successful": len([r for r in results.values() if r.get("status") == "success"]),
        "failed": len([r for r in results.values() if r.get("status") == "error"])
    })

@app.route('/api/status')
def system_status():
    """Get system status"""
    # Test Snowflake connection
    connection_status = "disconnected"
    try:
        sf_connector.execute_query("SELECT 1")
        connection_status = "connected"
    except:
        connection_status = "error"
    
    return jsonify({
        "system_status": "running",
        "snowflake_connection": connection_status,
        "cached_tables": len(table_data),
        "available_tables": len(QUERIES),
        "last_refresh_times": {
            name: refresh_time.isoformat() if refresh_time else None 
            for name, refresh_time in last_refresh.items()
        },
        "memory_usage": {
            "total_rows": sum(len(data["data"]) for data in table_data.values()),
            "table_counts": {name: len(data["data"]) for name, data in table_data.items()}
        }
    })

@app.route('/api/drill-down/<source_table>/<target_table>')
def drill_down(source_table: str, target_table: str):
    """Enable drill-down from one table to another"""
    # This endpoint will be used for React drill-down functionality
    filters = request.args.to_dict()
    
    # Get the target table data
    target_data = get_table_data(target_table)
    
    if "error" in target_data:
        return jsonify(target_data)
    
    # Apply filters based on drill-down context
    filtered_data = target_data["data"]
    
    # Example drill-down logic (can be expanded)
    if source_table == "warehouses" and target_table == "users":
        warehouse_name = filters.get("warehouse_name")
        if warehouse_name:
            # Filter users who used this warehouse
            # This would require joining data or additional queries
            pass
    
    return jsonify({
        "source_table": source_table,
        "target_table": target_table,
        "filters_applied": filters,
        "data": filtered_data,
        "row_count": len(filtered_data)
    })

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    # Initialize connection and test
    logger.info("Starting FinOps Analytics Application")
    
    if sf_connector.connect():
        logger.info("✅ Snowflake connection successful")
        
        # Pre-load critical tables
        logger.info("Pre-loading warehouse and user data...")
        try:
            refresh_table_data('warehouses')
            refresh_table_data('users')
            logger.info("✅ Initial data loaded successfully")
        except Exception as e:
            logger.error(f"❌ Error loading initial data: {e}")
    else:
        logger.error("❌ Failed to connect to Snowflake")
    
    # Run the Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)