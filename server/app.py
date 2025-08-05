from flask import Flask, jsonify
from flask_cors import CORS # This is important for your React app

app = Flask(__name__)
# This allows your React app on localhost:3000 to make requests to this API
CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

# Define the data structure with sample queries
# This is the same structure as the mock data in your JavaScript,
# but now it's coming from the server.
finops_data = [
    {
        "USER_NAME": "john.doe@company.com",
        "TOTAL_QUERIES": 15420,
        "TOTAL_CREDITS": 2580.5,
        "WEIGHTED_SCORE": 85.2,
        "COST_STATUS": "High Cost",
        "SPILLED_QUERIES": 45,
        "OVER_PROVISIONED_QUERIES": 23,
        "PEAK_HOUR_LONG_RUNNING_QUERIES": 12,
        "SELECT_STAR_QUERIES": 156,
        "UNPARTITIONED_SCAN_QUERIES": 78,
        "ZERO_RESULT_QUERIES": 234,
        "HIGH_COMPILE_QUERIES": 34,
        "UNLIMITED_ORDER_BY_QUERIES": 89,
        "CARTESIAN_JOIN_QUERIES": 5,
        "QUERY_SAMPLES": {
            "spilled": [
                {
                    "query_id": "Q1000",
                    "query_text": "SELECT customer_id, product_name, order_date, SUM(order_amount) as total_amount FROM orders GROUP BY customer_id, product_name, order_date;",
                    "start_time": "2024-07-16T10:30:00Z",
                    "execution_time_ms": 45000,
                    "bytes_scanned": 5368709120,
                    "warehouse_size": "LARGE",
                    "execution_status": "SUCCESS",
                    "compilation_time": 2000,
                    "rows_produced": 8500,
                },
                {
                    "query_id": "Q1001",
                    "query_text": "SELECT * FROM large_raw_data_table WHERE process_date > '2023-01-01';",
                    "start_time": "2024-07-16T11:45:00Z",
                    "execution_time_ms": 50000,
                    "bytes_scanned": 6442450944,
                    "warehouse_size": "X-LARGE",
                    "execution_status": "SUCCESS",
                    "compilation_time": 2500,
                    "rows_produced": 9500,
                }
            ],
            # This user has no over-provisioned queries, which is a key part of the fix.
            "over_provisioned": [], 
            "peak_hour_long_running": [
                {
                    "query_id": "Q1002",
                    "query_text": "SELECT id FROM logs WHERE created_at < '2024-01-01';",
                    "start_time": "2024-07-16T15:15:00Z",
                    "execution_time_ms": 70000,
                    "bytes_scanned": 10737418240,
                    "warehouse_size": "LARGE",
                    "execution_status": "SUCCESS",
                    "compilation_time": 3000,
                    "rows_produced": 10000,
                }
            ],
        },
    },
    {
        "USER_NAME": "jane.smith@company.com",
        "TOTAL_QUERIES": 8920,
        "TOTAL_CREDITS": 1250.75,
        "WEIGHTED_SCORE": 72.8,
        "COST_STATUS": "Optimal",
        "SPILLED_QUERIES": 12,
        "OVER_PROVISIONED_QUERIES": 45,
        "PEAK_HOUR_LONG_RUNNING_QUERIES": 8,
        "SELECT_STAR_QUERIES": 89,
        "UNPARTITIONED_SCAN_QUERIES": 34,
        "ZERO_RESULT_QUERIES": 123,
        "HIGH_COMPILE_QUERIES": 67,
        "UNLIMITED_ORDER_BY_QUERIES": 23,
        "CARTESIAN_JOIN_QUERIES": 2,
        "QUERY_SAMPLES": {
            "over_provisioned": [
                {
                    "query_id": "Q2000",
                    "query_text": "SELECT id, name FROM small_table WHERE id = 123;",
                    "start_time": "2024-07-17T14:15:00Z",
                    "execution_time_ms": 850,
                    "bytes_scanned": 104857600,
                    "warehouse_size": "X-LARGE",
                    "execution_status": "SUCCESS",
                    "compilation_time": 150,
                    "rows_produced": 1,
                }
            ],
            "spilled": [],
        },
    },
    {
        "USER_NAME": "mark.jones@company.com",
        "TOTAL_QUERIES": 25000,
        "TOTAL_CREDITS": 3500,
        "WEIGHTED_SCORE": 91.5,
        "COST_STATUS": "High Cost",
        "SPILLED_QUERIES": 60,
        "OVER_PROVISIONED_QUERIES": 30,
        "PEAK_HOUR_LONG_RUNNING_QUERIES": 20,
        "SELECT_STAR_QUERIES": 200,
        "UNPARTITIONED_SCAN_QUERIES": 90,
        "ZERO_RESULT_QUERIES": 300,
        "HIGH_COMPILE_QUERIES": 50,
        "UNLIMITED_ORDER_BY_QUERIES": 100,
        "CARTESIAN_JOIN_QUERIES": 8,
        "QUERY_SAMPLES": {
            "spilled": [
                {
                    "query_id": "Q3000",
                    "query_text": "SELECT * FROM another_large_table WHERE date_column = '2024-07-18';",
                    "start_time": "2024-07-18T09:00:00Z",
                    "execution_time_ms": 60000,
                    "bytes_scanned": 8589934592,
                    "warehouse_size": "2X-LARGE",
                    "execution_status": "SUCCESS",
                    "compilation_time": 3500,
                    "rows_produced": 15000,
                }
            ],
            # This user has no over-provisioned queries, which is a key part of the fix.
            "over_provisioned": [{
                    "query_id": "Q3000",
                    "query_text": "SELECT * FROM another_large_table WHERE date_column = '2024-07-18';",
                    "start_time": "2024-07-18T09:00:00Z",
                    "execution_time_ms": 60000,
                    "bytes_scanned": 8589934592,
                    "warehouse_size": "2X-LARGE",
                    "execution_status": "SUCCESS",
                    "compilation_time": 3500,
                    "rows_produced": 15000,
                },{
                    "query_id": "Q3000",
                    "query_text": "SELECT * FROM another_large_table WHERE date_column = '2024-07-18';",
                    "start_time": "2024-07-18T09:00:00Z",
                    "execution_time_ms": 60000,
                    "bytes_scanned": 8589934592,
                    "warehouse_size": "2X-LARGE",
                    "execution_status": "SUCCESS",
                    "compilation_time": 3500,
                    "rows_produced": 15000,
                }], 
        },
    }
]


@app.route('/api/users', methods=['GET'])
def get_users():
    """
    Returns the FinOps user data as a JSON object.
    """
    return jsonify(finops_data)

if __name__ == '__main__':
    # Run the server on port 5000
    app.run(debug=True, port=5000)