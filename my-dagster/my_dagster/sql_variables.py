# Place all SQL queries here for better asset readability

create_nyc_data_database: str = "CREATE DATABASE IF NOT EXISTS nyc_data"

create_camera_violations_table: str = """CREATE TABLE IF NOT EXISTS nyc_data.camera_violations
        (
        plate String,
        state LowCardinality(String),
        license_type LowCardinality(String),
        summons_number String,
        issue_date Date,
        violation LowCardinality(String),
        sub_violation LowCardinality(String),
        violation_time String,
        fine_amount Float32,
        penalty_amount Float32,
        interest_amount Float32,
        reduction_amount Float32,
        payment_amount Float32,
        amount_due Float32,
        precinct LowCardinality(String),
        county LowCardinality(String),
        issuing_agency LowCardinality(String),
        violation_status LowCardinality(String),
        sub_violation_status LowCardinality(String),
        summons_image String,
        judgment_entry_date Date
        )
        ENGINE=MergeTree
        PARTITION BY toYYYYMM(issue_date) 
        ORDER BY (violation, issue_date);"""