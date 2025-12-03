"""
Airflow 3 TaskFlow API DAG demonstrating best practices.

This DAG showcases:
1. Idempotent operations
2. Proper use of execution date vs datetime.now()
3. Support for manual runs and backfills
4. TaskFlow API syntax
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)


# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='best_practices_taskflow_dag',
    default_args=default_args,
    description='Demonstrates idempotency and proper date handling',
    schedule='@weekly',  # Run weekly
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Set to True if you want to backfill historical runs
    max_active_runs=1,
    tags=['best-practices', 'taskflow', 'example'],
)
def best_practices_dag():
    """
    Main DAG function using TaskFlow API.
    """

    @task
    def demonstrate_bad_practice(**context):
        """
        ❌ BAD PRACTICE: Using datetime.now()
        
        This will always use the current wall-clock time, making the task
        non-deterministic and breaking idempotency. If you backfill or retry,
        you'll get different results.
        """
        from datetime import datetime as dt
        
        # This is BAD - datetime.now() changes every time you run
        current_time = dt.now()
        logger.warning(f"❌ BAD: Using datetime.now() = {current_time}")
        logger.warning("This breaks idempotency and backfills!")
        
        return {
            'bad_timestamp': current_time.isoformat(),
            'problem': 'This timestamp changes on every retry/backfill'
        }

    @task
    def demonstrate_good_practice(**context):
        """
        ✅ GOOD PRACTICE: Using execution_date from context
        
        This uses the logical date of the DAG run, which is deterministic.
        Multiple runs of the same DAG for the same execution date will
        produce the same results - this is idempotent!
        """
        # Access execution date from context - this is GOOD
        execution_date = context['logical_date']  # Airflow 3 uses 'logical_date'
        ds = context['ds']  # Date string in YYYY-MM-DD format
        ds_nodash = context['ds_nodash']  # Date string in YYYYMMDD format
        
        logger.info(f"✅ GOOD: Using logical_date = {execution_date}")
        logger.info(f"✅ GOOD: Using ds = {ds}")
        logger.info(f"✅ GOOD: Using ds_nodash = {ds_nodash}")
        
        return {
            'execution_date': execution_date.isoformat(),
            'ds': ds,
            'ds_nodash': ds_nodash,
            'benefit': 'This is deterministic and idempotent!'
        }

    @task
    def idempotent_data_processing(good_practice_data: dict, **context):
        """
        ✅ IDEMPOTENT DATA PROCESSING
        
        This task demonstrates idempotent data processing:
        - Uses execution date to determine which data to process
        - Can be safely retried without side effects
        - Produces same results for same input date
        - Works correctly with backfills
        """
        execution_date = context['logical_date']
        ds = context['ds']
        
        logger.info(f"Processing data for date: {ds}")
        
        # Simulate idempotent operations:
        # 1. Read data for specific date
        data_path = f"/data/raw/{ds}/input.csv"
        logger.info(f"Reading from: {data_path}")
        
        # 2. Process with deterministic logic
        processed_records = simulate_processing(ds)
        
        # 3. Write with upsert (idempotent write)
        output_path = f"/data/processed/{ds}/output.csv"
        logger.info(f"Writing to: {output_path} (upsert mode)")
        
        return {
            'date_processed': ds,
            'records_processed': processed_records,
            'output_location': output_path,
            'is_idempotent': True
        }

    @task
    def compare_approaches(bad_data: dict, good_data: dict, processed_data: dict):
        """
        Compare the different approaches and log the differences.
        """
        logger.info("=" * 60)
        logger.info("COMPARISON OF APPROACHES")
        logger.info("=" * 60)
        
        logger.info("\n❌ BAD PRACTICE (datetime.now()):")
        logger.info(f"   Timestamp: {bad_data['bad_timestamp']}")
        logger.info(f"   Problem: {bad_data['problem']}")
        
        logger.info("\n✅ GOOD PRACTICE (execution_date/ds):")
        logger.info(f"   Execution Date: {good_data['execution_date']}")
        logger.info(f"   DS: {good_data['ds']}")
        logger.info(f"   Benefit: {good_data['benefit']}")
        
        logger.info("\n✅ IDEMPOTENT PROCESSING:")
        logger.info(f"   Date Processed: {processed_data['date_processed']}")
        logger.info(f"   Records: {processed_data['records_processed']}")
        logger.info(f"   Output: {processed_data['output_location']}")
        logger.info(f"   Idempotent: {processed_data['is_idempotent']}")
        
        logger.info("\n" + "=" * 60)
        logger.info("KEY TAKEAWAYS:")
        logger.info("=" * 60)
        logger.info("1. Always use context['logical_date'] or context['ds']")
        logger.info("2. Never use datetime.now() for business logic")
        logger.info("3. Design tasks to be idempotent (same input = same output)")
        logger.info("4. Use upsert operations instead of append-only")
        logger.info("5. Make tasks safely retryable without side effects")
        logger.info("=" * 60)
        
        return "Comparison complete!"

    @task
    def demonstrate_manual_run_support(**context):
        """
        ✅ MANUAL RUN SUPPORT
        
        This task works correctly for both scheduled and manual runs.
        We can detect if it's a manual run and handle accordingly.
        """
        run_type = context.get('dag_run').run_type  # 'scheduled', 'manual', 'backfill'
        is_manual = run_type == 'manual'
        logical_date = context['logical_date']
        
        logger.info(f"Run Type: {run_type}")
        logger.info(f"Is Manual: {is_manual}")
        logger.info(f"Logical Date: {logical_date}")
        
        if is_manual:
            logger.info("This is a manual run - processing current execution date")
        else:
            logger.info("This is a scheduled/backfill run - processing scheduled date")
        
        return {
            'run_type': run_type,
            'is_manual': is_manual,
            'processed_date': context['ds']
        }

    # Define task dependencies
    bad_result = demonstrate_bad_practice()
    good_result = demonstrate_good_practice()
    processed_result = idempotent_data_processing(good_result)
    manual_run_result = demonstrate_manual_run_support()
    
    # Final comparison task
    compare_approaches(bad_result, good_result, processed_result)


def simulate_processing(date_str: str) -> int:
    """
    Simulate deterministic data processing.
    
    For the same date, this always returns the same number of records.
    This demonstrates idempotency.
    """
    # Use date as seed for deterministic "randomness"
    date_hash = sum(ord(c) for c in date_str)
    return (date_hash % 1000) + 100


# Instantiate the DAG
dag_instance = best_practices_dag()


"""
USAGE INSTRUCTIONS:
===================

1. MANUAL RUN:
   - Go to Airflow UI
   - Click "Trigger DAG" button
   - The DAG will use the current logical date
   - All tasks will process data for that specific date

2. BACKFILL:
   Run from command line:
   
   airflow dags backfill \
       best_practices_taskflow_dag \
       --start-date 2024-01-01 \
       --end-date 2024-01-31
   
   This will run the DAG for each day in January 2024, and each run
   will be idempotent - you can run it multiple times safely.

3. SCHEDULED RUN:
   - The DAG runs automatically weekly (schedule='@weekly')
   - Each run processes data for its logical date
   - catchup=False means it won't backfill missed runs automatically

4. TESTING IDEMPOTENCY:
   - Run the DAG for a specific date
   - Clear the tasks and run again
   - Verify the outputs are identical
   - This proves idempotency!

KEY CONFIGURATION OPTIONS:
==========================

- catchup=False: Don't automatically backfill historical runs
              Set to True if you want automatic backfilling
              
- max_active_runs=1: Only one DAG instance runs at a time
                    Prevents parallel runs from interfering

- depends_on_past=False: Tasks don't depend on previous runs succeeding
                        Set to True if you need sequential processing

BEST PRACTICES DEMONSTRATED:
============================

✅ Use context['logical_date'] instead of datetime.now()
✅ Use context['ds'] for date string (YYYY-MM-DD)
✅ Design idempotent tasks (same input = same output)
✅ Use upsert operations for data writes
✅ Support both manual and scheduled runs
✅ Make tasks safely retryable
✅ Use proper error handling and logging
✅ Include data lineage in task outputs
"""
