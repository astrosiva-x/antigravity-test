
import pendulum
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
import datetime
import os

# Define a path for our demo output
DEMO_OUTPUT_DIR = "/tmp/airflow_demo_output"

@dag(
    dag_id="idempotent_best_practices_demo",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=True,  # Enable backfill: Airflow will run for all past intervals
    tags=["example", "best_practices", "idempotency"],
    doc_md="""
    # Best Practices Demo DAG
    
    This DAG demonstrates:
    1. **TaskFlow API**: Using modern `@dag` and `@task` decorators.
    2. **Idempotency**: Tasks can be run multiple times without side effects or duplication.
    3. **Deterministic Time**: Using `ds` (logical date) instead of `datetime.now()`.
    
    ## The Scenario
    We are simulating a daily job that writes a report file. 
    - If we use `datetime.now()`, running a backfill for 2023-01-01 today would generate a file named with TODAY'S date, which is wrong.
    - If we use `ds`, it will correctly generate a file named `2023-01-01.txt` even if we run it years later.
    """
)
def idempotent_dag():

    @task
    def create_setup_dir():
        """
        Idempotent setup: `exist_ok=True` ensures this doesn't fail if dir exists.
        """
        os.makedirs(DEMO_OUTPUT_DIR, exist_ok=True)
        print(f"Directory {DEMO_OUTPUT_DIR} is ready.")

    @task
    def bad_practice_task():
        """
        ANTI-PATTERN: Using datetime.now() for business logic.
        
        If you trigger a backfill for 2023-01-01, this task will print/use 
        the CURRENT system time, breaking the ability to re-process past data correctly.
        """
        now = datetime.datetime.now()
        print(f"BAD: I think the data date is {now}. If this is a backfill, I am wrong!")
        return str(now)

    @task
    def good_practice_task(ds=None):
        """
        BEST PRACTICE: Using Airflow context variables (ds, logical_date).
        
        `ds` is injected automatically by Airflow. It represents the logical date 
        (YYYY-MM-DD) of the DAG run.
        
        - Manual Run today: ds = Today
        - Backfill for 2023-01-01: ds = 2023-01-01 (even if run today)
        """
        print(f"GOOD: Processing data for logical date: {ds}")
        return ds

    @task
    def write_idempotent_file(date_str: str):
        """
        Idempotent Write:
        Instead of appending to a file (which would duplicate data on retry),
        we overwrite a specific file named by the ID/Date.
        
        Result: You can run this task 100 times, and the result is always 
        ONE file with the correct content.
        """
        filename = f"{DEMO_OUTPUT_DIR}/report_{date_str}.txt"
        
        # Write mode 'w' ensures we overwrite, not append.
        with open(filename, "w") as f:
            f.write(f"Report for date: {date_str}\n")
            f.write("This file was generated idempotently.\n")
            
        print(f"Successfully wrote {filename}")

    # Define the flow
    setup = create_setup_dir()
    
    # We run both bad and good to see the logs (in a real app, don't do the bad one!)
    bad_date = bad_practice_task()
    good_date = good_practice_task()
    
    # The writing task depends on setup and the date
    # We use the GOOD date for the file writing to ensure backfills work
    setup >> write_idempotent_file(good_date)

    # Just for demonstration, we chain them so they don't run in parallel purely for log readability
    chain(setup, bad_date, good_date)

# Instantiate the DAG
dag_instance = idempotent_dag()
