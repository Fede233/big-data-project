"""Provided runner: start Spark, validate inputs and execute the selected task."""

import argparse
import importlib.util
import os
from pathlib import Path
import sys
import tempfile

from common import (
    ASSIGNMENT_COLUMNS, CUSTOMER_COLUMNS, EMPLOYEE_COLUMNS,
    PROJECT_ROOT, TRANSACTION_COLUMNS, read_csv, write_csv,
)
from quality import classify_transaction, quality_summary


def create_spark_context():
    """Return a local SparkContext using project packages and temporary folders."""
    if sys.prefix == sys.base_prefix:
        raise RuntimeError("Run this application with the project virtual environment's Python")
    temporary = PROJECT_ROOT / ".cache" / "tmp"
    local_spark = PROJECT_ROOT / ".cache" / "spark"
    for directory in (temporary, local_spark):
        directory.mkdir(parents=True, exist_ok=True)
    # Environment changes apply to this process and its workers, not shell profiles.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["TMPDIR"] = str(temporary)
    os.environ["SPARK_LOCAL_DIRS"] = str(local_spark)
    # Override any shell setting pointing to a different Spark installation.
    spark_package = Path(importlib.util.find_spec("pyspark").origin).parent
    if not spark_package.is_relative_to(Path(sys.prefix)):
        raise RuntimeError("PySpark must be installed inside the selected virtual environment")
    os.environ["SPARK_HOME"] = str(spark_package)
    tempfile.tempdir = str(temporary)
    java_options = os.environ.get("JAVA_TOOL_OPTIONS", "")
    os.environ["JAVA_TOOL_OPTIONS"] = f'{java_options} -Djava.io.tmpdir="{temporary}"'.strip()

    from pyspark import SparkConf, SparkContext

    # local[2] means two task threads on the local machine; 1g is the JVM driver heap.
    configuration = SparkConf().setAppName("retail-quality").setMaster("local[2]")
    configuration.set("spark.driver.memory", "1g")
    configuration.set("spark.driver.host", "127.0.0.1")
    configuration.set("spark.driver.bindAddress", "127.0.0.1")
    configuration.set("spark.ui.enabled", "false")
    configuration.set("spark.local.dir", str(local_spark))
    sc = SparkContext(conf=configuration)
    sc.setLogLevel("WARN")
    # Worker processes must be able to import the same functions as the driver.
    for name in ("common.py", "quality.py", "task_1_sales.py", "task_2_products.py", "task_3_customers.py", "task_4_employees.py", "task_5_bonus.py"):
        module_path = PROJECT_ROOT / "src" / name
        # Later task modules may not have been written yet.
        if module_path.is_file():
            sc.addPyFile(str(module_path))
    return sc


def main():
    """Read inputs, check their quality and write the selected results."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=PROJECT_ROOT / "data" / "inputs")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "outputs")
    parser.add_argument("--task", choices=("quality", "1", "2", "3", "4", "5"), default="quality")
    args = parser.parse_args()
    input_dir = args.input_dir.resolve()
    output_dir = args.output_dir.resolve()
    destination = output_dir / "quality" / "quality_summary" if args.task == "quality" else output_dir / f"task_{args.task}"
    if not input_dir.is_relative_to(PROJECT_ROOT) or not destination.is_relative_to(PROJECT_ROOT):
        parser.error("Keep input and output paths inside this project directory")
    if destination.exists():
        parser.error(f"Output already exists. Choose a new --output-dir: {destination}")

    # Start Spark only after checking paths, so a rejected rerun does no work.
    sc = create_spark_context()
    classifiedTransactionsRDD = None
    try:
        workerEnvironmentsRDD = sc.parallelize([0, 1], 2).map(
            lambda _: (__import__("sys").executable, __import__("pyspark").__file__)
        )
        for executable, module_path in workerEnvironmentsRDD.collect():
            if not Path(executable).is_relative_to(Path(sys.prefix)) or not Path(module_path).is_relative_to(Path(sys.prefix)):
                raise RuntimeError("A Spark worker is using Python or PySpark outside the virtual environment")
        print(f"Python: {sys.executable}; Spark distribution: {os.environ['SPARK_HOME']}", flush=True)

        # Load CSV string tuples before parsing and checking transactions.
        transactionsRDD = read_csv(sc, input_dir / "transactions.csv", TRANSACTION_COLUMNS)
        customersRDD = read_csv(sc, input_dir / "customers.csv", CUSTOMER_COLUMNS).map(
            lambda row: tuple(value.strip() for value in row)
        ).cache()
        employeesRDD = read_csv(sc, input_dir / "employees.csv", EMPLOYEE_COLUMNS).map(
            lambda row: tuple(value.strip() for value in row)
        ).cache()
        assignmentsRDD = read_csv(sc, input_dir / "customer_assignments.csv", ASSIGNMENT_COLUMNS).map(
            lambda row: tuple(value.strip() for value in row)
        ).cache()
        input_counts = {}
        for name, rowsRDD in (
            ("transactions", transactionsRDD), ("customers", customersRDD),
            ("employees", employeesRDD), ("assignments", assignmentsRDD),
        ):
            input_counts[name] = rowsRDD.count()
            print(f"{name}: {input_counts[name]:,} rows; examples={rowsRDD.take(3)}", flush=True)

        # Classify transactions for the supplied quality checks and analytical tasks.
        # cache() reuses these records across several checks instead of reparsing them.
        classifiedTransactionsRDD = transactionsRDD.map(classify_transaction).cache()
        # Each record: (class, reason, normalised strings, typed transaction).
        checks = quality_summary(classifiedTransactionsRDD, customersRDD, employeesRDD, assignmentsRDD)
        if checks["total_transaction_rows"] != input_counts["transactions"]:
            raise AssertionError("Loading and classification row counts differ")

        if args.task == "quality":
            summaryRDD = sc.parallelize(sorted(checks.items()), 2)
            # key=check name, value=count; the small summary is written as CSV parts.
            write_csv(summaryRDD, destination, ("Check", "Count"))
            print(f"Spark {sc.version}; master={sc.master}")
            for name, value in sorted(checks.items()):
                print(f"{name}: {value}")
            print(f"PASS: quality checks completed. Output: {destination}")
        elif args.task == "1":
            from task_1_sales import run_task_1

            run_task_1(classifiedTransactionsRDD, output_dir)
            print(f"PASS: Task 1 completed. Output: {destination}")
        elif args.task == "2":
            from task_2_products import run_task_2

            run_task_2(classifiedTransactionsRDD, output_dir)
            print(f"PASS: Task 2 completed. Output: {destination}")
        elif args.task == "3":
            from task_3_customers import run_task_3

            run_task_3(classifiedTransactionsRDD, customersRDD, output_dir)
            print(f"PASS: Task 3 completed. Output: {destination}")
        elif args.task == "4":
            from task_4_employees import run_task_4

            run_task_4(classifiedTransactionsRDD, customersRDD, employeesRDD, assignmentsRDD, output_dir)
            print(f"PASS: Task 4 completed. Output: {destination}")
        else:
            from task_5_bonus import run_task_5

            run_task_5(classifiedTransactionsRDD, employeesRDD, assignmentsRDD, output_dir)
            print(f"PASS: Task 5 completed. Output: {destination}")
    finally:
        # Release Spark resources even if an input check raises an error.
        if classifiedTransactionsRDD is not None:
            classifiedTransactionsRDD.unpersist()
        sc.stop()


if __name__ == "__main__":
    main()
