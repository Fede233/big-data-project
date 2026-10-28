# Retail Sales & Customer Operations Analytics

A Big Data Analytics project using local Apache Spark. For the fictional
retailer PoliRetail, analyse real UCI transactions linked to simulated customer
profiles and employee portfolios using PySpark RDDs.

## Start here

Read [PROJECT_BRIEF.md](PROJECT_BRIEF.md): it contains the scenario, input
schemas, setup, working order, task requests and required outputs.
Use the setup in Section 4, then complete Tasks 1-5 in order.
Write only the five task modules and `docs/results.md`; all other files are
provided support.

## Project structure

| File or folder | Role |
| --- | --- |
| `PROJECT_BRIEF.md` | Assignment specification and instructions. |
| `requirements.txt` | Pinned Python dependencies for `.venv`. |
| `.vscode/launch.json` | Ready-to-use test and task runs in VS Code. |
| `scripts/prepare_inputs.py` | Provided tool to download and prepare the four inputs. |
| `src/main.py`, `src/common.py` | Provided Spark runner and CSV support. |
| `src/quality.py` | Provided transaction classification, input checks and quality summary. |
| `src/task_1_sales.py` to `src/task_5_bonus.py` | Analytical solution, one module per task. |
| `tests/test_quality.py` | Five checks for the provided input and quality support. |
| `tests/test_prepare_inputs.py` | Four provided checks for input preparation. |
| `tests/test_tasks.py` | Five provided checks for the analytical tasks. |
| `data/` | Local source workbook, prepared inputs and provenance manifest. |
| `outputs/` | Local CSV results, organised by task. |
| `docs/results.md` | Short report of computed results and comments. |

Data: [UCI Online Retail II](https://archive.ics.uci.edu/dataset/502/online+retail+ii),
Chen (2012), DOI [10.24432/C5CG6D](https://doi.org/10.24432/C5CG6D), CC BY 4.0.
