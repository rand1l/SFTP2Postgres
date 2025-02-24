# SFTP2Postgres

SFTP2Postgres is a Python-based tool that automates the process of importing CSV files from a remote server (accessed via SFTP) into a PostgreSQL database. It is designed to handle large CSV files efficiently through multithreaded batch processing.

## Features

- **Secure SFTP Connection:** Uses Paramiko to establish SFTP connections to remote servers.
- **CSV File Processing:** Supports flexible handling of CSV files:
  - Automatically generates header columns if not provided.
  - Uses a custom header configuration if available.
- **PostgreSQL Integration:** Connects to PostgreSQL using psycopg2 and performs batch inserts with upsert behavior (ON CONFLICT DO NOTHING).
- **Multithreading:** Utilizes `ThreadPoolExecutor` to process CSV data in parallel batches, improving performance.
- **Configurable Parameters:** Easily adjust SFTP, CSV, threading, and database configurations via dedicated classes.

You can install the Python dependencies using pip:

```bash
pip install paramiko psycopg2
