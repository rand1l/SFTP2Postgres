import itertools
import paramiko
import psycopg2

from concurrent.futures import ThreadPoolExecutor


class sftp_config:
    def __init__(self,
                 sftp_host,
                 sftp_username,
                 sftp_password,
                 sftp_remote_path,
                 sftp_port=22,
                 ssh_private_key_path=None):
        self.sftp_host = sftp_host
        self.sftp_port = sftp_port
        self.sftp_username = sftp_username
        self.sftp_password = sftp_password
        self.sftp_remote_path = sftp_remote_path
        self.ssh_private_key_path = ssh_private_key_path


class csv_remote_file_config:
    def __init__(self,
                 scip_lines,
                 header=None,
                 has_header=True,
                 auto_generate_schema=True):
        self.scip_lines = scip_lines
        self.header = header
        self.has_header = has_header
        self.auto_generate_schema = auto_generate_schema


class thread_config:
    def __init__(self,
                 batch_size=1,
                 thread_count=1):
        self.thread_count = thread_count
        self.batch_size = batch_size


class db_config:
    def __init__(self,
                 db,
                 user,
                 password,
                 host,
                 table,
                 port=5432):
        self.db = db
        self.user = user
        self.password = password
        self.host = host
        self.table = table
        self.port = port


def worker(partition, cursor, table, header):

    query = f"INSERT INTO {table} ({', '.join(header)}) VALUES \n"

    values = []
    for row in partition:
        if row == '\n':
            continue
        row = row.rstrip("\n")
        rows = [f"'{value}'" for value in row.split(',')]
        values.append(f"    ({', '.join(rows)})")

    query += ', \n'.join(values)
    query += f"\n    ON CONFLICT ({', '.join(header)}) DO NOTHING;"

    cursor.execute(query)
    cursor.commit()
    cursor.close()


def generate_header_with_header_auto(remote_file, config):
    for first_line in itertools.islice(remote_file, 1):
        return [s.strip() for s in first_line.split(",")]


def generate_header_with_header_config(remote_file, config):
    return config.header


def generate_header_without_header_auto(remote_file, config):
    for first_line in itertools.islice(remote_file, 1):
        return [f"column{i}" for i in range(1, first_line.count(",") + 1)]


def sftp_to_db(sftp_config, csv_remote_file_config, thread_config, db_config):

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connection = psycopg2.connect(
        user=db_config.user,
        password=db_config.password,
        host=db_config.host,
        port=db_config.port,
        database=db_config.db
    )

    if not sftp_config.ssh_private_key_path:
        ssh_client.connect(
            hostname=sftp_config.sftp_host,
            username=sftp_config.sftp_username,
            password=sftp_config.sftp_password,
            port=sftp_config.sftp_port
        )
    else:
        pkey = paramiko.RSAKey(filename=sftp_config.ssh_private_key_path)
        ssh_client.connect(
            hostname=sftp_config.sftp_host,
            username=sftp_config.sftp_username,
            password=sftp_config.sftp_password,
            port=sftp_config.sftp_port,
            pkey=pkey
        )

    try:

        with ssh_client.open_sftp() as sftp:

            remote_file = sftp.file(sftp_config.sftp_remote_path, 'r')
            remote_file.seek(0)

            header_generators = {
                (True, True): generate_header_with_header_auto,
                (True, False): generate_header_with_header_config,
                (False, True): generate_header_without_header_auto
            }

            key = (csv_remote_file_config.has_header, csv_remote_file_config.auto_generate_schema)
            if key not in header_generators:
                raise Exception("некорректные данные в csv_remote_file_config")

            header = header_generators[key](remote_file, csv_remote_file_config)

            for _ in itertools.islice(remote_file, csv_remote_file_config.scip_lines):
                pass

            with ThreadPoolExecutor(max_workers=thread_config.thread_count) as executor:
                while True:
                    partition = list(itertools.islice(remote_file, thread_config.batch_size))
                    if not any(partition):
                        break
                    executor.submit(worker, partition, connection.cursor(), db_config.table, header)
    finally:
        ssh_client.close()
        connection.close()


if __name__ == '__main__':
    sftp_config = sftp_config(
        sftp_host="host",
        sftp_password="password",
        sftp_username="username",
        sftp_remote_path="/data/example.csv"
    )

    csv_remote_file_config = csv_remote_file_config(
        scip_lines=1,
        header=["column1", "column2", "column3", "column4", "column5"],
        has_header=True,
        auto_generate_schema=False
    )

    thread_config = thread_config(
        batch_size=10,
        thread_count=2
    )

    db_config = db_config(
        db="db",
        password="password",
        user="user",
        host="0.0.0.0",
        table="table"
    )

    sftp_to_db(sftp_config,
               csv_remote_file_config,
               thread_config,
               db_config)

