import os
import duckdb
import yaml
import glob
from datetime import datetime
import json
from urllib.parse import urlparse
import boto3
from abc import ABC, abstractmethod


class FileProcessor(ABC):
    @abstractmethod
    def read_to_temp_table(self, conn, input_path, table_name):
        pass


class CSVFileProcessor(FileProcessor):
    def read_to_temp_table(self, conn, input_path, table_name):
        table_exists = \
            conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[
                0] > 0
        if not table_exists:
            query = f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{input_path}', union_by_name=True);
            """
            conn.execute(query)
            print(f"Table '{table_name}' created and data inserted from CSV.")
        else:
            query = f"""
            INSERT INTO {table_name}
            SELECT * FROM read_csv_auto('{input_path}', union_by_name=True);
            """
            conn.execute(query)
            print(f"New data inserted into '{table_name}' from CSV.")


class ParquetFileProcessor(FileProcessor):
    def read_to_temp_table(self, conn, input_path, table_name):
        table_exists = \
            conn.execute(
                f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[
                0] > 0
        if not table_exists:
            query = f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_parquet('{input_path}');
            """
            conn.execute(query)
            print(f"Table '{table_name}' created and data inserted from Parquet.")
        else:
            query = f"""
            INSERT INTO {table_name}
            SELECT * FROM read_parquet('{input_path}');
            """
            conn.execute(query)
            print(f"New data inserted into '{table_name}' from Parquet.")


class IncrementalFileProcessor:
    def __init__(self, path, checkpoint_path, minio_config=None):
        self.path = path
        self.checkpoint_path = checkpoint_path
        self.parsed_url = urlparse(self.path)
        self.checkpoint_parsed_url = urlparse(self.checkpoint_path)
        self.client = self._get_client(minio_config)
        self.last_checkpoint_time = self._load_checkpoint()

    def _get_client(self, minio_config):
        if self.parsed_url.scheme in ['s3', 's3a'] or self.checkpoint_parsed_url.scheme in ['s3', 's3a']:
            if minio_config:
                return boto3.client('s3', endpoint_url=minio_config['endpoint_url'],
                                    aws_access_key_id=minio_config['access_key'],
                                    aws_secret_access_key=minio_config['secret_key'])
            else:
                return boto3.client('s3')
        return None

    def _load_checkpoint(self):
        if self.checkpoint_parsed_url.scheme in ['s3', 's3a']:
            try:
                bucket, key = self._parse_s3_path(self.checkpoint_path)
                response = self.client.get_object(Bucket=bucket, Key=key)
                return json.load(response['Body']).get('last_processed_time', 0)
            except self.client.exceptions.NoSuchKey:
                return 0
        else:
            if os.path.exists(self.checkpoint_path):
                with open(self.checkpoint_path, 'r') as f:
                    return json.load(f).get('last_processed_time', 0)
            return 0

    def _parse_s3_path(self, s3_path):
        parsed = urlparse(s3_path)
        return parsed.netloc, parsed.path.lstrip('/')

    def _list_s3_files(self):
        bucket, prefix = self._parse_s3_path(self.path)
        files = []
        paginator = self.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['LastModified'].timestamp() > self.last_checkpoint_time:
                    files.append(f"s3://{bucket}/{obj['Key']}")
        return files

    def _list_local_files(self):
        files = []
        directory = self.parsed_url.path
        for root, _, filenames in os.walk(directory):
            for filename in filenames:
                file_path = os.path.join(root, filename)
                if os.path.getmtime(file_path) > self.last_checkpoint_time:
                    files.append(file_path)
        return files

    def get_new_files(self):
        if self.parsed_url.scheme in ['s3', 's3a']:
            return self._list_s3_files()
        elif self.parsed_url.scheme == 'file' or not self.parsed_url.scheme:
            return self._list_local_files()
        else:
            raise ValueError(f"Unsupported scheme: {self.parsed_url.scheme}")

    def commit_checkpoint(self):
        current_time = datetime.now().timestamp()
        checkpoint_data = json.dumps({'last_processed_time': current_time})
        if self.checkpoint_parsed_url.scheme in ['s3', 's3a']:
            bucket, key = self._parse_s3_path(self.checkpoint_path)
            self.client.put_object(Bucket=bucket, Key=key, Body=checkpoint_data)
        else:
            os.makedirs(os.path.dirname(self.checkpoint_path), exist_ok=True)
            with open(self.checkpoint_path, 'w') as f:
                f.write(checkpoint_data)
        print(f"Checkpoint updated to: {datetime.fromtimestamp(current_time)}")


class DataTransformer:
    def __init__(self, conn):
        self.conn = conn

    def transform_and_export(self, transform_sql, output_path, mode='append', output_format='csv'):
        self.conn.execute(f"CREATE OR REPLACE VIEW transformed_data AS {transform_sql};")
        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        if mode == 'overwrite':
            for file in glob.glob(os.path.join(output_path, f"*.{output_format}")):
                os.remove(file)
                print(f"Deleted existing file: {file}")
        output_file = os.path.join(output_path, f"{timestamp}.{output_format}")
        if output_format == 'csv':
            self.conn.execute(f"COPY (SELECT * FROM transformed_data) TO '{output_file}' WITH (FORMAT CSV, HEADER);")
        elif output_format == 'parquet':
            self.conn.execute(f"COPY (SELECT * FROM transformed_data) TO '{output_file}' WITH (FORMAT PARQUET);")
        else:
            raise ValueError("Unsupported output format. Use 'csv' or 'parquet'.")
        print(f"Data exported to {output_file} successfully!")


def load_config(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def create_output_directory(output_path):
    if not output_path.startswith('s3'):
        if not os.path.exists(output_path):
            os.makedirs(output_path)
            print(f"Created output directory: {output_path}")
    else:
        print(f"Skipping directory creation for S3 path: {output_path}")


def main():
    config = load_config('config.yaml')
    conn = duckdb.connect(config['duckdb']['path'])

    # Load extensions from config
    for extension in config['duckdb'].get('extension', []):
        extension_name = extension['name']
        conn.execute(f"INSTALL {extension_name};")
        conn.execute(f"LOAD {extension_name};")
        print(f"Loaded extension: {extension_name}")

    # Set AWS configuration (replace with your actual values)
    create_output_directory(config['output']['path'])

    input_format_processor_map = {
        'csv': CSVFileProcessor(),
        'parquet': ParquetFileProcessor()
    }

    for table_config in config['input']['tables']:
        table_name = table_config['name']
        input_format = table_config['format']
        input_path = table_config['path']
        input_mode = table_config['mode']

        input_processor = input_format_processor_map[input_format]

        if input_mode == 'full':
            input_processor.read_to_temp_table(conn, input_path, table_name)
        elif input_mode == 'INC':
            processor = IncrementalFileProcessor(input_path, config['checkpoint']['path'])
            new_files = processor.get_new_files()
            if new_files:
                for new_file in new_files:
                    print(f"Processing new file for {table_name}: {new_file}")
                    input_processor.read_to_temp_table(conn, new_file, table_name)
                processor.commit_checkpoint()
            else:
                print(f"No new files to process for {table_name} in incremental mode.")

    transformer = DataTransformer(conn)
    transform_sql = config['transform']['sql']
    transformer.transform_and_export(transform_sql,
                                     config['output']['path'],
                                     config['output']['mode'],
                                     config['output']['format'])

    conn.close()


if __name__ == '__main__':
    main()
