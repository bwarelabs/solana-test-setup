from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from google.oauth2 import service_account
import os
import pydoop
import pydoop.hdfs as hdfs

os.environ['BIGTABLE_EMULATOR_HOST'] = 'bigtable-emulator:8086'
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/usr/local/bin/solana-data-migration-1b9caa70b7cc.json'


def read_from_bigtable(project_id, instance_id, table_id):
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    table = instance.table(table_id)

    print("table: ", table.list_column_families)

    rows = table.read_rows(filter_=row_filters.PassAllFilter(flag=True))
    rows.consume_all()

    data = []
    for row in rows.rows.values():
        for column_family, cols in row.cells.items():
            for column, cell_list in cols.items():
                for cell in cell_list:
                    data.append((row.row_key.decode('utf-8'), cell.value))
    return data

def write_to_sequence_file(data, output_path):
    writer = pydoop.SequenceFile.Writer(output_path, key_class='org.apache.hadoop.io.Text',
                                        value_class='org.apache.hadoop.io.BytesWritable')
    try:
        for key, value in data:
            writer.append(key, value)
    finally:
        writer.close()

def print_table_schema(project_id, instance_id, table_id):
    # os.environ['BIGTABLE_EMULATOR_HOST'] = '172.30.0.2:8086'
    # os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/usr/local/bin/solana-data-migration-1b9caa70b7cc.json'

    client = bigtable.Client(project=project_id, admin=True)

    instance = client.instance(instance_id)
    table = instance.table(table_id)

    schema = table.list_column_families()
    print(f"Table schema for '{table_id}':")
    for family, gc_rule in schema.items():
        print(f"Column Family: {family}, GC Rule: {gc_rule}")


if __name__ == "__main__":
    print("Reading data from Bigtable and writing to SequenceFile...")

    print_table_schema('emulator', 'solana-ledger', 'blocks')

    print("Reading data from Bigtable...")

    data = read_from_bigtable('emulator', 'solana-ledger', 'blocks')
    print(data[0])

    output_path = '/output/output.seq' 
    write_to_sequence_file(data, output_path)
