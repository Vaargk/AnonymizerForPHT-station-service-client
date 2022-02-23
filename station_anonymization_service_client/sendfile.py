import asyncio
import os.path

import pandas as pd


async def send_to_anonymizer_and_receive(ip, port, table_file, key_file, syn_table_file):
    reader, writer = await asyncio.open_connection(
        ip, port)

    if not key_file:
        writer.write(b'00000000')
        writer.write(b'00000000')
    else:
        writer.write(len(key_file).to_bytes(2, byteorder='big'))
    print(f'Send key with length: {len(key_file)}')
    writer.write(key_file)
    await writer.drain()
    print(f'Send length of the table to be anonymized: {len(table_file)}')
    writer.write(len(table_file).to_bytes(3, byteorder='big'))
    writer.write(table_file)
    await writer.drain()
    print(f'Send now the table with synthetic data')
    writer.write(syn_table_file)
    await writer.drain()

    print('Close the connection')
    writer.close()
    await writer.wait_closed()


if __name__ == '__main__':
    input_str = ''
    while input_str != 'y' and input_str != 'n':
        input_str = input(f'Do you got data from a previous station? (y|n)')
    if input_str == 'y':
        prev_table_path = os.path.abspath(
            input(f'Please provide the absolute path from the anonymized table received from previous stations'))
        df = pd.read_csv(prev_table_path)
        df.set_index(df.columns[0], inplace=True)
    encrypt_flags_input: str = ''
    key_path = os.path.abspath(input(f"Please provide the absolute path to the public key of the central service!"))
    f = open(key_path, 'rb')
    key_file = f.read()
    f.close()
    table_path = os.path.abspath(input(f"Please provide the absolute path to the table to be anonymized!"))
    if input_str == 'y':
        tmp_df = pd.read_csv(os.path.abspath(table_path))
        tmp_df['flag'] = ''
        tmp_df.reset_index(drop=True)
        original_index = df.index.to_list()
        max_index = df.index.max()
        df = pd.concat([df, tmp_df], ignore_index=True, sort=False)
        df.reset_index(drop=True)
        df.set_index(pd.Index(original_index + list(range(max_index + 1, max_index + 1 + len(tmp_df)))),
                         inplace=True)
        df.to_csv(os.path.abspath('combined.csv'))
        f = open('combined.csv', 'rb')
        table_file = f.read()
        f.close()
    else:
        f = open(table_path, 'rb')
        table_file = f.read()
        f.close()
    syn_table_path = os.path.abspath(input(f"Please provide the absolute path to the synthetic table!"))
    f = open(syn_table_path, 'rb')
    syn_table_file = f.read()
    f.close()
    asyncio.run(send_to_anonymizer_and_receive('127.0.0.1', 5555, table_file, key_file, syn_table_file))
