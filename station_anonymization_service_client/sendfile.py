import asyncio
import os.path


async def send_to_anonymizer_and_receive(ip, port, table_file, key_file):
    reader, writer = await asyncio.open_connection(
        ip, port)

    if not key_file:
        writer.write(b'00000000')
        writer.write(b'00000000')
    else:
        writer.write(len(key_file).to_bytes(2, byteorder='big'))
    print(f'Send: {key_file!r}')
    writer.write(key_file)
    await writer.drain()
    print(f'Send: {table_file!r}')
    writer.write(table_file)
    await writer.drain()

    # data = await reader.read()
    # print(f'Received: {data.decode()!r}')

    print('Close the connection')
    writer.close()
    await writer.wait_closed()

if __name__ == '__main__':
    encrypt_flags_input: str = ''
    # while encrypt_flags_input != 'y' and encrypt_flags_input != 'n':
    #     encrypt_flags_input = input('Do you want to encrypt the flags of the dataframe? (y,n)')
    encrypt_flags_input = 'y' # no input required
    if encrypt_flags_input == 'y':
        key_path = os.path.abspath(input(f"Please provide the absolute path to the public key of the central service!"))
        f = open(key_path, 'rb')
        key_file = f.read()
        f.close()
    else:
        key_file = b''
    table_path = os.path.abspath(input(f"Please provide the absolute path to the table to be anonymized!"))
    f = open(table_path, 'rb')
    table_file = f.read()
    f.close()
    asyncio.run(send_to_anonymizer_and_receive('127.0.0.1', 5555, table_file, key_file))


