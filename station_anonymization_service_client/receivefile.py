import asyncio


async def send_to_anonymizer_and_receive(ip, port, file):
    reader, writer = await asyncio.open_connection(
        ip, port)

    print(f'Send: {file!r}')
    writer.write(file)
    await writer.drain()

    data = await reader.read()
    print(f'Received: {data.decode()!r}')

    print('Close the connection')
    writer.close()
    await writer.wait_closed()
    f = open('test_insert.csv', 'wb')
    f.write(data)
    f.close()

if __name__ == '__main__':
    f = bytes(1)
    asyncio.run(send_to_anonymizer_and_receive('127.0.0.1', 5556, f))
