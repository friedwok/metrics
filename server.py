import asyncio


commands = ['put', 'get']
err_data = 'error\nwrong command\n\n'
metrics = dict()

def process_data(data):
	raw_data = data.split(' ')
	if raw_data[0] not in commands:
		return err_data

	if raw_data[0] == 'get':
		if len(raw_data) == 2:
			return process_get(raw_data)
		else:
			return err_data
	elif raw_data[0] == 'put':
		if len(raw_data) == 4:
			try:
				process_put(raw_data)
			except:
				return err_data
			return 'ok\n\n'
		else:
			return err_data


def process_get(raw_data):
	resp = 'ok\n'
	name = raw_data[1]
	name = name.strip()

	if name == '*':
		for key in metrics.keys():
			for elem in metrics[key]:
				resp += key + ' ' + str(elem[0]) + ' ' + str(elem[1]) + '\n'
		return resp + '\n'

	if name not in metrics.keys():
		pass
	else:
		for elem in metrics[name]:
			resp += name + ' ' + str(elem[0]) + ' ' + str(elem[1]) + '\n'
	return resp + '\n'


def process_put(raw_data):
	if raw_data[1] not in metrics.keys():
		metrics[raw_data[1]] = []
	if not metrics[raw_data[1]]:
		metrics[raw_data[1]].append((float(raw_data[2]), int(raw_data[3].strip('\n'))))
		return

	if metrics[raw_data[1]][-1][1] != int(raw_data[3].strip('\n')):
		metrics[raw_data[1]].append((float(raw_data[2]), int(raw_data[3].strip('\n'))))
	else:
		metrics[raw_data[1]][-1] = (float(raw_data[2]), int(raw_data[3].strip('\n')))


class ClientServerProtocol(asyncio.Protocol):
	def connection_made(self, transport):
		self.transport = transport

	def data_received(self, data):
		resp = process_data(data.decode())
		self.transport.write(resp.encode())

def run_server(host, port):
	loop = asyncio.get_event_loop()
	coro = loop.create_server(
		ClientServerProtocol,
		host, port
	)

	server = loop.run_until_complete(coro)

	try:
		loop.run_forever()
	except KeyboardInterrupt:
		pass

	server.close()
	loop.run_until_complete(server.wait_closed())
	loop.close()

run_server('127.0.0.1', 8888)
