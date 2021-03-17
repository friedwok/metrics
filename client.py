import socket
import time
import bisect

class ClientError(Exception):
	pass


class Client:

	def __init__(self, host, port, timeout=None):
		self.host = host
		self.port = port
		self.timeout = timeout

		try:
			self.connection = socket.create_connection(
				(host, port), timeout
			)
		except socket.error as err:
			raise ClientError("Can't create connection", err)

	def _read(self):
		data = b""
		try:
			while not data.endswith(b"\n\n"):
				data += self.connection.recv(1024)
		except socket.error as err:
			raise ClientError("Read from socket error", err)

		return data.decode("utf-8")

	def _send(self, data):
		try:
			self.connection.sendall(data)
		except socket.error as err:
			raise ClientError(
				"Error sending data to the server",
				err
			)

	def put(self, name, value, timestamp=None):
		if not timestamp:
			timestamp = int(time.time())
		data = 'put ' + name + ' ' + str(value) + ' ' + str(timestamp) + '\n'
		self._send(data.encode("utf-8"))
		serv_echo = self._read().split('\n')
		if(serv_echo[0] == 'error'):
			raise ClientError(serv_echo[1])

	def get(self, key):
		self._send(f"get {key}\n".encode())
		raw_data = self._read()
		#print(f'Raw data inside get: {raw_data}')
		data = {}
		status, payload = raw_data.split('\n', 1)
		payload = payload.strip()

		if status != 'ok':
			raise ClientError('Server returns an error')

		if payload == '':
			return data

		try:
			for row in payload.splitlines():
				#print(f'Row: {row}')
				key, value, timestamp = row.split()
				if key not in data:
					data[key] = []
				bisect.insort(data[key], (int(timestamp), float(value)))
		except Exception as err:
			raise ClientError('Server returns invalid data', err)

		return data

	def close(self):
		try:
			self.connection.close()
		except socket.error as err:
			raise ClientError("Error with closing connection", err)

