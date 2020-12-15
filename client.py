import json
import socket
import ssl
import os
import shutil
import re
from tempfile import gettempdir
import zlib
import gzip
import uuid
from datetime import datetime
from logger import Logger
from exceptions import *
from file_structure import File_Structure



class Client:

    def __init__(self, hostname, port, struct, **kwargs):
        self.__hostname = hostname
        self.__port = port
        self.__struct = struct
        self.__root = struct.get_root()

        self.__conf = {
            'purge': kwargs.get('purge', False),
            'encrypt': kwargs.get('encrypt', False),
            'compression': kwargs.get('compression', 0),
            'compression_min': kwargs.get('compression_min', 1_000_000),
            'ram': kwargs.get('ram', 2048),
            'authenticate': kwargs.get('authenticate', False),
            'backup': kwargs.get('backup', False),
            'logging': kwargs.get('logging', False),
            'MAC': ':'.join(re.findall('..', '%012x' % uuid.getnode())),
        }

        home_dir = os.path.expanduser('~')
        stripped_root = os.path.basename(os.path.normpath(self.__root))
        conf_path = os.path.join(home_dir, '.conf')
        conf_path = os.path.join(conf_path, 'pysync')
        self.__conf_path = os.path.join(conf_path, stripped_root)
        os.makedirs(self.__conf_path, exist_ok=True)

        if self.__conf['backup']:
            self.__backup_path = os.path.join(self.__conf_path, 'backups')
            os.makedirs(self.__backup_path, exist_ok=True)

        self.__logger = Logger(self.__conf_path, hostname, False) if self.__conf['logging'] else None
        self.__conf['AUTH_KEY'] = self.__get_authkey() if self.__conf['authenticate'] else None

    def __get_authkey(self):
        self.__auth_path = os.path.join(self.__conf_path, 'keys_c.json')
        if os.path.exists(self.__auth_path):
            with open(self.__auth_path, 'r') as file:
                keys = json.load(file)
                return keys[self.__hostname]

    def run(self):
        start_time = datetime.now()
        self.__conn = self.__connect()
        self.__sync_config()
        self.__process()
        self.__conn.shutdown(socket.SHUT_RDWR)
        self.__conn.close()
        self.__log('Connection with Server closed.')
        time_elapsed = datetime.now() - start_time
        self.__log(f'Time elapsed {time_elapsed}.')

    def __connect(self):
        self.__log('Connecting to Server...')
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.__conf['encrypt']:
            context = ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=self.__hostname)
        sock.connect((self.__hostname, self.__port))
        self.__log('Connected to Server.')
        return sock

    def __sync_config(self):
        self.__log('Syncing configuration...')
        conf_stream = json.dumps(self.__conf).encode()
        self.__conn.sendall(conf_stream)
        data = self.__conn.recv(1024)
        self.__conf = json.loads(data)
        self.__conn.sendall(data)
        self.__log('Sync configured.')

    def __process(self):
        data = b'OPEN'
        while data[0:3] != b'BYE':
            data = self.__conn.recv(1024)
            if data == b'REQUEST STRUCT':
                self.__send_struct()
            elif data[0:7] == b'REQUEST':
                self.__send_files(data[8:].decode('UTF-8'))
            elif data[0:5] == b'MKDIR':
                self.__get_directory(data[6:])
            elif data[0:6] == b'MKFILE':
                self.__get_file(data[7:])
            elif data[0:6] == b'DELETE':
                self.__delete_down(data[7:])
            elif data[0:14] == b'CONFIRM DELETE':
                self.__confirm_delete(data[15:])
        if self.__conf['authenticate']:
            new_auth_key = data[4:]
            self.__save_auth_key(new_auth_key)
            self.__conn.sendall(data)
            self.__log('New key recieved.')

    def __send_struct(self):
        self.__log('Sending struct...')
        struct_bytes = self.__struct.dump_structure().encode('UTF-8')
        if self.__conf['compression'] and len(struct_bytes) >= self.__conf['compression_min']:
            struct_bytes = zlib.compress(struct_bytes, level=self.__conf['compression'])
        struct_info = f'STRUCT {len(struct_bytes)}'
        b_struct_info = struct_info.encode('UTF-8')
        self.__conn.sendall(b_struct_info)
        data = self.__conn.recv(1024)
        msg = data.decode('UTF-8')
        if msg == f'OK {struct_info}':
            self.__conn.sendall(struct_bytes)
            self.__log('Struct sent.')
        else:
            raise MissSpeakException('STRUCT MissMatch')

    def __send_files(self, path):
        abs_path = path.replace('.', self.__root, 1)
        info = self.__struct.get_structure()[abs_path]
        info['path'] = path
        byte_total = os.path.getsize(abs_path)
        compressed = False
        if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
                byte_total, abs_path = self.__compress(abs_path)
                compressed = True
        info['bytes'] = byte_total
        info_stream = json.dumps(info).encode()
        self.__conn.sendall(info_stream)

        data = self.__conn.recv(1024)
        if data == f'OK {byte_total}'.encode():
            if byte_total > 0:
                bytes_read = 0
                self.__log(f'Sending file {path} {byte_total}...')
                with open(abs_path, 'rb') as f:
                    byte_chunk = self.__conf['ram']
                    while bytes_read < byte_total:
                        file_bytes = f.read(byte_chunk)
                        self.__conn.sendall(file_bytes)
                        bytes_read += byte_chunk
                if compressed:
                    os.remove(abs_path)
        else:
            raise MissSpeakException('REQUEST File')

    def __get_directory(self, msg):
        msg = msg.decode('UTF-8')
        msg_parts = msg.split(' ')
        last_mod = int(msg_parts[-1])
        dir_path = ' '.join(msg_parts[:-1])
        abs_path = dir_path.replace('.', self.__root, 1)
        os.makedirs(abs_path, exist_ok=True)
        os.utime(abs_path, (last_mod, last_mod))
        ack = 'OK MKDIR ' + msg
        self.__conn.sendall(ack.encode())
        self.__log(f'Recieved directory {dir_path}')

    def __get_file(self, msg):
        info = json.loads(msg)
        path = info['path']
        abs_path = path.replace('.', self.__root, 1)
        ack = f"OK MKFILE {info['path']} {info['bytes']}"
        self.__conn.sendall(ack.encode())

        byte_total = int(info['bytes'])
        self.__log(f'Receiving file {path} {byte_total}...')
        if byte_total > 0:
            if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
                self.__recv_compressed_file(abs_path, byte_total)
            else:
                self.__recv_file(abs_path, byte_total)
        last_mod = int(info['last_mod'])
        os.utime(abs_path, (last_mod, last_mod))
        self.__conn.sendall(b'OK')

    def __recv_file(self, abs_path, byte_total):
        with open(abs_path, 'wb+') as file:
            byte_count = 0
            byte_chunk = self.__conf['ram']
            while byte_count < byte_total:
                data = self.__conn.recv(byte_chunk)
                file.write(data)
                byte_count += len(data)
    
    def __recv_compressed_file(self, abs_path, byte_total):
        file_name = f'{datetime.now().microsecond}_' + os.path.basename(os.path.normpath(abs_path)) + '.gz'
        z_path = os.path.join(gettempdir(), file_name)
        with open(z_path, 'wb+') as zip_file:
            byte_count = 0
            byte_chunk = self.__conf['ram']
            while byte_count < byte_total:
                data = self.__conn.recv(byte_chunk)
                zip_file.write(data)
                byte_count += len(data)
        with gzip.open(z_path, 'rb+') as zip_file:
            with open(abs_path, 'wb+') as file:
                shutil.copyfileobj(zip_file, file)

    def __delete_down(self, msg):
        path = msg.decode('UTF-8')
        abs_path = path.replace('.', self.__root, 1)
        try:
            self.__log(f'Deleting {path}...')
            if not self.__conf['backup']:
                if os.path.isdir(path):
                    shutil.rmtree(abs_path)
                else:
                    os.remove(abs_path)
            else:
                backup_path = path.replace('.', self.__backup_path, 1)
                shutil.move(abs_path, backup_path)
        except FileNotFoundError:
            pass
        except PermissionError:
            pass
        self.__conn.sendall(b'OK')

    def __confirm_delete(self, path):
        path = path.decode('UTF-8')
        abs_path = path.replace('.', self.__root, 1)
        if not os.path.exists(abs_path):
            ack = f'OK {path}'.encode()
            self.__conn.sendall(ack)
            self.__log(f'Delete {path} confirmed.')
        else:
            deny = f'NO {path}'.encode()
            self.__conn.sendall(deny)
            self.__log(f'Delete {path} denied.')

    def __save_auth_key(self, auth_key):
        self.__log('Receiving new key from Server...')
        auth_key = auth_key.decode('UTF-8')
        if os.path.exists(self.__auth_path):
            with open(self.__auth_path, 'r') as file:
                keys = json.load(file)
            keys[self.__hostname] = auth_key
        else:
            keys = {self.__hostname: auth_key}
        with open(self.__auth_path, 'w+') as file:
            json.dump(keys, file, indent=4)

    def __compress(self, path):
        file_name = f'{datetime.now().microsecond}_' + os.path.basename(os.path.normpath(path)) + '.gz'
        with open(path, 'rb') as f_in:
            z_path = os.path.join(gettempdir(), file_name)
            with gzip.open(z_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return os.path.getsize(z_path), z_path

    def __log(self, message):
        if self.__conf['logging']:
            self.__logger.log(message)


def get_conf(config_name):
    if not os.path.exists(config_name):
        home_dir = os.path.expanduser('~')
        conf_path = os.path.join(home_dir, '.conf')
        conf_path = os.path.join(conf_path, 'pysync')
        config_name = os.path.join(conf_path, config_name)
        if not os.path.exists(config_name):
            raise FileNotFoundError('Config file not found')
    with open(config_name, 'r') as file:
        conf = json.load(file)
    return conf


def main(hostname, port, path, config_name=None):
    if config_name is not None:
        conf = get_conf(config_name)
    else:
        conf = {
            'gitignore': False,
            'purge_limit': 7,
            'backup_limit': 7,
        }
    structure = File_Structure(path, conf['gitignore'], conf['purge_limit'])

    client = Client(hostname, port, structure, backup=True, compression=6, authenticate=True, purge=True, logging=True, **conf)
    client.run()

    structure.update_structure()
    structure.save_structure()


if __name__ == "__main__":
    hostname = 'localhost'
    port = 1818
    path = '.\\folder2'
    main(hostname, port, path)
