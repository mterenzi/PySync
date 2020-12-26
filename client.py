import json
import socket
import ssl
import os
import shutil
import re
import stat
from tempfile import gettempdir
import zlib
import gzip
import uuid
from datetime import datetime
from logger import Logger
from exceptions import *
from file_structure import File_Structure
import time



class Client:

    def __init__(self, struct, conf):
        self.__struct = struct
        self.__hostname = conf.get('hostname')
        self.__port = conf.get('port')
        self.__root = conf.get('root')
        self.__cert = conf.get('cert', None)
        self.__configure(conf)
        self.__logger = Logger(self.__conf['logging'], self.__conf_path, self.__hostname, self.__conf['logging_limit'])
        self.__dir_mods = []
    
    def __configure(self, conf):
        home_dir = os.path.expanduser('~')
        stripped_root = os.path.basename(os.path.normpath(self.__root))
        conf_path = os.path.join(home_dir, '.conf')
        conf_path = os.path.join(conf_path, 'pysync')
        self.__conf_path = os.path.join(conf_path, stripped_root)
        os.makedirs(self.__conf_path, exist_ok=True)
        self.__conf = {
            'purge': conf['purge'],
            'encryption': conf['encryption'],
            'compression': conf['compression'],
            'compression_min': conf['compression_min'],
            'ram': conf['ram'],
            'backup': conf['backup'],
            'backup_path': conf['backup_path'],
            'backup_limit': conf['backup_limit'],
            'logging': conf['logging'],
            'logging_limit': conf['logging_limit'],
            'MAC': ':'.join(re.findall('..', '%012x' % uuid.getnode())),
        }
        if self.__conf['backup']:
            if self.__conf['backup_path'] == 'DEFAULT':
                self.__conf['backup_path'] = os.path.join(self.__conf_path, 'backups')
            os.makedirs(self.__conf['backup_path'], exist_ok=True)

    def run(self):
        start_time = datetime.now()
        try:
            self.__conn = self.__connect()
            self.__sync_config()
            self.__process()
            self.__conn.shutdown(socket.SHUT_RDWR)
            self.__conn.close()
            self.__logger.log('Connection with Server closed.', 2)
        except socket.timeout:
            self.__logger.log('Connection timeout with Server.', 1)
        finally:
            if self.__conf['backup_limit'] is not None:
                self.__purge_backups()
            self.__timeshift_dirs()
            time_elapsed = datetime.now() - start_time
            self.__logger.log(f'Time elapsed {time_elapsed}.', 2)

    def __connect(self):
        self.__logger.log('Connecting to Server...', 2)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self.__conf['encryption']:
            context = ssl.create_default_context()
            context.load_verify_locations(self.__cert)
            sock = context.wrap_socket(sock, server_hostname=self.__hostname)
        sock.connect((self.__hostname, self.__port))
        print(f'Connected to Server ({self.__hostname}, {self.__port})')
        self.__logger.log('Connected to Server.', 2)
        return sock

    def __sync_config(self):
        self.__logger.log('Syncing configuration...', 2)
        conf_stream = json.dumps(self.__conf).encode()
        self.__conn.sendall(conf_stream)
        data = self.__conn.recv(1024)
        self.__conf = json.loads(data)
        self.__conn.sendall(data)
        self.__logger.log('Sync configured.', 2)

    def __process(self):
        data = b'OPEN'
        while data != b'BYE':
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
            elif data != b'BYE':
                self.__conn.sendall(b'RETRY')

    def __send_struct(self):
        self.__logger.log('Sending struct...', 2)
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
            self.__logger.log('Struct sent.', 2)
        else:
            self.__logger.log('Send structure error', 1)
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
                self.__logger.log(f'Sending file {path} {byte_total}...', 4)
                with open(abs_path, 'rb') as f:
                    byte_chunk = min(self.__conf['ram'], byte_total)
                    while bytes_read < byte_total:
                        if byte_chunk != -1:
                            file_bytes = f.read(byte_chunk)
                        else:
                            file_bytes = f.read()
                        self.__conn.sendall(file_bytes)
                        bytes_read += byte_chunk
                if compressed:
                    os.remove(abs_path)
        else:
            self.__logger.log('File send error', 1)
            raise MissSpeakException('REQUEST File')

    def __get_directory(self, msg):
        msg = msg.decode('UTF-8')
        msg_parts = msg.split(' ')
        last_mod = int(msg_parts[-1])
        dir_path = ' '.join(msg_parts[:-1])
        abs_path = dir_path.replace('.', self.__root, 1)
        os.makedirs(abs_path, exist_ok=True)
        os.utime(abs_path, (last_mod, last_mod))
        self.__dir_mods.append((abs_path, (last_mod, last_mod)))
        ack = 'OK MKDIR ' + msg
        self.__conn.sendall(ack.encode())
        self.__logger.log(f'Recieved directory {dir_path}', 4)

    def __get_file(self, msg):
        info = json.loads(msg)
        path = info['path']
        abs_path = path.replace('.', self.__root, 1)
        ack = f"OK MKFILE {info['path']} {info['bytes']}"
        self.__conn.sendall(ack.encode())

        byte_total = int(info['bytes'])
        self.__logger.log(f'Receiving file {path} {byte_total}...', 4)
        if byte_total > 0:
            if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
                self.__recv_compressed_file(abs_path, byte_total)
            else:
                self.__recv_file(abs_path, byte_total)
        else:
            open(abs_path, 'wb+').close()
        last_mod = int(info['last_mod'])
        os.utime(abs_path, (last_mod, last_mod))
        self.__conn.sendall(b'OK')

    def __recv_file(self, abs_path, byte_total):
        with open(abs_path, 'wb+') as file:
            byte_count = 0
            byte_chunk = min(self.__conf['ram'], byte_total)
            while byte_count < byte_total:
                if byte_chunk != -1:
                    data = self.__conn.recv(byte_chunk)
                else:
                    data = self.__conn.recv()
                file.write(data)
                byte_count += len(data)
    
    def __recv_compressed_file(self, abs_path, byte_total):
        file_name = f'{datetime.now().microsecond}_' + os.path.basename(os.path.normpath(abs_path)) + '.gz'
        z_path = os.path.join(gettempdir(), file_name)
        with open(z_path, 'wb+') as zip_file:
            byte_count = 0
            byte_chunk = min(self.__conf['ram'], byte_total)
            while byte_count < byte_total:
                if byte_chunk != -1:
                    data = self.__conn.recv(byte_chunk)
                else:
                    data = self.__conn.recv()
                zip_file.write(data)
                byte_count += len(data)
        with gzip.open(z_path, 'rb+') as zip_file:
            with open(abs_path, 'wb+') as file:
                shutil.copyfileobj(zip_file, file)

    def __delete_down(self, msg):
        path = msg.decode('UTF-8')
        abs_path = path.replace('.', self.__root, 1)
        try:
            self.__logger.log(f'Deleting {path}...', 3)
            if not self.__conf['backup']:
                if os.path.isdir(path):
                    shutil.rmtree(abs_path)
                else:
                    os.remove(abs_path)
            else:
                backup_path = path.replace('.', self.__conf['backup_path'], 1)
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
            self.__logger.log(f'Delete {path} confirmed.', 3)
        else:
            deny = f'NO {path}'.encode()
            self.__conn.sendall(deny)
            self.__logger.log(f'Delete {path} denied.', 3)

    def __purge_backups(self):
        self.__logger.log('Cleaning backups...', 2)
        day_limit = self.__conf['backup_limit']
        now_time = datetime.now()
        for _, dirs, files in os.walk(self.__conf['backup_path'], topdown=True):
            for _dir in dirs:
                status = os.stat(_dir)
                last_mod = datetime.fromtimestamp(status[stat.ST_MTIME])
                delta = now_time - last_mod
                if delta.days >= day_limit:
                    try:
                        shutil.rmtree(_dir)
                    except IOError:
                        continue
            for file in files:
                status = os.stat(file)
                last_mod = datetime.fromtimestamp(status[stat.ST_MTIME])
                delta = now_time - last_mod
                if delta.days >= day_limit:
                    try:
                        os.remove(file)
                    except IOError:
                        continue
        self.__logger.log('Backups cleaned...', 2)

    def __compress(self, path):
        self.__logger.log(f'Compressing {path}...', 4)
        file_name = f'{datetime.now().microsecond}_' + os.path.basename(os.path.normpath(path)) + '.gz'
        with open(path, 'rb') as f_in:
            z_path = os.path.join(gettempdir(), file_name)
            with gzip.open(z_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return os.path.getsize(z_path), z_path
    
    def __timeshift_dirs(self):
        for abs_path, mod in self.__dir_mods:
            os.utime(abs_path, mod)


def client_start(conf):
    # socket.setdefaulttimeout(conf['timeout'])
    structure = File_Structure(conf['root'], conf['gitignore'], conf['purge_limit'])
    tries = 0
    while True:
        structure.update_structure()
        structure.save_structure()
        try:
            client = Client(structure, conf.copy())
            client.run()
            tries = 0
        except ConnectionRefusedError:
            seconds = 30*tries
            print(f'Connection refused. Sleeping for {seconds} seconds')
            time.sleep(seconds)
            tries += 1
            tries = min(tries, 30)
            continue
        except ConnectionResetError:
            pass

        if conf['sleep_time'] == -1:
            break
        time.sleep(conf['sleep_time'])