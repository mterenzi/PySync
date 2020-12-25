from datetime import datetime
import stat
import zlib
from logger import Logger
from file_structure import File_Structure
from binascii import hexlify
from structure_comparer import Structure_Comparer
from threading import Thread
import socket
import ssl
import json
import gzip
from tempfile import gettempdir
import shutil
import os
from exceptions import *


class ServerThread(Thread):

    def __init__(self, conn, addr, server_struct, **kwargs):
        self.__conn = conn
        self.__server_struct = server_struct
        self.__root = kwargs['root']
        self.__configure(kwargs)
        self.__logger = Logger(self.__conf['logging'], self.__conf_path, addr[0], False, self.__conf['logging_limit'])
        Thread.__init__(self)
        
    def __configure(self, kwargs):
        home_dir = os.path.expanduser('~')
        stripped_root = os.path.basename(os.path.normpath(self.__root))
        conf_path = os.path.join(home_dir, '.conf')
        conf_path = os.path.join(conf_path, 'pysync')
        self.__conf_path = os.path.join(conf_path, stripped_root)
        os.makedirs(self.__conf_path, exist_ok=True)
        self.__conf = {
            'purge': kwargs.get('purge', False),
            'compression': kwargs.get('compression', 0),
            'compression_min': kwargs.get('compression_min', 1_000_000),
            'ram': kwargs.get('ram', 2048),
            'backup': kwargs.get('backup', False),
            'backup_path': kwargs.get('backup_path', 'DEFAULT'),
            'backup_limit': kwargs.get('backup_limit', 7),
            'key_size': kwargs.get('key_size', 128),
            'logging': kwargs.get('logging', 0),
            'logging_limit': kwargs.get('logging_limit', 1_000_000),
        }
        if self.__conf['backup']:
            if self.__conf['backup_path'] == 'DEFAULT':
                self.__conf['backup_path'] = os.path.join(self.__conf_path, 'backups')
            os.makedirs(self.__conf['backup_path'], exist_ok=True)

    def run(self):
        start_time = datetime.now()
        self.__sync_configs()
        self.__process()
        if self.__conf['backup_limit'] is not None:
            self.__purge_backups()
        time_elapsed = datetime.now() - start_time
        self.__logger.log(f'Time elapsed {time_elapsed}.', 2)

    def __sync_configs(self):
        self.__logger.log('Syncing configuration...', 2)
        data = self.__conn.recv(1024)
        client_conf = json.loads(data)
        self.__conf['purge'] = client_conf['purge'] and self.__conf['purge']
        client_conf['purge'] = self.__conf['purge']
        self.__conf['compression'] = client_conf['compression'] and self.__conf['compression']
        client_conf['compression'] = self.__conf['compression']
        if self.__conf['compression']:
            self.__conf['compression_min'] = max(
                min(client_conf['compression_min'], self.__conf['compression_min']), 0)
        client_conf['compression'] = self.__conf['compression_min']
        self.__conf['ram'] = min(client_conf['ram'], self.__conf['ram'])
        client_conf['ram'] = self.__conf['ram']

        conf_stream = json.dumps(client_conf).encode()
        self.__conn.sendall(conf_stream)
        data = self.__conn.recv(1024)
        if data != conf_stream:
            self.__logger.log('Configuration sync failed.', 1)
            raise MissSpeakException('CONF SYNC FAIL')
        self.__logger.log('Synced configured.', 2)

    def __process(self):
        self.__client_struct = self.__request_struct()
        comparer = Structure_Comparer(self.__server_struct.copy(),
                                      self.__client_struct.copy())
        creates, deletes = comparer.compare_structures(self.__conf['purge'])
        self.__logger.log('Syncing Server and Client...', 2)
        if creates is not None and deletes is not None:
            self.__handle_creates(creates)
            if self.__conf['purge']:
                self.__handle_deletes(deletes)
        self.__logger.log('Synced Server and Client.', 2)
        self.__conn.sendall(b'BYE')
        self.__conn.shutdown(socket.SHUT_RDWR)
        self.__conn.close()
        self.__logger.log('Connection with Client closed.', 2)

    def __request_struct(self):
        self.__logger.log('Requesting struct...', 2)
        self.__conn.sendall(b'REQUEST STRUCT')
        data = self.__conn.recv(1024)
        msg = data.decode('UTF-8').split(' ')
        if msg[0] == 'STRUCT':
            byte_total = msg[1]
            struct_confirm = f'OK STRUCT {byte_total}'
            self.__conn.sendall(struct_confirm.encode())
            struct = self.__recv_bytes(int(byte_total))
            self.__logger.log('Struct recieved.', 2)
            return json.loads(struct)
        else:
            raise MissSpeakException('STRUCT MissMatch')

    def __recv_bytes(self, byte_total):
        data = b''
        byte_chunk = self.__conf['ram']
        while len(data) < byte_total:
            if byte_chunk != -1:
                data += self.__conn.recv(byte_chunk)
            else:
                data += self.__conn.recv()
        if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
            data = zlib.decompress(data)
        return data

    def __handle_creates(self, creates):
        self.__logger.log('Handling creates...', 2)
        down, up = creates
        down_dirs, down_files = down
        self.__logger.log('Getting directories...', 2)
        self.__get_directories(down_dirs)
        self.__logger.log('Getting files...', 2)
        self.__get_files(down_files)

        up_dirs, up_files = up
        self.__logger.log('Sending directories...', 2)
        self.__send_directories(up_dirs)
        self.__logger.log('Sending files...', 2)
        self.__send_files(up_files)
        self.__logger.log('Creates handled.', 2)

    def __get_directories(self, dirs):
        for _dir in dirs:
            abs_path = _dir.replace('.', self.__root, 1)
            self.__logger.log(f'Creating directory {_dir}...', 4)
            last_mod = self.__client_struct[_dir]['last_mod']
            os.makedirs(abs_path, exist_ok=True)
            os.utime(abs_path, (last_mod, last_mod))

    def __get_files(self, files):
        for path in files:
            self.__download_file(path)

    def __download_file(self, path):
        req = f'REQUEST {path}'
        self.__conn.sendall(req.encode())

        data = self.__conn.recv(1024)
        info = json.loads(data)
        if info['path'] != path:
            raise MissSpeakException('REQUEST File')
        byte_total = info['bytes']
        ack = f"OK {byte_total}"
        self.__conn.sendall(ack.encode())
        self.__logger.log(f'Receiving file {path} {byte_total}...', 4)
        abs_path = path.replace('.', self.__root, 1)
        if byte_total > 0:
            if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
                self.__recv_compressed_file(abs_path, byte_total)
            else:
                self.__recv_file(abs_path, byte_total)
        else:
            open(abs_path, 'w+').close()
        last_mod = int(info['last_mod'])
        os.utime(abs_path, (last_mod, last_mod))

    def __recv_file(self, abs_path, byte_total):
        with open(abs_path, 'wb+') as file:
            byte_count = 0
            byte_chunk = self.__conf['ram']
            while byte_count < byte_total:
                if byte_chunk != -1:
                    data = self.__conn.recv(byte_chunk)
                else:
                    data = self.__conn.recv()
                file.write(data)
                byte_count += len(data)

    def __recv_compressed_file(self, abs_path, byte_total):
        file_name = f'{datetime.now().microsecond}_' + \
            os.path.basename(os.path.normpath(abs_path)) + '.gz'
        z_path = os.path.join(gettempdir(), file_name)
        with open(z_path, 'wb+') as zip_file:
            byte_count = 0
            byte_chunk = self.__conf['ram']
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

    def __send_directories(self, dirs):
        for _dir in dirs:
            abs_path = _dir.replace('.', self.__root, 1)
            cmd = f"MKDIR {_dir} {self.__server_struct[abs_path]['last_mod']}"
            self.__conn.sendall(cmd.encode())
            self.__logger.log(f'Sending directory {_dir}...', 4)

            data = self.__conn.recv(1024)
            if data == ('OK ' + cmd).encode():
                pass
            else:
                raise MissSpeakException('MKDIR MissMatch')

    def __send_files(self, up_files):
        for path in up_files:
            abs_path = path.replace('.', self.__root, 1)
            info = self.__server_struct[abs_path]
            info['path'] = path
            byte_total = os.path.getsize(abs_path)
            compressed = False
            if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
                byte_total, abs_path = self.__compress(abs_path)
                compressed = True
            info['bytes'] = byte_total
            cmd = f"MKFILE {json.dumps(info)}"
            self.__conn.sendall(cmd.encode())

            data = self.__conn.recv(1024)
            client_ack = f"OK MKFILE {path} {byte_total}"
            if data == client_ack.encode():
                self.__logger.log(f'Sending file {path} {byte_total}...', 4)
                if byte_total > 0:
                    bytes_read = 0
                    with open(abs_path, 'rb') as f:
                        byte_chunk = self.__conf['ram']
                        while bytes_read < byte_total:
                            if byte_chunk != -1:
                                file_bytes = f.read(byte_chunk)
                            else:
                                file_bytes = f.read()
                            self.__conn.sendall(file_bytes)
                            bytes_read += len(file_bytes)
                    if compressed:
                        os.remove(abs_path)
                data = self.__conn.recv(1024)
                if data != b'OK':
                    raise MissSpeakException('MKFILE ACK FINAL')
            else:
                raise MissSpeakException('MKFILE ACK')

    def __handle_deletes(self, deletes):
        self.__logger.log('Handling deletes...', 2)
        down, up = deletes
        down_dirs, down_files = down
        self.__logger.log('Deleting Server side...', 2)
        self.__down_deletes(down_dirs, down_files)

        up_dirs, up_files = up
        self.__logger.log('Deleting Client side...', 2)
        self.__up_deletes(up_dirs, up_files)
        self.__logger.log('Deletes handled.', 2)

    def __down_deletes(self, down_dirs, down_files):
        for _dir in down_dirs:
            if self.__confirm_delete(_dir):
                abs_file = _dir.replace('.', self.__root, 1)
                try:
                    if not self.__conf['backup']:
                        shutil.rmtree(abs_file)
                    else:
                        backup_path = _dir.replace('.', self.__conf['backup_path'], 1)
                        shutil.move(abs_file, backup_path)
                except FileNotFoundError:
                    pass
                except PermissionError:
                    pass
        for file in down_files:
            if self.__confirm_delete(file):
                abs_file = file.replace('.', self.__root, 1)
                try:
                    if not self.__conf['backup']:
                        os.remove(abs_file)
                    else:
                        backup_path = file.replace('.', self.__conf['backup_path'], 1)
                        shutil.move(abs_file, backup_path)
                except FileNotFoundError:
                    pass
                except PermissionError:
                    pass

    def __confirm_delete(self, path):
        req = f'CONFIRM DELETE {path}'.encode()
        self.__conn.sendall(req)
        data = self.__conn.recv(1024)
        if data == f'OK {path}'.encode():
            self.__logger.log(f'Delete {path} confirmed.', 3)
            return True
        elif data == f'NO {path}'.encode():
            self.__logger.log(f'Delete {path} denied.', 3)
            return False
        else:
            raise MissSpeakException('CONFIRM DELETE')

    def __up_deletes(self, up_dirs, up_files):
        deletes = up_dirs + up_files
        for path in deletes:
            abs_path = path.replace('.', self.__root, 1)
            if not os.path.exists(abs_path):
                cmd = f'DELETE {path}'
                self.__conn.sendall(cmd.encode())
                self.__logger.log(f'Sending {cmd}', 3)

                data = self.__conn.recv(1024)
                if data != b'OK':
                    raise MissSpeakException('DELETE')

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
                    shutil.rmtree(_dir)
            for file in files:
                status = os.stat(file)
                last_mod = datetime.fromtimestamp(status[stat.ST_MTIME])
                delta = now_time - last_mod
                if delta.days >= day_limit:
                    os.remove(file)
        self.__logger.log('Backups cleaned...', 2)

    def __compress(self, path):
        file_name = f'{datetime.now().microsecond}_' + \
            os.path.basename(os.path.normpath(path)) + '.gz'
        with open(path, 'rb') as f_in:
            z_path = os.path.join(gettempdir(), file_name)
            with gzip.open(z_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return os.path.getsize(z_path), z_path
    
    def terminate(self):
        self.__conn.shutdown(socket.SHUT_RDWR)
        self.__conn.close()
        super.terminate()
