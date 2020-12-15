from datetime import datetime
import stat
import zlib
from logger import Logger
from file_structure import File_Structure
from binascii import hexlify
from structure_comparer import Structure_Comparer
import socket
import ssl
import json
import gzip
from tempfile import gettempdir
import shutil
import os
from exceptions import *


class ServerThread:

    def __init__(self, conn, addr, server_struct, **kwargs):
        self.__conn = conn
        self.__server_struct = server_struct
        self.__root = self.__server_struct['root']

        self.__conf = {
            'purge': kwargs.get('purge', False),
            'compression': kwargs.get('compression', 0),
            'compression_min': kwargs.get('compression_min', 1_000_000),
            'ram': kwargs.get('ram', 2048),
            'authenticate': kwargs.get('authenticate', False),
            'backup': kwargs.get('backup', False),
            'backup_limit': kwargs.get('backup_limit', 7),
            'key_size': kwargs.get('key_size', 128),
            'logging': kwargs.get('logging', False),
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

        self.__logger = Logger(self.__conf_path, addr, True) if self.__conf['logging'] else None

    def run(self):
        start_time = datetime.now()
        self.__sync_configs()
        self.__process()
        self.__purge_backups
        time_elapsed = datetime.now() - start_time
        self.__log(f'Time elapsed {time_elapsed}.')

    def __sync_configs(self):
        self.__log('Syncing configuration...')
        data = self.__conn.recv(1024)
        client_conf = json.loads(data)
        self.__client_mac = client_conf['MAC']
        if self.__conf['authenticate']:
            self.__log('Authenticating...')
            self.__log(f'Client reported MAC: {self.__client_mac}')
            if client_conf['AUTH_KEY'] != self.__get_auth_key(self.__client_mac):
                self.__log('Authentication failed.')
                raise PermissionError
            self.__log('Authenticated')
        self.__conf['purge'] = client_conf['purge'] and self.__conf['purge']
        client_conf['purge'] = self.__conf['purge']
        self.__conf['compression'] = max(min(client_conf['compression'], self.__conf['compression']), 0)
        client_conf['compression'] = self.__conf['compression']
        self.__conf['compression_min'] = max(min(client_conf['compression_min'], self.__conf['compression_min']), 0)
        client_conf['compression'] = self.__conf['compression_min']
        self.__conf['ram'] = min(client_conf['ram'], self.__conf['ram'])
        client_conf['ram'] = self.__conf['ram']

        conf_stream = json.dumps(client_conf).encode()
        self.__conn.sendall(conf_stream)
        data = self.__conn.recv(1024)
        if data != conf_stream:
            self.__log('Configuration sync failed.')
            raise MissSpeakException('CONF SYNC FAIL')
        self.__log('Synced configured.')
        
    def __process(self):
        self.__client_struct = self.__request_struct()
        comparer = Structure_Comparer(self.__server_struct.copy(),
                                      self.__client_struct.copy())
        creates, deletes = comparer.compare_structures(self.__conf['purge'])
        self.__log('Syncing Server and Client...')
        if creates is not None and deletes is not None:
            self.__handle_creates(creates)
            if self.__conf['purge']:
                self.__handle_deletes(deletes)
        self.__log('Synced Server and Client.')
        if self.__conf['authenticate']:
            self.__log('Sending new key to Client.')
            self.__key_shake()
            self.__log('Key send and received.')
        else:
            self.__conn.sendall(b'BYE')
            self.__conn.shutdown(socket.SHUT_RDWR)
            self.__conn.close()
        self.__log('Connection with Client closed.')

    def __request_struct(self):
        self.__log('Requesting struct...')
        self.__conn.sendall(b'REQUEST STRUCT')
        data = self.__conn.recv(1024)
        msg = data.decode('UTF-8').split(' ')
        if msg[0] == 'STRUCT':
            byte_total = msg[1]
            struct_confirm = f'OK STRUCT {byte_total}'
            self.__conn.sendall(struct_confirm.encode())
            struct = self.__recv_bytes(int(byte_total))
            self.__log('Struct recieved.')
            return json.loads(struct)
        else:
            raise MissSpeakException('STRUCT MissMatch')

    def __recv_bytes(self, byte_total):
        byte_count = 0
        data = None
        byte_chunk = self.__conf['ram']
        while byte_count < byte_total:
            if data is None:
                data = self.__conn.recv(byte_chunk)
            else:
                data += self.__conn.recv(byte_chunk)
            byte_count = len(data)
        if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
            data = zlib.decompress(data)
        return data

    def __handle_creates(self, creates):
        self.__log('Handling creates...')
        down, up = creates
        down_dirs, down_files = down
        self.__log('Getting directories...')
        self.__get_directories(down_dirs)
        self.__log('Getting files...')
        self.__get_files(down_files)

        up_dirs, up_files = up
        self.__log('Sending directories...')
        self.__send_directories(up_dirs)
        self.__log('Sending files...')
        self.__send_files(up_files)
        self.__log('Creates handled.')

    def __get_directories(self, dirs):
        for _dir in dirs:
            abs_path = _dir.replace('.', self.__root, 1)
            self.__log(f'Creating directory {_dir}...')
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
        self.__log(f'Receiving file {path} {byte_total}...')
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

    def __send_directories(self, dirs):
        for _dir in dirs:
            abs_path = _dir.replace('.', self.__root, 1)
            cmd = f"MKDIR {_dir} {self.__server_struct[abs_path]['last_mod']}"
            self.__conn.sendall(cmd.encode())
            self.__log(f'Sending directory {_dir}...')

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
                self.__log(f'Sending file {path} {byte_total}...')
                if byte_total > 0:
                    bytes_read = 0
                    with open(abs_path, 'rb') as f:
                        byte_chunk = self.__conf['ram']
                        while bytes_read < byte_total:
                            file_bytes = f.read(byte_chunk)
                            self.__conn.sendall(file_bytes)
                            bytes_read += byte_chunk
                    if compressed:
                        os.remove(abs_path)
                data = self.__conn.recv(1024)
                if data != b'OK':
                    raise MissSpeakException('MKFILE ACK FINAL')
            else:
                raise MissSpeakException('MKFILE ACK')

    def __handle_deletes(self, deletes):
        self.__log('Handling deletes...')
        down, up = deletes
        down_dirs, down_files = down
        self.__log('Deleting Server side...')
        self.__down_deletes(down_dirs, down_files)

        up_dirs, up_files = up
        self.__log('Deleting Client side...')
        self.__up_deletes(up_dirs, up_files)
        self.__log('Deletes handled.')

    def __down_deletes(self, down_dirs, down_files):
        for _dir in down_dirs:
            if self.__confirm_delete(_dir):
                abs_file = _dir.replace('.', self.__root, 1)
                try:
                    if not self.__conf['backup']:
                        shutil.rmtree(abs_file)
                    else:
                        backup_path = _dir.replace('.', self.__backup_path, 1)
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
                        backup_path = file.replace('.', self.__backup_path, 1)
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
            self.__log(f'Delete {path} confirmed.')
            return True
        elif data == f'NO {path}'.encode():
            self.__log(f'Delete {path} denied.')
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
                self.__log(f'Sending {cmd}')

                data = self.__conn.recv(1024)
                if data != b'OK':
                    raise MissSpeakException('DELETE')

    def __get_auth_key(self, mac):
        self.__auth_path = os.path.join(self.__conf_path, 'keys_s.json')
        if os.path.exists(self.__auth_path):
            with open(self.__auth_path, 'r') as file:
                keys = json.load(file)
                return keys.get(self.__client_mac, None)
        else:
            return None

    def __key_shake(self):
        new_key =  hexlify(os.urandom(self.__conf['key_size'] // 2)).decode('UTF-8')
        key_msg = f'BYE {new_key}'.encode()
        self.__conn.sendall(key_msg)
        
        data = self.__conn.recv(1024)
        if data == key_msg:
            if os.path.exists(self.__auth_path):
                with open(self.__auth_path, 'r') as file:
                    keys = json.load(file)
                keys[self.__client_mac] = new_key
            else:
                keys = {self.__client_mac: new_key}
            with open(self.__auth_path, 'w+') as file:
                json.dump(keys, file, indent=4)
        else:
            raise MissSpeakException('AUTH KEY NOT CONFIRMED')

    def __purge_backups(self):
        self.__log('Cleaning backups...')
        day_limit = self.__conf['backup_limit']
        now_time = datetime.now()
        for _, dirs, files in os.walk(self.__backup_path, topdown=True):
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
        self.__log('Backups cleaned...')

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


def main(hostname, port, path, config_name=None):
    if config_name is not None:
        if not os.path.exists(config_name):
            home_dir = os.path.expanduser('~')
            conf_path = os.path.join(home_dir, '.conf')
            conf_path = os.path.join(conf_path, 'pysync')
            config_name = os.path.join(conf_path, config_name)
            if not os.path.exists(config_name):
                raise FileNotFoundError('Config file not found')
        with open(config_name, 'r') as file:
            conf = json.load(file)
    else:
        conf = {
            'gitignore': False,
            'purge_limit': 30,
            'encryption': False,
        }

    structure = File_Structure(path, conf['gitignore'], conf['purge_limit'])

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((hostname, port))
        sock.listen(5)
        if True:
            ssock = sock
            if True:
                conn, addr = ssock.accept()
                print(f'Connected to {addr}')
                if conf['encryption']:
                    context = ssl.create_default_context()
                    conn = context.wrap_socket(conn, server_side=True)
                st = ServerThread(conn, addr, structure.get_structure(), compression=6, authenticate=True, purge=True, backup=True, logging=True, **conf)
                st.run()
                structure.update_structure()
                structure.save_structure()


if __name__ == "__main__":
    hostname = 'localhost'
    port = 1818
    path = '.\\folder1'
    main(hostname, port, path)
