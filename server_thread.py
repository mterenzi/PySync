from datetime import datetime
import stat
import zlib
from logger import Logger
from structure_comparer import Structure_Comparer
from threading import Thread
import socket
import json
import gzip
from tempfile import gettempdir
import shutil
import os
import random
from exceptions import *
from thread_locker import File_Thread_Locker


class ServerThread(Thread):
    """
    Server Thread class. Responsible for handling independent client 
    connections.
    """

    def __init__(self, conn, addr, server_struct, conf):
        """
        Builds server thread. Call with start() method.

        Args:
            conn (Socket): Socket with active connection between client and
            server.
            addr (tuple): Tuple of IP and port of client.
            server_struct (dict): Local file structure dictionary.
            conf (dict): Configuration dictionary for server thread.
        """
        self.__conn = conn
        self.__server_struct = server_struct
        self.__root = conf['root']
        self.__configure(conf)

        Thread.__init__(self)
        self.setName(datetime.now().microsecond)
        self.setDaemon(True)
        
        self.__logger = Logger(self.__conf['logging'], self.__conf_path,
                                addr[0], self.__conf['logging_limit'],
                                thread=self.getName())
        self.__dir_mods = []
        
    def __configure(self, conf):
        """
        Additional configuration. Called within __init__.

        Args:
            conf (dict): Configuration dictionary.
        """
        home_dir = os.path.expanduser('~')
        stripped_root = os.path.basename(os.path.normpath(self.__root))
        conf_path = os.path.join(home_dir, '.conf')
        conf_path = os.path.join(conf_path, 'pysync')
        self.__conf_path = os.path.join(conf_path, stripped_root)
        os.makedirs(self.__conf_path, exist_ok=True)
        self.__conf = conf
        if self.__conf['backup']:
            if self.__conf['backup_path'] == 'DEFAULT':
                self.__conf['backup_path'] = os.path.join(self.__conf_path, 'backups')
            os.makedirs(self.__conf['backup_path'], exist_ok=True)

    def run(self):
        """
        Main function of started Server thread. Performs sync between client and
        server.
        """
        start_time = datetime.now()
        try:
            self.__sync_configs()
            self.__process()
            self.__conn.sendall(b'BYE')
        except socket.timeout:
            self.__logger.log('Connection timeout with client.', 1)
        except json.JSONDecodeError:
            self.__logger.log('Critical json decoding error', 1)
        except MissSpeakException:
            self.__logger.log('Critical miscommunication. Closing connection.',
                                1)
        except SkipResponseException:
            self.__logger.log('Client requested skip at incorrect time', 4)
        finally:
            try:
                self.__conn.shutdown(socket.SHUT_RDWR)
                self.__conn.close()
            except:
                pass
            self.__logger.log('Connection with Client closed.', 2)
            if self.__conf['backup_limit'] is not None:
                    self.__purge_backups()
            self.__timeshift_dirs()
            time_elapsed = datetime.now() - start_time
            self.__logger.log(f'Time elapsed {time_elapsed}.', 2)

    def __sync_configs(self):
        """
        Syncs configuration between Server and Client.

        Raises:
            MissSpeakException: Raised if configuration sync client
            acknowledgment fails.
        """
        self.__logger.log('Syncing configuration...', 2)
        data = self.__conn.recv(1024)
        client_conf = json.loads(data)
        self.__logger.log(f'Client MAC address: {client_conf["MAC"]}', 2)
        self.__conf['purge'] = client_conf['purge'] and self.__conf['purge']
        client_conf['purge'] = self.__conf['purge']
        self.__conf['compression'] = min(client_conf['compression'], self.__conf['compression'])
        client_conf['compression'] = self.__conf['compression']
        if self.__conf['compression']:
            self.__conf['compression_min'] = max(
                min(client_conf['compression_min'], self.__conf['compression_min']), 0)
        client_conf['compression_min'] = self.__conf['compression_min']
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
        """
        Performs file synchronization steps.
        """
        self.__client_struct = self.__request_struct()
        comparer = Structure_Comparer(self.__server_struct.copy(),
                                        self.__client_struct.copy())
        creates, deletes = comparer.compare_structures(self.__conf['purge'])
        self.__logger.log('Syncing Server and Client...', 2)
        if creates is not None and deletes is not None:
            self.__handle_creates(creates)
            if self.__conf['purge'] and deletes is not None:
                self.__handle_deletes(deletes)
        self.__logger.log('Synced Server and Client.', 2)

    def __request_struct(self, error_count=0):
        """
        Requests file structure dictionary from client.

        Raises:
            MissSpeakException: Raises if client is not prepared to send file
            structure dictionary

        Returns:
            dict: Client file structure dictionary.
        """
        self.__logger.log('Requesting struct...', 2)
        self.__conn.sendall(b'REQUEST STRUCT')
        data = self.__recv(1024, b'REQUEST STRUCT')
        msg = data.decode('UTF-8').split(' ')
        if msg[0] == 'STRUCT':
            byte_total = msg[1]
            struct_confirm = f'OK STRUCT {byte_total}'
            self.__conn.sendall(struct_confirm.encode())
            struct = self.__recv_bytes(int(byte_total))
            self.__logger.log('Struct recieved.', 2)
            try:
                return json.loads(struct)
            except json.JSONDecodeError:
                self.__logger.log('Json decode failed', 1)
                raise MissSpeakException('STRUCT MissMatch')
        else:
            error_count += 1
            if error_count >= 5:
                self.__logger.log('Struct missmatch', 1)
                raise MissSpeakException('STRUCT MissMatch')
            else:
                return self.__request_struct()

    def __recv_bytes(self, byte_total):
        """
        Handles incoming bytes from client.

        Args:
            byte_total (int): Total number of bytes expected to be received.

        Returns:
            bytes: Bytes received from client.
        """
        data = b''
        byte_chunk = min(self.__conf['ram'], byte_total)
        while len(data) < byte_total:
            if byte_chunk != -1:
                data += self.__conn.recv(byte_chunk)
            else:
                data += self.__conn.recv()
        if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
            data = zlib.decompress(data)
        return data

    def __handle_creates(self, creates):
        """
        Synchronizes creations locally and with the client.

        Args:
            creates (Tuple(Tuple(), Tuple())): Paths to be created.
            ((Down_Directories, Down_Files), (Up_Directories, Up_Files))
        """
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
        """
        Creates new local directories.

        Args:
            dirs (list): List of directories to create.
        """
        for _dir in dirs:
            abs_path = _dir.replace('.', self.__root, 1)
            self.__logger.log(f'Creating directory {_dir}...', 4)
            last_mod = self.__client_struct[_dir]['last_mod']
            try:
                os.makedirs(abs_path, exist_ok=True)
                os.utime(abs_path, (last_mod, last_mod))
                self.__dir_mods.append((abs_path, (last_mod, last_mod)))
            except PermissionError:
                self.__logger.log('Permssion error encountered creating '
                                    + f'directory {_dir}. Skipping...', 1)
            except:
                self.__logger.log('Unknown error encountered creating '
                                    + f'directory {_dir}. Skipping...', 1)

    def __get_files(self, files):
        """
        Retrieves files from client.

        Args:
            files (list(str)): List of file paths to retrieve.
        """
        error_count = 0
        for path in files:
            try:
                self.__download_file(path)
                error_count = 0
            except MissSpeakException as error:
                error_count += 1
                if error_count >= 5:
                    raise error
            except SkipResponseException:
                self.__logger.log('Client requested to directory creation.', 4)

    def __download_file(self, path):
        """
        Requests file from client.

        Args:
            path (str): File path to download.

        Raises:
            MissSpeakException: Client has not successfully acknowledged file
            request
        """
        req = f'REQUEST {path}'
        self.__conn.sendall(req.encode())
    
        data = self.__recv(1024, req.encode())
        info = json.loads(data)
        if info['path'] != path:
            self.__logger.log('File request error', 1)
            raise MissSpeakException('REQUEST File')
        byte_total = info['bytes']
        ack = f"OK {byte_total}"
        self.__conn.sendall(ack.encode())
        self.__logger.log(f'Receiving file {path} {byte_total}...', 4)
        abs_path = path.replace('.', self.__root, 1)
        if byte_total > 0:
            try:
                if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
                    self.__recv_compressed_file(abs_path, byte_total)
                else:
                    self.__recv_file(abs_path, byte_total)
            except PermissionError:
                self.__conn.sendall(b'!!SKIP!!SKIP!!')
                self.__logger.log('Permssion error encountered creating '
                                    + f'file {abs_path}. Skipping...', 1)
            except:
                self.__conn.sendall(b'!!SKIP!!SKIP!!')
                self.__logger.log('Unknown error encountered creating '
                                    + f'file {abs_path}. Skipping...', 1)
        else:
            with File_Thread_Locker(abs_path):
                try:
                    open(abs_path, 'w+').close()
                except PermissionError:
                    self.__logger.log('Permssion error encountered creating '
                                        + f'file {abs_path}. Skipping...', 1)
                except:
                    self.__logger.log('Unknown error encountered creating '
                                        + f'file {abs_path}. Skipping...', 1)
        last_mod = int(info['last_mod'])
        try:
            os.utime(abs_path, (last_mod, last_mod))
        except PermissionError:
            self.__logger.log(
                'Permission error updating last modification time for file'
                + path, 1)

    def __recv_file(self, abs_path, byte_total):
        """
        Handles downloading of a remote file.

        Args:
            abs_path (str): Absolute path of the file to be downloaded.
            byte_total (int): Total bytes expected to be received.
        """
        with File_Thread_Locker(abs_path), open(abs_path, 'wb+') as file:
            byte_count = 0
            byte_chunk = min(self.__conf['ram'], byte_total)
            skip_cache = b''
            while byte_count < byte_total:
                if byte_chunk != -1:
                    data = self.__conn.recv(byte_chunk)
                else:
                    data = self.__conn.recv()
                if b'!!SKIP!!SKIP!!' in skip_cache+data:
                    raise SkipResponseException()
                file.write(data)
                byte_count += len(data)
                skip_cache = data[-14:]

    def __recv_compressed_file(self, abs_path, byte_total):
        """
        Handles downloading and decompression of remote file.

        Args:
            abs_path (str): Absolute path of the file to be downloaded.
            byte_total (int): Total bytes expected to be received.
        """
        file_name = f'{datetime.now().microsecond}_' + \
            os.path.basename(os.path.normpath(abs_path)) + '.gz'
        z_path = os.path.join(gettempdir(), file_name)
        with File_Thread_Locker(abs_path), open(z_path, 'wb+') as zip_file:
            byte_count = 0
            byte_chunk = min(self.__conf['ram'], byte_total)
            skip_cache = b''
            while byte_count < byte_total:
                if byte_chunk != -1:
                    data = self.__conn.recv(byte_chunk)
                else:
                    data = self.__conn.recv()
                if b'!!SKIP!!SKIP!!' in skip_cache+data:
                    raise SkipResponseException()
                zip_file.write(data)
                byte_count += len(data)
                skip_cache = data[-14:]
        with gzip.open(z_path, 'rb+') as zip_file:
            with File_Thread_Locker(abs_path), open(abs_path, 'wb+') as file:
                shutil.copyfileobj(zip_file, file)

    def __send_directories(self, dirs):
        """
        Commands the client to create directories.

        Args:
            dirs (list): List of directories to remotely create.

        Raises:
            MissSpeakException: Raises if client fails to create directory.
        """
        error_count = 0
        for _dir in dirs:
            try:
                self.__send_directory(_dir)
                error_count = 0
            except MissSpeakException as error:
                error_count += 1
                self.__logger.log('Send directory error', 1)
                if error_count >= 5:
                    raise error
            except SkipResponseException:
                self.__logger.log('Client requested to directory creation.', 4)

    def __send_directory(self, _dir):
        abs_path = _dir.replace('.', self.__root, 1)
        cmd = f"MKDIR {_dir} {self.__server_struct[abs_path]['last_mod']}"
        self.__conn.sendall(cmd.encode())
        self.__logger.log(f'Sending directory {_dir}...', 4)

        data = self.__recv(1024, cmd.encode())
        if data != ('OK ' + cmd).encode():
            self.__logger.log('Send directory error', 1)
            raise MissSpeakException('MKDIR MissMatch')

    def __send_files(self, up_files):
        """
        Sends files to remote client.

        Args:
            up_files (list): List of paths of files to send.

        Raises:
            MissSpeakException: Raises if client does not acknowledge the
            request.
            MissSpeakException: Raises if client does not acknowledge receiving
            of file.
        """
        error_count = 0
        for path in up_files:
            try:
                self.__send_file(path)
                error_count = 0
            except MissSpeakException as error:
                error_count += 1
                if error_count >= 5:
                    raise error
            except SkipResponseException:
                self.__logger.log('Client requested to skip receiving file.', 4)

    def __send_file(self, path):
        abs_path = path.replace('.', self.__root, 1)
        info = self.__server_struct[abs_path]
        info['path'] = path
        byte_total = os.path.getsize(abs_path)
        compressed = False
        if self.__conf['compression'] and byte_total >= self.__conf['compression_min']:
            try:
                byte_total, abs_path = self.__compress(abs_path)
            except PermissionError:
                return
            compressed = True
        info['bytes'] = byte_total
        cmd = f"MKFILE {json.dumps(info)}"
        self.__conn.sendall(cmd.encode())

        data = self.__recv(1024, cmd.encode())
        client_ack = f"OK MKFILE {path} {byte_total}"
        if data == client_ack.encode():
            self.__logger.log(f'Sending file {path} {byte_total}...', 4)
            if byte_total > 0:
                bytes_read = 0
                byte_chunk = min(self.__conf['ram'], byte_total)
                try:
                    with File_Thread_Locker(abs_path), open(abs_path, 'rb') as f:
                        while bytes_read < byte_total:
                            if byte_chunk != -1:
                                file_bytes = f.read(byte_chunk)
                            else:
                                file_bytes = f.read()
                            self.__conn.sendall(file_bytes)
                            bytes_read += len(file_bytes)
                except PermissionError:
                    self.__logger.log(
                        'Permssion error encountered sending file' + path, 1)
                    self.__conn.sendall(b'!!SKIP!!SKIP!!')
                    raise SkipResponseException()
                except FileNotFoundError:
                    self.__conn.sendall(b'!!SKIP!!SKIP!!')
                    raise SkipResponseException()
                if compressed:
                    try:
                        os.remove(abs_path)
                    except PermissionError:
                        self.__logger.log('Unable to delete compressed file.', 1)
                    except FileNotFoundError:
                        pass
            data = self.__conn.recv(1024)
            if data != b'OK':
                self.__logger.log('Send file final ACK error', 1)
                raise MissSpeakException('MKFILE ACK FINAL')
            elif data == b'!!SKIP!!SKIP!!':
                raise SkipResponseException()
        else:
            self.__logger.log('Send file ACK error', 1)
            raise MissSpeakException('MKFILE ACK')

    def __handle_deletes(self, deletes):
        """
        Handles local and remote deletions.

        Args:
            deletes (Tuple(Tuple(), Tuple())): Paths to be deleted.
            ((Down_Directories, Down_Files), (Up_Directories, Up_Files))
        """
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
        """
        Deletes local file structure objects.

        Args:
            down_dirs (list): List of directories to delete.
            down_files (list): List of files to delete.
        """
        error_count = 0
        for _dir in down_dirs:
            try:
                if self.__confirm_delete(_dir):
                    self.__delete_dir(_dir)
                    error_count = 0
            except MissSpeakException as error:
                error_count += 1
                if error_count >= 5:
                    raise error
            except SkipResponseException:
                self.__logger.log('Client requested to skip deletion.', 3)
        error_count = 0
        for file in down_files:
            try:
                if self.__confirm_delete(file):
                    self.__delete_file(file)
                    error_count = 0
            except MissSpeakException as error:
                error_count += 1
                if error_count >= 5:
                    raise error
            except SkipResponseException:
                self.__logger.log('Client requested to skip deletion.', 3)

    def __confirm_delete(self, path):
        """
        Confirms with Client file structure object is still marked for deletion.

        Args:
            path (str): Path to delete.

        Raises:
            MissSpeakException: Client is unable to confirm or deny deletion.

        Returns:
            bool: Whether allowed to delete path.
        """
        req = f'CONFIRM DELETE {path}'.encode()
        self.__conn.sendall(req)
        data = self.__recv(1024, req)
        if data == f'OK {path}'.encode():
            self.__logger.log(f'Delete {path} confirmed.', 3)
            return True
        elif data == f'NO {path}'.encode():
            self.__logger.log(f'Delete {path} denied.', 3)
            return False
        else:
            self.__logger.log('Confirm delete error', 1)
            raise MissSpeakException('CONFIRM DELETE')

    def __delete_dir(self, _dir):
        """
        Deletes a local directory

        Args:
            _dir (str): Relative path of directory
        """
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
            self.__logger.log('Permission error encountered deleting ' 
                                + _dir, 1)

    def __delete_file(self, file):
        """
        Deletes a local file

        Args:
            file (str): Relative path of file
        """
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
            self.__logger.log('Permission error encountered deleting ' 
                                + file, 1)

    def __up_deletes(self, up_dirs, up_files):
        """
        Sends delete commands to client.

        Args:
            up_dirs (list): List of directories to delete.
            up_files (list): List of files to delete.

        Raises:
            MissSpeakException: Raises if client cannot acknowledge deletion
            request
        """
        deletes = up_dirs + up_files
        
        error_count = 0
        for path in deletes:
            try:
                self.__send_delete_cmd(path)
                error_count = 0
            except MissSpeakException as error:
                error_count += 1
                if error_count >= 5:
                    raise error

    def __send_delete_cmd(self, path):
        """
        Sends delete command to client

        Args:
            path (str): Relative path to remotely delete.

        Raises:
            MissSpeakException: Raises if client cannot acknowledge deletion
            request
        """
        abs_path = path.replace('.', self.__root, 1)
        if not os.path.exists(abs_path):
            cmd = f'DELETE {path}'
            self.__conn.sendall(cmd.encode())
            self.__logger.log(f'Sending {cmd}', 3)

            data = self.__recv(1024, cmd.encode())
            if data != b'OK':
                self.__logger.log('Send deletion error', 1)
                raise MissSpeakException('DELETE')

    def __purge_backups(self):
        """
        Removes old backups
        """
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
        """
        Compresses file using gzip.

        Args:
            path (str): Path of file to be compressed.

        Returns:
            Tuple(int, str): Size of compressed file and compressed file path.
        """
        file_name = f'{datetime.now().microsecond}_' + \
            os.path.basename(os.path.normpath(path)) + '.gz'
        with File_Thread_Locker(path), open(path, 'rb') as f_in:
            z_path = os.path.join(os.path.join(gettempdir(), 'pysync'), file_name)
            with gzip.open(z_path, 'wb', 
                            compresslevel=self.__conf['compression']) as f_out:
                shutil.copyfileobj(f_in, f_out)
        return os.path.getsize(z_path), z_path
    
    def __recv(self, b, prev_cmd):
        """
        Waits for client response and resends previous command if RETRY code is
        received.

        Args:
            b (int): Number of bytes expected to be received.
            prev_cmd (str): Previously sent command.

        Returns:
            bytes: Data received from client.
        """
        data = self.__conn.recv(b)
        if data == b'RETRY':
            self.__conn.sendall(prev_cmd)
            return self.__recv(b, prev_cmd)
        elif data == b'!!SKIP!!SKIP!!':
            raise SkipResponseException()
        else:
            return data
        
    def __timeshift_dirs(self):
        """
        Updates directories with last modified time.
        """
        for abs_path, mod in self.__dir_mods:
            os.utime(abs_path, mod)

    def terminate(self):
        """
        Terminates thread slightly more gracefully.
        """
        try:
            self.__conn.shutdown(socket.SHUT_RDWR)
            self.__conn.close()
        except:
            pass
        super.terminate()
