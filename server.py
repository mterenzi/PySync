from client import delete_down
from file_structure import File_Structure
from structure_comparer import Structure_Comparer
import socket
import ssl
import json
import shutil
import os
from exceptions import *


def request_struct(conn):
    conn.sendall(b'REQUEST STRUCT')
    data = conn.recv(1024)
    msg = data.decode('UTF-8').split(' ')
    if msg[0] == 'STRUCT':
        byte_total = msg[1]
        struct_confirm = f'OK STRUCT {byte_total}'
        conn.sendall(struct_confirm.encode())
        struct = recv_bytes(conn, int(byte_total))
        return json.loads(struct)
    else:
        raise MissSpeakException('STRUCT MissMatch')

def recv_bytes(conn, byte_total):
    byte_count = 0
    data = None
    while byte_count < byte_total:
        if data is None:
            data = conn.recv(2048)
        else:
            data += conn.recv(2048)
        byte_count = len(data)
    return data


def recv_file(conn, file_path, byte_total):
    with open(file_path, 'wb+') as file:
        byte_count = 0
        while byte_count < byte_total:
            data = conn.recv(2048)
            file.write(data)
            byte_count += len(data)


def download_file(conn, path, root):
    req = f'REQUEST {path}'
    conn.sendall(req.encode())

    data = conn.recv(1024)
    info = json.loads(data)
    if info['path'] != path:
        raise MissSpeakException('REQUEST File')
    byte_total = info['bytes']
    ack = f"OK {byte_total}"
    conn.sendall(ack.encode())

    abs_path = path.replace('.', root, 1)
    if byte_total > 0:
        recv_file(conn, abs_path, byte_total)
    else:
        open(abs_path, 'w+').close()
    last_mod = int(info['last_mod'])
    os.utime(abs_path, (last_mod, last_mod))


def handle_creates(conn, creates, struct, client_struct):
    root = struct['root']
    down, up = creates
    down_dirs, down_files = down
    get_directories(down_dirs, root, client_struct)
    get_files(conn, down_files, root, client_struct)

    up_dirs, up_files = up
    send_directories(conn, up_dirs, root, struct)
    send_files(conn, up_files, root, struct)


def get_directories(dirs, root, struct):
    for _dir in dirs:
        last_mod = struct[_dir]['last_mod']
        _dir = _dir.replace('.', root, 1)
        os.makedirs(_dir, exist_ok=True)
        os.utime(_dir, (last_mod, last_mod))


def get_files(conn, files, root, struct):
    for path in files:
        download_file(conn, path, root)


def send_directories(conn, dirs, root, struct):
    for _dir in dirs:
        abs_path = _dir.replace('.', root, 1)
        cmd = f"MKDIR {_dir} {struct[abs_path]['last_mod']}"
        conn.sendall(cmd.encode())

        data = conn.recv(1024)
        if data == ('OK ' + cmd).encode():
            pass
        else:
            raise MissSpeakException('MKDIR MissMatch')


def send_files(conn, up_files, root, struct):
    for path in up_files:
        abs_path = path.replace('.', root, 1)
        info = struct[abs_path]
        info['path'] = path
        byte_total = os.path.getsize(abs_path)
        info['bytes'] = byte_total
        cmd = f"MKFILE {json.dumps(info)}"
        conn.sendall(cmd.encode())

        data = conn.recv(1024)
        client_ack = f"OK MKFILE {path} {byte_total}"
        if data == client_ack.encode():
            if byte_total > 0:
                _bytes = 2048
                bytes_read = 0
                with open(abs_path, 'rb') as f:
                    while bytes_read < byte_total:
                        file_bytes = f.read(_bytes)
                        conn.sendall(file_bytes)
                        bytes_read += _bytes
            data = conn.recv(1024)
            if data != b'OK':
                raise MissSpeakException('MKFILE ACK FINAL')
        else:
            raise MissSpeakException('MKFILE ACK')


def down_deletes(down_dirs, down_files, root):
    for _dir in down_dirs:
        _dir = _dir.replace('.', root, 1)
        try:
            shutil.rmtree(_dir)
        except FileNotFoundError:
            pass
        except PermissionError:
            pass
    for file in down_files:
        file = file.replace('.', root, 1)
        try:
            os.remove(file)
        except FileNotFoundError:
            pass
        except PermissionError:
            pass


def up_deletes(conn, up_dirs, up_files):
    deletes = up_dirs + up_files
    for path in deletes:
        cmd = f'DELETE {path}'
        conn.sendall(cmd.encode())
        
        data = conn.recv(1024)
        if data != b'OK':
            raise MissSpeakException('DELETE')


def handle_deletes(conn, deletes, root):
    down, up = deletes
    down_dirs, down_files = down
    down_deletes(down_dirs, down_files, root)

    up_dirs, up_files = up
    up_deletes(conn, up_dirs, up_files)


def main(hostname, port, path):
    structure = File_Structure(path, gitignore=True)

    context = ssl.create_default_context()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((hostname, port))
        sock.listen(5)
        # with context.wrap_socket(sock, server_side=True) as ssock:
        if True:
            ssock = sock
            conn, addr = ssock.accept()
            client_struct = request_struct(conn)
            comparer = Structure_Comparer(structure.get_structure().copy(),
                                            client_struct.copy())
            creates, deletes = comparer.compare_structures()
            if creates is not None and deletes is not None:
                handle_creates(conn, creates, structure.get_structure(), client_struct)
                handle_deletes(conn, deletes, structure.get_root())
            conn.sendall(b'BYE')
            print('Synced')
            structure.update_structure()
            structure.save_structure()


if __name__ == "__main__":
    hostname = 'localhost'
    port = 1818
    path = '.\\folder1'
    main(hostname, port, path)
