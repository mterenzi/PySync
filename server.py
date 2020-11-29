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


def recv_file(conn, file_path, byte_total, last_mod):
    with open(file_path, 'wb+') as file:
        byte_count = 0
        while byte_count < byte_total:
            data = conn.recv(2048)
            file.write(data)
            byte_count += len(data)
    os.utime(file_path, (last_mod, last_mod))


def download_file(conn, path, root, struct):
    req = f"REQUEST {path.replace(' ', '|')}"
    conn.sendall(req.encode())
    data = conn.recv(1024)
    msg = data.decode('UTF-8').split(' ')
    byte_total = int(msg[1])
    if byte_total > 0:
        if msg[0] == path.replace(' ', '|'):
            req_confirm = f"OK {path.replace(' ', '|')} {byte_total}"
            conn.sendall(req_confirm.encode())
            file_path = path.replace('.', root, 1)
            last_mod = struct[path]['last_mod']
            recv_file(conn, file_path, int(byte_total), int(last_mod))
        else:
            raise MissSpeakException('REQUEST MissMatch')


def handle_creates(conn, creates, root, struct, client_struct):
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
        download_file(conn, path, root, struct)


def send_directories(conn, dirs, root, struct):
    for _dir in dirs:
        path = _dir.replace('.', root, 1)
        cmd = f"MKDIR {_dir.replace(' ', '|')} {struct[path]['last_mod']}"
        conn.sendall(cmd.encode())
        data = conn.recv(1024)
        if data == ('OK ' + cmd).encode():
            pass
        else:
            raise MissSpeakException('MKDIR MissMatch')


def send_files(conn, up_files, root, struct):
    for file in up_files:
        path = file.replace('.', root, 1)
        with open(path, 'rb') as f:
            file_bytes = f.read()
        file_info = f"MKFILE {file.replace(' ', '|')} {len(file_bytes)} {struct[path]['last_mod']}"
        conn.sendall(file_info.encode())
        data = conn.recv(1024)
        if data == ('OK ' + file_info).encode():
            if len(file_bytes) > 0:
                conn.sendall(file_bytes)
                data = conn.recv(1024)
                if data != b'OK':
                    raise MissSpeakException('MKFILE MissMatch')


def down_deletes(down_dirs, down_files, root):
    for _dir in down_dirs:
        _dir = _dir.replace('.', root, 1)
        try:
            shutil.rmtree(_dir)
        except FileNotFoundError:
            pass
    for file in down_files:
        file = file.replace('.', root, 1)
        try:
            os.remove(file)
        except FileNotFoundError:
            pass


def up_deletes(conn, up_dirs, up_files):
    for _dir in up_dirs:
        cmd = f"DELETE {_dir.replace('|', ' ')}"
        conn.send(cmd.encode())
        data = conn.recv(1024)
        if data != ('OK '+cmd).encode():
            raise MissSpeakException('DELETE MissMatch')
    for file in up_files:
        cmd = f"DELETE {file.replace('|', ' ')}"
        conn.send(cmd.encode())
        data = conn.recv(1024)
        if data != ('OK '+cmd).encode():
            raise MissSpeakException('DELETE MissMatch')


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
            comparer = Structure_Comparer(structure.get_structure(), client_struct.copy())
            creates, deletes = comparer.compare_structures()
            # print(creates)
            # print(deletes)
            if creates is not None and deletes is not None:
                handle_creates(conn, creates, structure.get_root(), structure.get_structure(), client_struct)
                handle_deletes(conn, deletes, structure.get_root())
            conn.sendall(b'BYE')
            print('Synced')
            structure.save_structure()


if __name__ == "__main__":
    hostname = 'localhost'
    port = 1818
    path = '.\\folder1'
    main(hostname, port, path)
