import os
import stat
import shutil
import datetime
import re


class File_Descriptor:

    def __init__(self, root, path, dir, sftp):
        self.__root = root
        self.path = path
        self.__dir = dir
        self.__sftp = sftp
        self.discover()

    def discover(self):
        if self.__sftp is None:
            status = os.stat(self.path)
            self.info = {
                'perm': status[stat.ST_MODE],
                'size': status[stat.ST_SIZE],
                'last_mod': status[stat.ST_MTIME],
            }
        else:
            self.info = {}

    def root_swap(self, new_root):
        return self.path.replace(self.__root, new_root)

    def isdir(self):
        return self.__dir


class PySyncer:
    """
    Syncs from Source to Target
    """

    def __init__(self, source, target, sync=False, purge=False, gitignore=True):
        self.__dir1, sftp1 = source
        self.__dir2, sftp2 = target

        if sftp1 is not None:
            self.__sftp_direction = 1
            self.__sftp = sftp1
        elif sftp2 is not None:
            self.__sftp_direction = 2
            self.__sftp = sftp2
        else:
            self.__sftp_direction = 0
            self.__sftp = None

        self.__sync = sync
        self.__purge = purge
        self.__gitignore = gitignore

        self.__n_create_d = 0
        self.__n_create_f = 0
        self.__n_update_d = 0
        self.__n_update_f = 0
        self.__n_delete_d = 0
        self.__n_delete_f = 0

        self.__time_start = datetime.datetime.now()

    def run(self):
        creates, updates, deletes = self.__dif()
        self.__info()
        if self.__sync:
            if self.__n_create_d > 0 or self.__n_create_f > 0:
                print('Creating new files and directories...')
                self.__create(*creates)
            if self.__n_update_d > 0 or self.__n_update_f > 0:
                print('Updating files and directories...')
                self.__update(*updates)
            if self.__purge:
                if self.__n_delete_d > 0 or self.__n_delete_f > 0:
                    print('Deleting files and directories...')
                    self.__delete(*deletes)
        print(f'Finished in {datetime.datetime.now() - self.__time_start}')

    def __dif(self):
        descriptors = []

        if self.__sftp_direction == 1:
            sftp = self.__sftp
        else:
            sftp = None

        for root, dirs, files in os.walk(self.__dir1, topdown=True):
            if self.__gitignore and '.gitignore' in files:
                dirs, files = self.__process_gitignore(root, dirs, files)
            for name in dirs:
                descriptor = File_Descriptor(root=self.__dir1,
                                             path=os.path.join(root, name),
                                             dir=True,
                                             sftp=sftp)
                descriptors.append(descriptor)
            for name in files:
                descriptor = File_Descriptor(root=self.__dir1,
                                             path=os.path.join(root, name),
                                             dir=False,
                                             sftp=sftp)
                descriptors.append(descriptor)

        creates_d, creates_f, descriptors = self.__creatable(descriptors)
        self.__n_create_d = len(creates_d)
        self.__n_create_f = len(creates_f)

        updates_d, updates_f = self.__updateable(descriptors)
        self.__n_update_d = len(updates_d)
        self.__n_update_f = len(updates_f)

        if self.__purge:
            deletes_d, deletes_f = self.__deleteable()
            self.__n_delete_d = len(deletes_d)
            self.__n_delete_f = len(deletes_f)
        else:
            deletes_d, deletes_f = None, None
        return (creates_d, creates_f), (updates_d, updates_f), (deletes_d, deletes_f)

    def __creatable(self, descriptors):
        creates_d = []
        creates_f = []

        non_creatables = []
        for descriptor in descriptors:
            if not self.exists(descriptor.root_swap(self.__dir2)):
                if descriptor.isdir():
                    creates_d.append(descriptor)
                else:
                    creates_f.append(descriptor)
            else:
                non_creatables.append(descriptor)
        return creates_d, creates_f, non_creatables

    def __updateable(self, descriptors):
        updates_d = []
        updates_f = []
        for descriptor in descriptors:
            if self.__sftp_direction == 2:
                sftp = self.__sftp
            else:
                sftp = None
            check_file = File_Descriptor(root=self.__dir2,
                                         path=descriptor.root_swap(
                                             self.__dir2),
                                         dir=descriptor.isdir(),
                                         sftp=sftp)
            update, update_type = self.update_check(descriptor, check_file)
            if update:
                if descriptor.isdir():
                    updates_d.append((descriptor, update_type))
                else:
                    updates_f.append((descriptor, update_type))
        return updates_d, updates_f

    def update_check(self, descriptor, check_file):
        desc_info = descriptor.info
        check_info = check_file.info

        if desc_info['last_mod'] > check_info['last_mod']:
            if not descriptor.isdir():
                return True, 'hard'
            elif desc_info['perm'] != check_info['perm']:
                return True, 'soft'
        return False, None

    def __deleteable(self):
        deletes_d = []
        deletes_f = []
        if self.__sftp_direction != 2:
            sftp = None
            for root, dirs, files in os.walk(self.__dir2, topdown=True):
                for name in dirs:
                    descriptor = File_Descriptor(root=self.__dir2,
                                                 path=os.path.join(root, name),
                                                 dir=True,
                                                 sftp=sftp)
                    if not self.exists(descriptor.root_swap(self.__dir1)):
                        deletes_d.append(descriptor)
                for name in files:
                    descriptor = File_Descriptor(root=self.__dir2,
                                                 path=os.path.join(root, name),
                                                 dir=False,
                                                 sftp=sftp)
                    if not self.exists(descriptor.root_swap(self.__dir1)):
                        deletes_f.append(descriptor)
        return deletes_d, deletes_f

    def __info(self):
        print(f'{self.__n_create_d} createable directories...')
        print(f'{self.__n_create_f} createable files...')
        print(f'{self.__n_update_d} updateable directories...')
        print(f'{self.__n_update_f} updateable files...')
        if self.__purge:
            print(f'{self.__n_delete_d} deletable directories...')
            print(f'{self.__n_delete_f} deletable files...')

    def __create(self, creates_d, creates_f):
        if self.__sftp_direction != 2:
            for descriptor in creates_d:
                path = descriptor.root_swap(self.__dir2)
                mode = descriptor.info['perm']
                os.makedirs(path, mode=mode, exist_ok=True)
            for descriptor in creates_f:
                shutil.copy(descriptor.path, descriptor.root_swap(self.__dir2))

    def __update(self, updates_d, updates_f):
        if self.__sftp_direction != 2:
            for descriptor, _ in updates_d:
                path = descriptor.root_swap(self.__dir2)
                perm = descriptor.info['perm']
                mtime = descriptor.info['last_mod']
                os.chmod(path, perm)
                os.utime(path, (mtime, mtime))
            for descriptor, update_type in updates_f:
                path = descriptor.root_swap(self.__dir2)
                if update_type == 'soft':
                    perm = descriptor.info['perm']
                    os.chmod(path, perm)
                else:
                    os.remove(path)
                    shutil.copy(descriptor.path, path)
                mtime = descriptor.info['last_mod']
                os.utime(path, (mtime, mtime))

    def __delete(self, deletes_d, deletes_f):
        if self.__sftp_direction != 2:
            for descriptor in deletes_d:
                try:
                    shutil.rmtree(descriptor.root_swap(self.__dir2))
                except FileNotFoundError:
                    pass
            for descriptor in deletes_f:
                try:
                    os.remove(descriptor.root_swap(self.__dir2))
                except FileNotFoundError:
                    pass

    def exists(self, path):
        if self.__sftp_direction != 2:
            return os.path.exists(path)
        # TODO SFTP exists

    def __process_gitignore(self, root, dirs, files):
        gitignore_path = os.path.join(root, '.gitignore')
        with open(gitignore_path, 'r') as f:
            lines = f.readlines()
        while '\n' in lines:
            lines.remove('\n')
        ignore_lines = []
        for line in lines:
            line = line.strip()
            line = line.strip('/')
            line = line.replace('*', r'.*')
            line = line.replace('.', r'\.')
            line = line.replace('[', r'\[')
            line = line.replace(']', r'\]')
            if line[0] != '#':
                ignore_lines.append(line)
        remove_dirs = []
        remove_files = []
        for pattern in ignore_lines:
            for dir in dirs:
                if re.match(pattern, dir) is not None:
                    remove_dirs.append(dir)
            for file in files:
                if re.match(pattern, file) is not None:
                    remove_files.append(file)
        for _dir in remove_dirs:
            dirs.remove(_dir)
        for _file in remove_files:
            files.remove(_file)
        return dirs, files


def main():
    print('Running sync...')
    print()
    syncer = PySyncer(('.\\folder1', None), ('.\\folder2', None),
                      sync=True, purge=True)
    syncer.run()


if __name__ == "__main__":
    main()
