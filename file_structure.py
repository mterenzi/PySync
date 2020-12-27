import os
import re
import stat
import json
import datetime


class File_Structure:
    """
    Class for interrogating the local file structure.
    """

    def __init__(self, root, gitignore, purge_limit):
        """
        Initializes File_Structure. Builds representation of file structure
        for sync.

        Args:
            root (str): Root directory to parse.
            gitignore (bool): Whether to parse gitignores
            purge_limit (int): Number of days to keep deleted file records
        """
        self.__root = os.path.abspath(root)
        if not os.path.exists(self.__root):
            os.mkdir(self.__root)

        self.__gitignore = gitignore
        self.__purge_limit = purge_limit
        home_dir = os.path.expanduser('~')

        stripped_root = os.path.basename(os.path.normpath(self.__root))
        conf_path = os.path.join(home_dir, '.conf')
        conf_path = os.path.join(conf_path, 'pysync')
        self.__user_conf_path = os.path.join(conf_path, stripped_root)
        self.__json_path = os.path.join(self.__user_conf_path, stripped_root+'.json')
        
        self.__structure = self.__get_structure()
        
    def __get_structure(self):
        """
        Gets full file structure

        Returns:
            dict: File structure dictionary
        """
        old_structure = self.__read_old_structure()
        new_structure = self.__build_file_structure()
        if old_structure is not None:
            old_structure = self.__check_deletions(old_structure)
            return {**old_structure, **new_structure}
        return new_structure

    def __read_old_structure(self):
        """
        Grabs old file structure from previous syncs.

        Returns:
            dict: Old file structure dictionary.
        """
        if os.path.exists(self.__json_path):
            with open(self.__json_path, 'r') as file:
                return json.load(file)
        return None

    def __build_file_structure(self):
        """
        Builds new file structure dictionary from local files.

        Returns:
            dict: New file structure dictionary.
        """
        paths = []
        for root, _dirs, _files in os.walk(self.__root, topdown=True):
            if self.__gitignore and '.gitignore' in _files:
                _dirs, _files = self.__pattern_filter(_dirs, _files, self.__process_gitignore(root))
            _paths = _dirs + _files
            for path in _paths:
                paths.append(os.path.join(root, path))

        file_structure = {'root': self.__root}
        for path in paths:
            file_structure[path] = self.__discover(path)
        return file_structure

    def __process_gitignore(self, root):
        """
        Parse gitignore for exclusion patterns.

        Args:
            root (str): Root path

        Returns:
            list: List of regexs for exclusion.
        """
        gitignore_path = os.path.join(root, '.gitignore')
        with open(gitignore_path, 'r') as f:
            lines = f.readlines()
        while '\n' in lines:
            lines.remove('\n')
        ignore_patterns = []
        for line in lines:
            line = line.strip()
            line = line.strip('/')
            line = line.replace('*', r'.*')
            line = line.replace('.', r'\.')
            line = line.replace('[', r'\[')
            line = line.replace(']', r'\]')
            if line[0] != '#':
                ignore_patterns.append(line)
        return ignore_patterns

    def __pattern_filter(self, dirs, files, ignore_patterns):
        """
        Filters directories and files by patterns.

        Args:
            dirs (list): List of directories.
            files (list): List of files.
            ignore_patterns (list): List of regular expressions.

        Returns:
            Tuple(list, list): Tuple of directories and files.
        """
        remove_dirs = []
        remove_files = []
        for pattern in ignore_patterns:
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

    def __discover(self, path):
        """
        Get information on path object.

        Args:
            path (str): Object path

        Returns:
            dict: File object information
        """
        status = os.stat(path)
        info = {
            'type': int(stat.S_ISDIR(status[stat.ST_MODE])),
            'perm': status[stat.ST_MODE],
            'size': status[stat.ST_SIZE],
            'last_mod': status[stat.ST_MTIME],
            'deleted': None,
        }
        return info

    def __check_deletions(self, structure):
        """
        Checks if old paths have been deleted and removes stale deletions.

        Args:
            structure (dict): File structure dictionary.

        Returns:
            dict: File structure dictionary.
        """
        _structure = structure.copy()
        for path, info in structure.items():
            if path == 'root':
                continue
            if info['deleted'] is None:
                if not os.path.exists(path):
                    timestamp = datetime.datetime.now().timestamp()
                    _structure[path]['deleted'] = timestamp
                    _structure[path]['last_mod'] = timestamp
            else:
                delete_time = datetime.datetime.fromtimestamp(info['deleted'])
                now_time = datetime.datetime.now()
                delta = now_time - delete_time
                if self.__purge_limit is not None and delta.days > self.__purge_limit:
                    del _structure[path]
        structure = _structure
        return structure

    def update_structure(self):
        """
        Updates structure in RAM.
        """
        structure = self.__check_deletions(self.__structure)
        new_structure = self.__build_file_structure()
        self.__structure = {**structure, **new_structure}

    def save_structure(self):
        """
        Saves file structure to disk.
        """
        os.makedirs(self.__user_conf_path, exist_ok=True)
        with open(self.__json_path, 'w+') as file:
            json.dump(self.__structure, file, indent=4)

    def dump_structure(self):
        """
        Gets file structure in bytes

        Returns:
            bytes: File structure dictionary as json dumps byte stream.
        """
        new_structure = {}
        for path in self.__structure.keys():
            _path = path.replace(self.__root, '.', 1)
            new_structure[_path] = self.__structure[path]
        new_structure.pop('root')
        return json.dumps(new_structure)

    def get_structure(self):
        """
        Get copy of file structure dictionary.

        Returns:
            dict: File structure dictionary copy.
        """
        return self.__structure.copy()

    def print_structure(self):
        """
        Prints file structure dictionary to stdout.
        """
        for path, info in self.__structure.items():
            print(f'{path}: {info}')

    def get_root(self):
        """
        Gets file structure root

        Returns:
            str: File structure root.
        """
        return self.__root
