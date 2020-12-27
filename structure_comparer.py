
class Structure_Comparer:
    """
    Generates a list of differences between different file structures.
    """

    def __init__(self, structure1, structure2):
        """
        Initializes comparer with strutures.

        Args:
            structure1 (dict): File structure dictionary one.
            structure2 (dict): File structure dictionary two.
        """
        self.__root1 = structure1.pop('root')
        self.__structure1 = self.__purge_roots(structure1, self.__root1)
        self.__structure2 = structure2

    def __purge_roots(self, structure, root):
        """
        Removes absolute paths from the structure dictionaries and makes them
        relative to a base parent.

        Args:
            structure (dict): File structure dictionary.
            root (str): Root of the structure.

        Returns:
            dict: Purged file structure dictionary.
        """
        new_structure = {}
        for path in structure.keys():
            _path = path.replace(root, '.', 1)
            new_structure[_path] = structure[path]
        return new_structure

    def compare_structures(self, purge):
        """
        Compares file structures for creations and deletions.

        Args:
            purge (bool): Whether to search for deletions.

        Returns:
            Tuple: Tuple of creation and tuple of deletions.
        """
        creates = self.__find_creates()
        if purge:
            deletes = self.__find_deletes()
        else:
            deletes = ((), ())
        if len(creates[0]) == 0 and len(creates[1]) == 0 and len(deletes[0]) == 0 \
            and len(deletes[1]) == 0:
            return None, None
        return creates, deletes

    def __find_creates(self):
        """
        Finds what needs to be created in structure1 and structure2 to be equal.

        Returns:
            Tuple: Creations for structure1 and creations for structure2
        """
        create_2, remove_1 = self.__create_filter(self.__structure1, self.__structure2)
        create_1, remove_2 = self.__create_filter(self.__structure2, self.__structure1)

        for path in remove_1:
            self.__structure1.pop(path)
        for path in remove_2:
            self.__structure2.pop(path)
        
        return create_1, create_2

    def __create_filter(self, structure1, structure2):
        """
        Filter for creations in two file structures.

        Args:
            structure1 (dict): File structure dictionary.
            structure2 (dict): File structure dictionary.

        Returns:
            Tuple(Tuple, list): A Tuple of creations and a list of what was
            processed.
        """
        dirs_2 = []
        files_2 = []
        remove_1 = []
        for path, info_1 in structure1.items():
            if info_1['deleted'] is None:
                if path in structure2:
                    info_2 = structure2[path]
                    if info_1['last_mod'] <= info_2['last_mod']:
                        continue
                if info_1['type'] == 0:
                    files_2.append(path)
                elif info_1['type'] == 1:
                    dirs_2.append(path)
                remove_1.append(path)
        return (dirs_2, files_2), remove_1

    def __find_deletes(self):
        """
        Find deletions to equalize structure one and two.

        Returns:
            Tuple: Tuple of deletions for local and remote use.
        """
        delete_up = self.__filter_deletes(self.__structure1, self.__structure2)
        delete_down = self.__filter_deletes(self.__structure2, self.__structure1)
        return delete_down, delete_up

    def __filter_deletes(self, structure1, structure2):
        """
        Filter for deletions in two file structures.

        Args:
            structure1 (dict): File structure dictionary.
            structure2 (dict): File structure dictionary.

        Returns:
            Tuple: A tuple of directories to delete and files to delete.
        """
        dirs_2 = []
        files_2 = []
        for path, info_1 in structure1.items():
            if info_1['deleted'] is not None and path in structure2:
                info_2 = structure2[path]
                if info_2['deleted'] is None and info_1['last_mod'] > info_2['last_mod']:
                    if info_1['type'] == 0:
                        files_2.append(path)
                    elif info_1['type'] == 1:
                        dirs_2.append(path)
        return dirs_2, files_2
