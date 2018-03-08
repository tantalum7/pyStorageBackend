
# Library imports
import json
from typing import Callable

# Project imports
from exceptions import StorageLockedException


class JsonCache:

    def __init__(self, read_method: Callable, overwrite_method: Callable[[str], None],
                 set_lock_method: Callable[[], bool], release_lock_method: Callable):
        """
        Generic json interface, with local caching. Operates as a dict like object, entirely in memory.

        When the dict is initialised, it reads using the read_method and parses json into local dict.

        When the dict is sync'd or closed, it dumps the local dict into a json string and overwrites the file
        using the overwrite_method.

        Simple set/release lock methods are used to prevent simultaneous operations. The lock is taken when the
        object is initialised, and released when it's closed or deleted.

        :param None read_method(): Returns the json file as a string
        :param None overwrite_method(str): Overwrites the json file with the string passed
        :param bool set_lock_method(): Attempts to grab the lock, returns true/false
        :param None release_lock_method(): Releases the lock, if its held or not
        """
        # Store methods in class
        self._read = read_method
        self._write = overwrite_method
        self._set_lock = set_lock_method
        self._release_lock = release_lock_method

        # If we can't get the lock, raise an exception
        if not self._set_lock():
            raise StorageLockedException

        # Read the file from storage, parse as json string, store as dict
        self._cache = json.loads(read_method())

    def __getitem__(self, key: str) -> str:
        return self._cache[key]

    def __delitem__(self, key):
        del self._cache[key]

    def __len__(self):
        return len(self._cache)

    def values(self) -> [str]:
        return self._cache.values()

    def keys(self) -> [str]:
        return self._cache.keys()

    def items(self) -> [(str, str)]:
        return self._cache.items()

    def sync(self):
        """
        Overwrites the stored file with the memory cache contents using the overwrite method
        :return:
        """
        self._write(json.dumps(self._cache, ensure_ascii=True))

    def close(self):
        """
        Syncs storage, releases the lock and clears the local cache and forgets external read/write/lock methods
        :return:
        """
        self.sync()
        self._release_lock()
        self._cache = None
        self._release_lock = self._set_lock = self._read = self._write = None

    def __exit__(self, *exc_info):
        self.close()

    def as_dict(self):
        return self._cache.copy()
