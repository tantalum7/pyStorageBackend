
# Project imports
from pyStorageBackend.uid import UID
from pyStorageBackend.json_cache import JsonCache
from pyStorageBackend import DocumentNotFoundException


class GenericBackend(object):

    def __init__(self):
        pass

    def open(self):
        raise NotImplemented

    def close(self, options=None):
        raise NotImplemented

    def get(self, uid, key):
        raise NotImplemented

    def get_document(self, uid):
        raise NotImplemented

    def put(self, uid, key, value):
        raise NotImplemented

    def delete(self, uid, key):
        raise NotImplemented

    def delete_document(self, uid):
        raise NotImplemented

    def sync(self):
        raise NotImplemented

    def count(self, uid):
        raise NotImplemented


class GenericJsonBackend(GenericBackend):

    def __init__(self, settings: dict):
        """
        Generic backend, implementing a JSON cache. All operations are in memory until sync() is called.
        _read, _overwrite, _set_lock and _release_lock must be overridden with useful functions
        """
        self._db = None
        self.settings = settings

    def open(self):
        """
        Opens the JSON file at the path provided, and loads contents into memory.
        :param path:
        :return:
        """
        self._db = JsonCache(read_method=self._read, overwrite_method=self._overwrite,
                             set_lock_method=self._set_lock, release_lock_method=self._release_lock)

    def close(self, options: dict=None):
        """
        Closes the json file, performs a last sync() and then drops contents from memory.
        :param options:
        :return:
        """
        self.sync()
        self._db.close()
        self._db = None

    def get(self, uid: UID, key: str) -> str:
        """
        Gets the string stored against the key passed, for document with the UID passed
        :param uid:
        :param key:
        :return:
        """
        # Try and get the document with the UID passed
        try:
            doc = self._db[str(uid)]

        # Re-raise a KeyError as a DocumentNotFoundException
        except KeyError:
            raise DocumentNotFoundException

        # Return the string stored against the key passed, or None
        else:
            return str(doc.get(key, None))

    def get_document(self, uid: UID) -> dict:
        """
        Gets the entire document with the UID passed, returns it as a dict
        :param uid:
        :return:
        """
        # Try and get the document with the UID passed
        try:
            doc = self._db[str(uid)]

        # Re-raise a KeyError as a DocumentNotFoundException
        except KeyError:
            raise DocumentNotFoundException

        # Return the doc dict as is
        else:
            return doc

    def delete_document(self, uid: UID):
        """
        Deletes the entire document with the UID passed
        :param uid:
        :return:
        """
        # Try and delete the document with the UID passed
        try:
            del self._db[str(uid)]

        # Re-raise a KeyError as a DocumentNotFoundException
        except KeyError:
            raise DocumentNotFoundException

    def put(self, uid: UID, key: str, value: str):
        """
        Puts string value against string key, to the document with the UID passed.
        Put works as an insert or update command. If the document doesn't exist, it is created
        :param uid:
        :param key:
        :param value:
        :return:
        """
        # If the document with this UID can't be found, create it
        if not self._db[str(uid)]:
            self._db[str(uid)] = {}

        # Store the value string
        self._db[str(uid)][key] = str(value)

    def sync(self, options: dict=None):
        """
        Synchronises the memory cache with storage file
        :param options:
        :return:
        """
        self._db.sync()

    def count(self, uid: UID) -> int:
        """
        Returns the number of keys stored in a document with the given UID
        :param uid:
        :return:
        """
        # Return the len() of the document, or 0 if the doc can't be found
        return len(self._db[str(uid)]) if self._db[str(uid)] else 0

    def _read(self) -> str:
        raise NotImplemented

    def _overwrite(self, contents: str):
        raise NotImplemented

    def _set_lock(self) -> bool:
        raise NotImplemented

    def _release_lock(self):
        raise NotImplemented
