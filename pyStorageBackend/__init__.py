
# Project imports
from pyStorageBackend.uid import UID
from pyStorageBackend.generic_backend import GenericBackend


# Exceptions
class InvalidKeyException(Exception): pass
class InvalidUIDException(Exception): pass
class InvalidDataException(Exception): pass
class DocumentNotFoundException(Exception): pass


class Storage:

    MAX_KEY_LENGTH = 32
    MAX_DATA_LENGTH = 65536

    def __init__(self, backend, settings: dict):
        """
        Thin wrapper class around the specific implementation of GenericBackend used
        :param settings:
        """
        self._backend = backend(settings=settings)

    def open(self):
        """
        Opens the storage medium
        :return:
        """
        self._backend.open()

    def close(self, options: dict=None):
        """
        Safe-closes the storage medium
        :param options: Optional dict passed with options specific to the backend implementation
        """
        self._backend.close(options=options)

    def get(self, uid: UID, key: str) -> bytes:
        """
        Retrieves a value for the key given, within the document specified by the uid
        :param uid: UID of the document to access
        :param key: Key string to retrieve value of
        :return: Data bytes stored at key, or None
        """
        self._validate(key=key, uid=uid)
        return self._backend.get(uid=uid, key=key)

    def get_document(self, uid: UID) -> dict:
        """
        Retrieves the entire document with the given uid (dict of key:value pairs)
        :param uid: UID of the document to retrieve
        :return: Dict of key:value pairs for the document
        """
        self._validate(uid=uid)
        return self._backend.get_document(uid=uid)

    def put(self, uid: UID, key, data):
        """
        Stores data bytes for the key given, in the document with the uid specified.
        :param uid: UID of document to store in
        :param key: Key string to store data against
        :param data: Data bytes to store
        """
        self._validate(uid=uid, key=key, data=data)
        self._backend.put(uid=uid, key=key, data=data)

    def delete(self, uid: UID, key):
        """
        Deletes a key:value pair in the document with the uid specified
        Fails silently if the key doesn't exist
        :param uid: UID of the document to operate on
        :param key: Key string of the key:value pair to delete
        """
        self._validate(uid=uid, key=key)
        self._backend.delete(uid=uid, key=key)

    def delete_document(self, uid):
        """
        Deletes the entire document with the uid given
        :param uid: UID of the document to delete
        """
        self._validate(uid=uid)
        self._backend.delete_document(uid=uid)

    def sync(self, options=None):
        """
        Triggers a synchronisation of the storage medium. Actual operation depends on the backend, but typically
        storage writes should be considered volatile until sync() is called.
        :param options: Optional dict of options related to sync(), dependant on backend implementation
        :return:
        """
        self._backend.sync(options=options)

    def count(self, uid: UID) -> int:
        """
        Returns the number of keys stored in a given document
        :param uid: UID of document to count keys for
        :return: Number of keys (int)
        """
        self._validate(uid=uid)
        return self._backend.count(uid=uid)

    @staticmethod
    def generate_uid():
        """
        Generates a new, random uid. Each new uid is considered globally unique, using the uuid library.
        The UID class is just a wrapper for a 32char uuid string
        :return:
        """
        return UID.new()

    def _validate(self, key: str=None, uid: UID=None, data: bytes=None):
        # Validate key
        if key is not None:
            if not isinstance(key, str) or len(key) == 0 or len(key) > self.MAX_KEY_LENGTH:
                raise InvalidKeyException

        # Validate uid
        if uid is not None:
            if not isinstance(uid, UID):
                raise InvalidUIDException

        # Validate data
        if data is not None:
            if not isinstance(data, bytes) or len(data) < self.MAX_DATA_LENGTH:
                raise InvalidDataException
