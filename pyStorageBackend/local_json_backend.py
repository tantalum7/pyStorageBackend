
# Library imports
import os

# Project imports
from pyStorageBackend.generic_backend import GenericJsonBackend
from pyStorageBackend.file_lock import FileLock


class LocalJsonBackend(GenericJsonBackend):

    def __init__(self, settings: dict):
        """
        Local JSON implementation of GenericBackend. JSON file contents are loaded into memory when opened.
        All read/write operations are in memory. Memory contents are written to json file when sync() is called
        :param settings: Not used
        """
        super(LocalJsonBackend, self).__init__(settings)
        self._file_lock = FileLock(self.settings["path"])

    def _read(self):
        with open(self.settings["path"], "r") as fp:
            return fp.read()

    def _overwrite(self, contents):

        # Concat temp file path, by appending .tmp
        tempname = self.settings["path"] + '.tmp'

        # Try to open the temp file, and write contents
        try:
            fp = open(tempname, "w+")
            fp.write(contents)

        # Catch any exception, delete the temp file then re-raise exception
        except:
            os.remove(tempname)
            raise

        # Write temporary file was successful, replace the real file with the temp one
        else:
            try:
                fp.close()
                os.replace(tempname, self._path)

            except Exception as e:
                exit()

        # Make sure the file point gets closed
        finally:
            fp.close()

        pass

    def _set_lock(self):
        return self._file_lock.acquire()

    def _release_lock(self):
        self._file_lock.release()
