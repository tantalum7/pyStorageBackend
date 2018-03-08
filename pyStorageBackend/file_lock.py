
# Library imports
import os
import uuid


# Exceptions
class FileLockException(Exception):
    pass


class FileLock(object):

    def __init__(self, file_path: str, uuid_string: str=None):
        """
        Simple file locking primitive. Works be creating a .lock version of the file it's locking, and stores a unique
        ID inside. Can pass an existing uuid_string to recreate an existing lock.

        :param file_path:
        :param uuid_string:
        """
        self._is_locked = False
        self._lockfile = os.path.join(os.path.dirname(file_path), os.path.basename(file_path)+".lock")
        self.uuid = str(uuid.uuid4()) if not uuid_string else uuid_string

    @property
    def is_locked(self) -> bool:
        """
        Returns true if this instance has the lock. This check requires opening the lock file, and does not refer to
        an internal state for lock status.
        :return:
        """
        # Try to open the lock file, if it exists then return true if it contents our uuid
        try:
            with open(self._lockfile, "r") as fp:
                return str(fp.read()) == str(self.uuid)

        # Lock file doesn't exist, so it isn't locked. Return false
        except IOError:
            return False

    def acquire(self):
        """
        Attempts to acquire the lock, and returns true if successful. Returns true if we already had the lock.
        :return:
        """
        # Try to open the lock file
        fp = None
        try:
            fp = open(self._lockfile, "r+")

        # If the file doesn't exist, create it and return true (we have the lock)
        except IOError:
            fp = open(self._lockfile, "w")
            fp.write(self.uuid)

        # Make sure the file is closed before we leave
        finally:
            if fp: fp.close()

        # Perform check to see if we have lock and return result (second check to make sure created file is there)
        return bool(self.is_locked)

    def release(self, force_release: bool=False):
        """
        Get rid of the lock by deleting the lockfile.
        When working in a `with` statement, this gets automatically
        called at the end.
        Setting force_release to true deletes the lock file, regardless who made it
        :return:
        """
        # If we have the lock, delete the lock file and clear the is locked flag
        if self.is_locked or force_release:
            os.remove(self._lockfile)

    def __enter__(self):
        """
        Activated when used in the with statement.
        Should automatically acquire a lock to be used in the with block.
        :return:
        """
        # If we don't have the lock, get it, then return this instance
        if not self.is_locked:
            self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        """
        Activated at the end of the with statement.
        It automatically releases the lock if it isn't locked.
        :return:
        """
        # If we have the lock, release it
        if self._is_locked:
            self.release()

    def __del__(self):
        """
        Make sure that the FileLock instance doesn't leave a lockfile
        lying around.
        :return:
        """
        self.release()
