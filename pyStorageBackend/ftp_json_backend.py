
# Library imports
import ftplib
import io
import os

# Project imports
from pyStorageBackend.generic_backend import GenericJsonBackend
from pyStorageBackend.json_cache import JsonCache


class FtpJsonBackend(GenericJsonBackend):

    class _FtpConnection:

        NOOP = "NOOP"
        STOR = "STOR"
        RETR = "RETR"
        DOWNLOAD_SUCCESS = "226"

        def __init__(self, url: str, username: str, password: str):
            self.url = url
            self.username = username
            self.password = password
            self._download = ""

        def __enter__(self):
            self._ftp = ftplib.FTP(host=self.url, user=self.username, passwd=self.password)
            return self

        def __exit__(self, *args, **kwargs):
            self._ftp.quit()
            self._ftp.close()

        @property
        def is_connected(self):
            try:
                self._ftp.voidcmd(self.NOOP)
                return True
            except ftplib.error_reply:
                return False

        def download(self, path: str) -> str:
            self._set_cwd(path)
            self._download = ""
            result = self._ftp.retrbinary("{} {}".format(self.RETR, os.path.basename(path)), self._download_callback)
            print("Downloaded {} bytes".format(len(self._download)))
            try:
                return self._download if result.startswith(self.DOWNLOAD_SUCCESS) else None
            except Exception as e:
                print(e)

        def upload(self, path: str, contents: str):
            self._set_cwd(path)
            fp = io.BytesIO(bytes(contents.encode("utf-8")))
            try:
                self._ftp.storbinary("{} {}".format(self.STOR, os.path.basename(path)), fp)
            except Exception as e:
                print(e)
            print("Uploaded {} bytes".format(len(contents)))

        def delete(self, path: str):
            self._set_cwd(path)
            self._ftp.delete(os.path.basename(path))

        def rename(self, source_path, new_name):
            self._set_cwd(source_path)
            self._ftp.rename(os.path.basename(source_path), new_name)

        def _set_cwd(self, path):
            path = os.path.dirname(path) if os.path.dirname(path).startswith("/") else "/"+os.path.dirname(path)
            path = path if path.endswith("/") else path + "/"
            self._ftp.cwd(os.path.dirname(path))

        def _download_callback(self, chunk):
            self._download += chunk.decode("utf-8")

    def __init__(self, settings: dict):
        """
        Json based remote FTP implementation of GenericBackend.
        :param settings:
        """
        super(FtpJsonBackend, self).__init__(settings)

    def open(self):

        with self._get_connection() as ftp:
            assert ftp.is_connected

        self._db = JsonCache(read_method=self._read, overwrite_method=self._overwrite,
                             set_lock_method=self._set_lock, release_lock_method=self._release_lock)

    def _read(self):
        with self._get_connection() as ftp:
            return ftp.download(self.settings["path"])

    def _overwrite(self, contents: str):

        # Concat temp file path, by appending .tmp
        tempname = self.settings["path"] + '.tmp'

        with self._get_connection() as ftp:
            ftp.upload(tempname, contents)
            download_contents = ftp.download(tempname)

            if contents == download_contents:
                ftp.delete(self.settings["path"])
                ftp.rename(tempname, os.path.basename(self.settings['path']))

            else:
                print("FTP overwrite error, remote contents do not match local")

    def _set_lock(self):
        return True

    def _release_lock(self):
        pass

    def _get_connection(self) -> _FtpConnection:
        return self._FtpConnection(url=self.settings["url"], username=self.settings["username"],
                                   password=self.settings["password"])
