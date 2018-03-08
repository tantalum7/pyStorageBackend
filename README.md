# pyStorageBackend
Generic storage interface, which abstracts the storage method from the app.

### App Interface:
* Key: value pairs and stored within a document (basically a dict, that stores only bytes), each with a unqiue ID. 
  * Example application is a Note object, with a single document linked to it. The Note has a key:value pair for the title, body, date etc.
* Simple api methods: open, close, get, get_document, put, delete, delete_document, sync, count
* Stores key:value pairs against a unqiue id number. 
  * Unique id comes from the UID class (as simple as ```UID.new()```)
  * Key is a simple string, less than 32 chars.
  * Data is bytes storage up to 65KB per key:value (just encode your strings before storing them)
* Supports lazy write or redundant copying applications with a sync() command.

### Backend Interface:
* Actual backend can be selected at runtime or via startup config
* Easy to add new backends, just inherit GenericBackend class and fill in the blanks
* Working backends already written:
  * Local file json (with lazy writing)
  * FTP json file (with lazy writing)
  * Local sqlite3 (can easily be ported to remote server, and to another SQL flavour)
  * Generic mem-cached (lazy writing) local file interface, for different file formats (could be used to make yaml, ini, binary etc)
  
### Notes
* Written for python 3.x, but could be backported if someone wants that hassle.
* Well documented code, with type hints abound.
* No encryption or security. This is left as an exercise to the reader.
* Permissive license

### TODO:
* Write more backends
* Write more structured test code
