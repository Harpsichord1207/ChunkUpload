from io import BytesIO
from threading import Lock

lock = Lock()


class VirtualFileManager:

    _data = {}

    @classmethod
    def append(cls, file_obj, index, offset):
        with lock:
            if file_obj.filename not in cls._data:
                cls._data[file_obj.filename] = []
            cls._data[file_obj.filename].append({
                'data': file_obj.stream.read(),
                'index': index,
                'offset': offset
            })

    @classmethod
    def check(cls, filename, total_chunk):
        with lock:
            return len(cls._data.get(filename, [])) == total_chunk

    @classmethod
    def merge(cls, filename, total_chunk):
        with lock:
            data_length = cls._data.get(filename, [])
            if len(data_length) != total_chunk or data_length == 0:
                return None
            output = BytesIO()
            file_data = cls._data[filename]
            for chunk in sorted(file_data, key=lambda c: c['index']):
                output.seek(chunk['offset'])
                output.write(chunk['data'])
            output.seek(0)
            return output
