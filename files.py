import boto3

from io import BytesIO
from threading import Lock

lock = Lock()
lock2 = Lock()


class VirtualFileManager:

    _data = {}
    _data2 = {}

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
    def merge_and_upload(cls, filename, total_chunk):
        if total_chunk == 0:
            return
        # 一定要在check之后调用此方法
        output = BytesIO()
        file_data = cls._data[filename]
        for chunk in sorted(file_data, key=lambda c: c['index']):
            output.seek(chunk['offset'])
            output.write(chunk['data'])
        output.seek(0)
        cls._data.pop(filename)
        s3c = boto3.client('s3')
        # 根据文档: This is a managed transfer which will perform a multipart upload in multiple threads if necessary.
        s3c.upload_fileobj(output, 'cig-test-ningxia', f'derek/{filename}')
        return output


class VirtualFile:

    def __init__(self):
        self.data = []  # (index, offset, bytes)
        self.name = None
        self.upload_id = None

    def merge(self):
        pass

    def append(self):
        pass

    def done(self):
        pass

    def open_multi_upload(self):
        pass