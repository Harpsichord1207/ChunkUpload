import threading

import boto3
import logging

from io import BytesIO
from threading import Lock

lock = Lock()
lock2 = Lock()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    @classmethod
    def get_virtual_file(cls, filename, file_total_size, total_index):
        # 通过文件名和文件体积来确定同一个文件，其实通过md5最好
        key = f'{filename}_{file_total_size}_{total_index}'
        with lock2:
            # dict操作应该是线程安全的，但不要依赖这一点，加个锁代价很小
            if key not in cls._data2:
                cls._data2[key] = VirtualFile(filename, total_index)
            return cls._data2[key]

    @classmethod
    def remove_virtual_file(cls, filename, file_total_size, total_index):
        key = f'{filename}_{file_total_size}_{total_index}'
        with lock2:
            cls._data2.pop(key)
            logger.info(f'Remove virtual file {filename}')


class VirtualFile:

    def __init__(self, filename, total_index):
        logger.info(f'Init virtual file {filename} with {total_index} chunks')
        self.s3client = boto3.client('s3')
        self.data = {}  # k = index, value = byte_data
        self.name = filename
        self.upload_id = None
        self.open_multi_upload()
        self.current_index = 0
        self.total_index = total_index
        self.parts = []
        self._upload_running = False

    def open_multi_upload(self):
        resp = self.s3client.create_multipart_upload(Bucket='cig-test-ningxia', Key=f'derek/{self.name}')
        self.upload_id = resp['UploadId']
        logger.info(f'Open multipart upload for file {self.name}, upload id = {self.upload_id}')

    def _upload(self):
        while self.current_index in self.data:
            bytes_io = BytesIO()
            bytes_io.write(self.data[self.current_index])
            bytes_io.seek(0)

            logger.info(f'Start to upload Part {self.current_index+1} for file {self.name}')

            resp = self.s3client.upload_part(
                Bucket='cig-test-ningxia',
                Key=f'derek/{self.name}',
                Body=bytes_io,
                UploadId=self.upload_id,
                PartNumber=self.current_index+1
            )
            self.parts.append({'ETag': resp['ETag'], 'PartNumber': self.current_index + 1})
            logger.info(f'Upload Part {self.current_index+1} for file {self.name}')
            self.current_index += 1

        if self.all_upload():
            self.s3client.complete_multipart_upload(
                Bucket='cig-test-ningxia',
                Key=f'derek/{self.name}',
                UploadId=self.upload_id,
                MultipartUpload={'Parts': self.parts}
            )
            logger.info(f'Finish upload {self.name} to s3')

    def append(self, index, byte_data):
        self.data[index] = byte_data
        logger.info(f'Received Part {index + 1} for file {self.name}')
        with lock2:
            # self._upload_running是否线程安全？
            if not self._upload_running:
                logger.info(f'Start new thread to upload s3 for file {self.name}')
                self._upload_running = True
                threading.Thread(target=self._upload).start()

    def all_received(self):
        return len(self.data) == self.total_index

    def all_upload(self):
        return self.current_index == self.total_index
