import time

from files import VirtualFileManager, VirtualFile
from flask import Flask, render_template, make_response, request


app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/upload', methods=['POST'])
def upload():
    print(request.form)
    chunk_index = int(request.form['dzchunkindex'])
    total_chunk = int(request.form['dztotalchunkcount'])
    offset = int(request.form['dzchunkbyteoffset'])
    file = request.files['file']
    VirtualFileManager.append(file, chunk_index, offset)
    if VirtualFileManager.check(file.filename, total_chunk):
        VirtualFileManager.merge_and_upload(file.filename, total_chunk)
        print('Finish upload to s3.')
    return make_response(('ok', 200))


@app.route('/upload2', methods=['POST'])
def upload2():
    # 分片上传到服务器的同时分片上传到S3
    chunk_index = int(request.form['dzchunkindex'])
    total_size = int(request.form['dztotalfilesize'])
    total_chunk = int(request.form['dztotalchunkcount'])
    file = request.files['file']
    virtual_file = VirtualFileManager.get_virtual_file(file.filename, total_size, total_chunk)
    assert isinstance(virtual_file, VirtualFile)
    virtual_file.append(chunk_index, file.stream.read())

    # 加入此部分逻辑，最后一个chunk会阻塞等上传s3才返回
    if virtual_file.all_received():
        while 1:
            if virtual_file.all_upload():
                return make_response(('ok', 200))
            time.sleep(0.1)

    return make_response(('ok', 200))


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
