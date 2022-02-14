from files import VirtualFileManager
from flask import Flask, render_template, make_response, request


app = Flask(__name__)


@app.route('/')
def home():
    return render_template('home.html')


@app.route('/upload', methods=['POST'])
def upload():
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
def upload():
    # 分片上传到服务器的同时分片上传到S3
    # TODO TBD
    chunk_index = int(request.form['dzchunkindex'])
    total_chunk = int(request.form['dztotalchunkcount'])
    offset = int(request.form['dzchunkbyteoffset'])
    file = request.files['file']
    VirtualFileManager.append(file, chunk_index, offset)
    return make_response(('ok', 200))


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
