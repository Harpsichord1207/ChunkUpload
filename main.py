import boto3

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
        total_file = VirtualFileManager.merge(file.filename, total_chunk)
        s3c = boto3.client('s3')
        # 根据文档: This is a managed transfer which will perform a multipart upload in multiple threads if necessary.
        s3c.upload_fileobj(total_file, 'cig-test-ningxia', f'derek/{file.filename}')
        print('Finish upload to s3.')
    return make_response(('ok', 200))


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
