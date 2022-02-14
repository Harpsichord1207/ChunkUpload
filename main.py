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
        with open(file.filename, mode='wb') as fd:
            total_file.seek(0)
            fd.write(total_file.read())
    return make_response(('ok', 200))


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
