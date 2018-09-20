from io import BytesIO

def split(f, segment_size=20*1024*1024):
    while True:
        buff = BytesIO()
        chunk = f.read(segment_size)
        if chunk == b'':
            break
        buff.write(chunk)
        buff.seek(0)
        yield buff


def join(fs, out_file, buff_size=20*1024*1024):
    for f in fs:
        while True:
            chunk = f.read()
            if chunk == b'':
                break
            out_file.write(chunk)
    out_file.seek(0)
