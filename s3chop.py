import boto3
import wave
import os
import datetime as dt
import io
import uuid
import time
import pickle

def get_boto3_s3_client(region='us-east-2'):
    """Get a Boto3 S3 client using the local credential
    file s3sak.txt and return it.
    """
    with open('s3sak.txt', 'r') as fd:
        lines = list(fd.readlines())
        access_key = lines[0].strip()
        secret_access_key = lines[1].strip()

    # devtestuser:devtest

    c = boto3.client(
        service_name='s3',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )

    return c

def send_package(s3c, pkg):
    fd = io.BytesIO(pickle.dumps(pkg))
    uid = uuid.uuid4().hex
    pkg_key = '%s-%s-%s' % (
        pkg['timestamp'],
        uid,
        pkg['id']
    )
    print('uploading')
    while True:
        try:
            s3c.upload_fileobj(
                fd,
                'audio248',
                pkg_key,
                ExtraArgs={
                    # STANDARD, STANDARD_IA, REDUCED_REDUNDANCY, GLACIER
                    'StorageClass': 'STANDARD_IA',
                }
            )
            break
        except:
            pass
    print('uploaded')

def main(path):
    s3c = get_boto3_s3_client()

    stat_data = os.lstat(path)
    ctime = stat_data.st_birthtime
    mtime = stat_data.st_mtime

    with wave.open(path, 'rb') as w:
        chunk_count = int(1024 * 1024 * 4 / 2)
        ts = ctime
        while True:
            chunk = w.readframes(chunk_count)
            if len(chunk) == 0:
                break
            pkg = {
                'id': 'hgws1',
                'note': '16-bit PCM',
                'description': 'A high-gain parabolic in window slot.',
                'audio_pcm': chunk,
                'timestamp': ts,
            }
            print('send', dt.datetime.fromtimestamp(ts))
            send_package(s3c, pkg)
            ts += len(chunk) / 2 / 48000

if __name__ == '__main__':
    main('C:\\Users\\frita\\OneDrive\\Documents\\Sound Recordings\\Recording2.wav')
