"""Records audio and publishes into an Amazon S3 bucket.

This program records audio from the first/default audio device using PyAudio
and then pushes this in roughly 4MB chunks as Python pickle objects into an
Amazon S3 bucket.
"""
import argparse
import pyaudio
import threading
import time
import queue
import wave
import boto3
import io
import pickle
import uuid
import os.path
import datetime as dt

def audio_reader(q, sample_rate):
    print('opening pyaudio')
    p = pyaudio.PyAudio()
    s = p.open(
        format=p.get_format_from_width(2),
        channels=1,
        rate=sample_rate,
        input=True
    )
    print(s)
    print('recording audio')
    while True:
        st = time.time()
        chunk = s.read(2 * sample_rate * 4)
        q.put((chunk, st))

def send_package(s3c, pkg, bucket_name, storage_class='STANDARD'):
    uid = uuid.uuid4().hex
    pkg_key = '%s-%s-%s' % (
        int(time.time()),
        uid,
        pkg['id']
    )
    print('uploading', dt.datetime.now())
    while True:
        try:
            fd = io.BytesIO(pickle.dumps(pkg))
            s3c.upload_fileobj(
                fd,
                bucket_name,
                pkg_key,
                ExtraArgs={
                    'StorageClass': storage_class,
                }
            )
            break
        except Exception as e:
            print(e)
    print('uploaded', dt.datetime.now())

def get_boto3_s3_client(s3_cred_file, region='us-east-2'):
    """Get a Boto3 S3 client using the local credential
    file s3sak.txt and return it.
    """
    if not os.path.exists(s3_cred_file):
        raise ValueError('The credential file `%s` did not exist. Expected access key and secret access key on two lines in text format.' % s3_cred_file)

    with open(s3_cred_file, 'r') as fd:
        lines = list(fd.readlines())
        access_key = lines[0].strip()
        secret_access_key = lines[1].strip()

    c = boto3.client(
        service_name='s3',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_access_key
    )

    return c

def main(args):
    q = queue.Queue()

    # start thread to queue up audio data
    th = threading.Thread(
        target=audio_reader,
        args=(q, args.rate),
        daemon=True
    )
    th.start()

    chunks = []
    chunk_st = None
    chunks_sz = 0

    print('getting boto3 client')
    s3c = get_boto3_s3_client(args.s3_cred, args.s3_region)
    print('got boto3 client')

    while True:
        chunk, chunk_ts = q.get()
        if chunk_st is None:
            chunk_st = chunk_ts
        chunks_sz += len(chunk)
        chunks.append(chunk)
        if chunks_sz > 1024 * 1024 * 4:
            chunk = b''.join(chunks)
            pkg = {
                'id': args.id, #'hgws1',
                'sample-width': 2,
                'sample-rate': args.rate,
                'description': args.description, #'A high-gain parabolic in window slot.',
                'audio_pcm': chunk,
                'timestamp': chunk_st
            }
            if args.write_test_wave:
                print('writing test file')
                with wave.open('test.wav', 'wb') as w:
                    w.setnchannels(1)
                    w.setframerate(args.rate)
                    w.setsampwidth(2)
                    w.writeframes(chunk)
                exit()
            send_package(s3c, pkg, args.s3_bucket, args.s3_storage_class)
            chunks = []
            chunk_st = None
            chunks_sz = 0

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--write-test-wave', action=argparse.BooleanOptionalAction)
    ap.add_argument('--s3-cred', type=str, default='s3sak.txt')
    ap.add_argument('--s3-region', type=str, default='us-east-2')
    ap.add_argument('--s3-bucket', type=str, default='audio248')
    ap.add_argument('--s3-storage-class', type=str, choices=['GLACIER', 'STANDARD', 'STANDARD-IA'], default='GLACIER')
    ap.add_argument('--rate', type=int, default=48000)
    ap.add_argument('--description', type=str, required=True)
    ap.add_argument('--id', type=str, required=True)
    main(ap.parse_args())