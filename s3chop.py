import boto3
import wave
import os
import datetime as dt
import io
import uuid
import time
import pickle
import argparse
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import soundfile as sf
import numpy as np

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

def send_package(s3c, pkg, aes_key=None):
    fd = io.BytesIO(pickle.dumps(pkg))
    uid = uuid.uuid4().hex
    pkg_key = '%s-%s-%s' % (
        pkg['timestamp'],
        uid,
        pkg['id']
    )

    if aes_key is not None:
        actual_key = aes_key[:32]
        cipher = AES.new(actual_key, AES.MODE_CTR)
        pkg['encrypted'] = 'aes-256-ctr'
        pkg['nonce'] = cipher.nonce
        pkg['audio_pcm'] = cipher.encrypt(pkg['audio_pcm'])
        print('encrypted')

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

def main(args: object):
    s3c = get_boto3_s3_client()

    if os.path.isdir(args.data_path):
        for node in os.listdir(args.data_path):
            fnode = os.path.join(args.data_path, node)
            if os.path.isfile(fnode):
                process_file(s3c, args, fnode)
    else:
        process_file(s3c, args, args.data_path)

def process_file(s3c, args: object, path: str):
    stat_data = os.lstat(path)
    ctime = stat_data.st_birthtime
    mtime = stat_data.st_mtime

    ctime_dt = dt.datetime.fromtimestamp(ctime)

    print('creation-time', ctime_dt)

    if args.aes_key_path is None:
        aes_key = None
    else:
        with open(args.aes_key_path, 'rb') as fd:
            aes_key = fd.read()

    with sf.SoundFile(path, 'r') as f:
        chunk_count = int(1024 * 1024 * 4 / 4)
        ts = ctime
        while f.tell() < len(f):
            chunk = f.read(chunk_count)

            assert chunk.dtype == np.float64

            chunk = chunk.astype(np.float32)

            if len(chunk) == 0:
                break
            
            pkg = {
                'id': args.id,
                'sample-width': 4,
                'sample-rate': f.samplerate,
                'channel-count': f.channels,
                'description': args.description,
                'audio_pcm': chunk.tobytes(),
                'timestamp': ts,
            }
            
            print('send', ctime, dt.datetime.fromtimestamp(ts))
            send_package(s3c, pkg, aes_key)
            ts += len(chunk) / f.channels / f.samplerate
            
    os.remove(path)

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--data-path', type=str, required=True)
    ap.add_argument('--aes-key-path', type=str, default=None)
    ap.add_argument('--s3-cred', type=str, default='s3sak.txt')
    ap.add_argument('--s3-region', type=str, default='us-east-2')
    ap.add_argument('--s3-bucket', type=str, default='audio248')
    ap.add_argument('--s3-storage-class', type=str, choices=['GLACIER', 'STANDARD', 'STANDARD-IA'], default='GLACIER')
    ap.add_argument('--description', type=str, required=True)
    ap.add_argument('--id', type=str, required=True)
    main(ap.parse_args())
