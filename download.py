import os
import json
import glob
import time
import shutil
import logging
import argparse
from io import StringIO
from functools import partial
from multiprocessing import Manager
from multiprocessing import Pool, get_context

import wandb
import yt_dlp
from tqdm import tqdm

global num_processes
num_processes = None


def download_audio(video_info, args):
    try:
        file_idx, video_info = video_info
        subfolder_idx = args.final_path
        video_info = video_info.replace(' ', '').split(',')
        to = float((video_info[2]))
        start = float(video_info[1])
    except IndexError:
        print(f"Error in {video_info}")

    outpath = os.path.join(args.root_path, subfolder_idx)
    os.makedirs(outpath, exist_ok=True)

    st = f"{int(start//3600)}:{int(start//60)-60*int(start//3600)}:{start%60}"
    dur = f"{int(to//3600)}:{int(to//60)-60*int(to//3600)}:{to%60}"
    ids = video_info[0]
    categories = [c.replace('"','') for c in video_info[3:]]

    ytdl_logger = logging.getLogger()
    log_stream = StringIO()
    logging.basicConfig(stream=log_stream, level=logging.INFO)

    ydl_opts = {
        "logger": ytdl_logger,
        "cookiefile" : f"temps/{ids}/cookies.txt",
        "ignoreerrors": True,
        "outtmpl": "temps/%(id)s/audio.%(ext)s",
        "quiet" : True,
        "format": "bestaudio/best",
        "postprocessors": [{
            "key": "FFmpegExtractAudio",
            "preferredcodec": args.format
        }],
        "postprocessor_args": ["-ar", str(args.sample_rate)],
        "external_downloader": "ffmpeg",
        "external_downloader_args": ["-ss", st, "-to", dur, "-loglevel", "quiet"]
    }
    url = f"https://www.youtube.com/watch?v={ids}"

    file_exist = os.path.isfile(os.path.join(outpath, f'{ids}.{args.format}'))

    if file_exist:
        print(f"Skipping {ids}, already exists in {outpath}")
        return None

    os.makedirs(f"temps/{ids}", exist_ok=True)
    shutil.copy(args.cookie_path, f"temps/{ids}/cookies.txt")
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            file_exist = os.path.isfile(os.path.join(outpath, f'{ids}.{args.format}'))
            info=ydl.extract_info(url, download=not file_exist)
            filename = f"{ids}.{args.format}"
            if not file_exist:
                shutil.move(os.path.join(f"temps/{ids}", f"audio.{args.format}"), os.path.join(outpath, filename))
            else:
                pass
        os.system(f"rm -rf temps/{ids}")
    except Exception as e:
        os.system(f"rm -rf temps/{ids}")
        return f"{url} - ytdl : {log_stream.getvalue()}, system : {str(e)}"
    return None


def download_audioset_split(args) -> None:
    file = open(args.metadata_path, 'r').read()

    os.makedirs(args.root_path, exist_ok=True)

    rows = file.split('\n')
    logs = []
    p = get_context("spawn").Pool(num_processes*2)
    download_audio_split = partial(download_audio, args=args)

    with tqdm(total=len(rows), leave=False) as pbar:
        for log in p.imap_unordered(download_audio_split, enumerate(rows)):
            logs.append(log)
            pbar.update()
    p.close()
    p.join()


wandb.login()
def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "--root-path",
        type=str,
        default="audioset",
        help="root path of the dataset",
    )
    argparser.add_argument(
        "--final-path",
        type=str,
        default="wavs",
        help="final path of the dataset",
    )
    argparser.add_argument(
        "--metadata-path",
        type=str,
        default=None,
        help="path to the metadata",
        required=False,
    )
    argparser.add_argument(
        "--n-jobs",
        type=int,
        default=16,
        help="number of parallel jobs",
    )
    argparser.add_argument(
        "--format",
        type=str,
        default="flac",
        help="format of the audio file (flac, m4a, wav)",
    )
    argparser.add_argument(
        "--sample-rate",
        type=int,
        default=24000,
        help="quality of the audio file (0: best, 10: worst)",
    )
    argparser.add_argument(
        "--cookie-path",
        type=str,
        default="cookies.txt",
        help="path to the cookies file",
    )

    args = argparser.parse_args()
    global num_processes
    num_processes = args.n_jobs

    wandb.init(project="FastAudioSetDownload")
    wandb.config.update(args)

    os.makedirs("temps", exist_ok=True)

    download_audioset_split(args)


if __name__ == "__main__":
    main()

