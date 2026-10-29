#!/usr/bin/env bash
# Generates a 10-second synthetic test video (moving shapes + burned-in timestamp) with ffmpeg.
# Used by CI so the repo never ships a copyrighted clip. Replace with real dashcam footage for demos.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p samples
ffmpeg -y -f lavfi -i "testsrc2=size=640x360:rate=30:duration=10" \
  -vf "drawbox=x='mod(t*60,540)':y=200:w=100:h=60:color=red@0.8:t=fill,drawtext=text='%{pts\:hms}':fontcolor=white:fontsize=24:x=10:y=10" \
  -c:v libx264 -pix_fmt yuv420p samples/demo.mp4
echo "samples/demo.mp4 ready (10 s, 30 fps, 640x360)"
