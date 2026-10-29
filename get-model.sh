#!/usr/bin/env bash
# Exports the pretrained YOLOv8n model to ONNX (opset 12, static 640x640, batch 1).
# Requires: pip install ultralytics onnx
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p models
python3 - <<'EOF'
from ultralytics import YOLO
m = YOLO("yolov8n.pt")            # downloads weights on first run
m.export(format="onnx", imgsz=640, opset=12, dynamic=False, simplify=True)
import shutil, pathlib
shutil.move("yolov8n.onnx", "models/yolov8n.onnx")
print("models/yolov8n.onnx ready")
EOF
