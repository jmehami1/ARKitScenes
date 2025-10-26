# ARKitScenes Complete Processing Suite

**Automated pipeline to download, clean, subsample, and visualize the ARKitScenes dataset for machine learning applications.**

## 🚀 Quick Start

### 1. **Environment Setup**
```bash
cd /path/to/ARKitScenes
python3 -m venv env
source env/bin/activate
pip install pandas opencv-python-headless numpy flask
```

### 2. **Test Processing (Recommended First)**
```bash
python batch_download.py --subsample 10 --count 3 --execute
```

### 3. **Process Full Dataset**
```bash
# Process all Training scenes with 1/10 subsampling (~150GB)
python batch_download.py --subsample 10 --split Training --execute

# Process all Validation scenes
python batch_download.py --subsample 10 --split Validation --execute

# Process everything (Training + Validation)
python batch_download.py --subsample 10 --execute
```

### 4. **Visualize Results**
```bash
python viewer.py ./data
# Open http://localhost:5000 in browser
```

## 🛠️ Available Scripts

| Script | Purpose | Command |
|--------|---------|---------|
| `batch_download.py` | Batch download, clean, and subsample | `python batch_download.py [options]` |
| `viewer.py` | Web viewer for RGB-D data | `python viewer.py ./data` |
| `scripts/download_data.py` | Download individual scenes | `python scripts/download_data.py [options]` |
| `scripts/clean_matching_files.py` | Clean mismatched files | `python scripts/clean_matching_files.py [path]` |
| `scripts/scene_utils.py` | Scene verification utilities | `python scripts/scene_utils.py [path]` |

## 📊 Processing Pipeline

Each scene undergoes:

1. **Download** → RGB, depth, and intrinsics files
2. **Clean** → Remove unmatched files across modalities
3. **Subsample** → Keep every Nth frame, delete the rest

**Example transformation:**
- Before: 3,610 RGB + 3,610 depth + 3,610 intrinsics = 10,830 files
- After (1/10 subsampling): 361 RGB + 361 depth + 361 intrinsics = 1,083 files
- **Space saved: 90%**

## 📝 Command Reference

### Basic Batch Processing
```bash
# Dry run (see what would be processed)
python batch_download.py --subsample 10 --count 5

# Execute processing
python batch_download.py --subsample 10 --count 5 --execute

# Process specific split
python batch_download.py --subsample 10 --split Training --execute

# Process range of scenes
python batch_download.py --subsample 10 --start 100 --count 50 --execute
```

### Advanced Options
```bash
# Skip downloads, process existing data
python batch_download.py --subsample 10 --skip_download --execute

# Custom download directory
python batch_download.py --subsample 10 --download_dir /path/to/data --execute

# Specific assets only
python batch_download.py --subsample 10 --assets lowres_wide lowres_depth --execute

# Force reprocess all scenes
python batch_download.py --subsample 10 --force_reprocess --execute
```

### Individual Scene Operations
```bash
# Download single scene
python scripts/download_data.py --split Training --video_id 47333462 --download_dir ./data --raw_dataset_assets lowres_wide lowres_depth lowres_wide_intrinsics

# Clean single scene
python scripts/clean_matching_files.py ./data/raw/Training/47333462/ --execute

# Verify scene integrity
python scripts/scene_utils.py ./data/raw/Training/47333462/
```

### Background Processing
```bash
# Start background job
./run_batch_background.sh --split Training --count 100

# Monitor progress
./monitor_batch.sh status

# View live logs
./monitor_batch.sh tail
```

## 📁 Data Structure

### After Processing
```
data/
└── raw/
    ├── Training/
    │   ├── 40753679/
    │   │   ├── lowres_wide/          # RGB images (256x192, .png)
    │   │   ├── lowres_depth/         # Depth images (256x192, .png)
    │   │   └── lowres_wide_intrinsics/ # Camera intrinsics (.pincam)
    │   └── ...
    └── Validation/
        └── ...
```

### File Formats
- **RGB Images**: PNG, 256x192 resolution, 3 channels (RGB)
- **Depth Images**: PNG uint16 format, values in millimeters, 256x192 resolution
- **Camera Intrinsics**: Text files with format: `width height fx fy cx cy`

## 📊 Storage Requirements

| Subsampling | Training | Validation | Total | Use Case |
|-------------|----------|------------|-------|----------|
| None (1/1) | ~1.5TB | ~500GB | ~2TB | Full dataset |
| 1/5 | ~300GB | ~100GB | ~400GB | High quality |
| **1/10** | **~150GB** | **~50GB** | **~200GB** | **Recommended** |
| 1/20 | ~75GB | ~25GB | ~100GB | Storage constrained |
| 1/50 | ~30GB | ~10GB | ~40GB | Ultra minimal |

## 🔧 Web Viewer Features

### Interactive RGB-D Visualization
- **Side-by-side display**: RGB and depth images
- **Synchronized navigation**: Navigate through frame pairs
- **Colorized depth**: Viridis colormap for depth visualization
- **Frame slider**: Direct frame jumping
- **Keyboard shortcuts**: Arrow keys, Home, End

### Information Display
- **File details**: Timestamps, filenames
- **Image properties**: Dimensions, data ranges
- **Camera intrinsics**: Focal lengths, principal points
- **Depth statistics**: Min/max values in meters

## 📈 Progress Monitoring

Scripts provide real-time progress:
```
--- 42/100: 47333462 (Training) ---
Downloading 47333462...
✅ Downloaded scene 47333462
lowres_wide: 3610 files -> keeping 361 (1/10)
lowres_depth: 3610 files -> keeping 361 (1/10)
lowres_wide_intrinsics: 3610 files -> keeping 361 (1/10)
Removed 9750 files, kept 361 matched sets
✅ Success: 47333462
Progress: 42/100, rate: 2.3 scenes/min, ~25min remaining
```

## 🛡️ Safety & Error Handling

- **Dry run mode**: Default behavior shows what would be deleted
- **Resume support**: Restart from any scene number
- **Timeout protection**: 30-minute limit per scene download
- **Error recovery**: Skips failed downloads, continues processing

## 🆘 Troubleshooting

### Common Issues
```bash
# Disk full - use higher subsampling
python batch_download.py --subsample 20 --execute

# Resume from specific scene
python batch_download.py --subsample 10 --start 150 --execute

# Process existing downloads only
python batch_download.py --subsample 10 --skip_download --execute

# Check scene integrity
python scripts/scene_utils.py ./data/raw/Training/47333462/
```

### Web Viewer Issues
```bash
# Cannot load scene - verify path and file matching
ls ./data/raw/Training/47333462/

# Images not displaying - check browser console
# Slow loading - close other applications
```

## ⚡ Performance Tips

1. **Start small**: Test with `--count 3` first
2. **Monitor storage**: Check disk space regularly
3. **Use SSD**: Faster I/O improves processing speed
4. **Stable network**: Use reliable connection for downloads
5. **Batch processing**: Process in chunks if needed

## 📖 Python Usage Example

```python
from scripts.scene_utils import verify_scene_integrity

# Verify scene integrity
is_valid, info = verify_scene_integrity("/path/to/scene")
print(f"Valid: {is_valid}, Matched files: {info['matched_count']}")

# Load images manually
import cv2
rgb = cv2.imread("path/to/rgb.png")
depth = cv2.imread("path/to/depth.png", cv2.IMREAD_UNCHANGED) / 1000.0  # Convert to meters
```

---

**🎉 Complete ARKitScenes processing suite with automated subsampling and visualization!**</content>
<parameter name="filePath">/home/jmehami/Git/ARKitScenes/README_UPDATED.md