#!/usr/bin/env python3
"""
ARKitScenes Dataset Viewer - Browse Training and Validation scenes
HTML-based viewer for RGB, depth, and camera intrinsics data.

Usage:
    python viewer.py <data_root_path>
    python viewer.py ./data
"""

from flask import Flask, render_template, request, jsonify
import cv2
import numpy as np
from pathlib import Path
import io
import base64
import json
import os
import sys
import argparse

from scripts.scene_utils import verify_scene_integrity, get_matched_file_triplets, load_camera_intrinsics

app = Flask(__name__)

# Global variables
data_root = None
current_scene_path = None
file_triplets = []
scene_info = {}
available_scenes = {'Training': [], 'Validation': []}


def validate_data_structure(data_root_path):
    """Validate that the data root contains Training and Validation folders with scenes."""
    data_path = Path(data_root_path)
    
    if not data_path.exists():
        raise ValueError(f"Data root path does not exist: {data_root_path}")
    
    if not data_path.is_dir():
        raise ValueError(f"Data root path is not a directory: {data_root_path}")
    
    # Check for Training and Validation folders
    training_path = data_path / "Training"
    validation_path = data_path / "Validation"
    
    if not training_path.exists() or not training_path.is_dir():
        raise ValueError(f"Training folder not found in: {data_root_path}")
    
    if not validation_path.exists() or not validation_path.is_dir():
        raise ValueError(f"Validation folder not found in: {data_root_path}")
    
    # Find scene folders in each split
    training_scenes = []
    validation_scenes = []
    
    for item in training_path.iterdir():
        if item.is_dir() and item.name.isdigit():
            training_scenes.append(item.name)
    
    for item in validation_path.iterdir():
        if item.is_dir() and item.name.isdigit():
            validation_scenes.append(item.name)
    
    if not training_scenes:
        raise ValueError(f"No scene folders found in Training directory: {training_path}")
    
    if not validation_scenes:
        raise ValueError(f"No scene folders found in Validation directory: {validation_path}")
    
    return sorted(training_scenes), sorted(validation_scenes)


def load_scene(scene_path):
    """Load a scene and update global variables."""
    global current_scene_path, file_triplets, scene_info
    
    # Verify scene integrity
    is_valid, info = verify_scene_integrity(scene_path)
    if not is_valid:
        raise ValueError(f"Scene has mismatched files: {info}")
    
    current_scene_path = scene_path
    file_triplets = get_matched_file_triplets(scene_path)
    scene_info = info
    
    return is_valid, info


def create_depth_colormap_simple(depth_image, min_depth=None, max_depth=None):
    """Create a simple grayscale visualization of depth image."""
    # Convert depth to float and handle invalid values
    depth = depth_image.astype(np.float32)
    
    # Set depth range
    if min_depth is None:
        min_depth = np.percentile(depth[depth > 0], 1)
    if max_depth is None:
        max_depth = np.percentile(depth[depth > 0], 99)
    
    # Normalize depth to 0-255 range
    depth_normalized = np.clip((depth - min_depth) / (max_depth - min_depth), 0, 1)
    depth_uint8 = (depth_normalized * 255).astype(np.uint8)
    
    # Convert to 3-channel for consistency
    depth_rgb = cv2.applyColorMap(depth_uint8, cv2.COLORMAP_VIRIDIS)
    depth_rgb = cv2.cvtColor(depth_rgb, cv2.COLOR_BGR2RGB)
    
    return depth_rgb, min_depth, max_depth


def array_to_base64(image_array):
    """Convert numpy array to base64 string for web display."""
    # Encode image as PNG
    success, buffer = cv2.imencode('.png', cv2.cvtColor(image_array, cv2.COLOR_RGB2BGR))
    if not success:
        raise ValueError("Could not encode image")
    
    img_str = base64.b64encode(buffer).decode()
    return f"data:image/png;base64,{img_str}"


@app.route('/')
def index():
    """Main page with scene selector and image viewer."""
    return render_template('index.html', 
                         training_scenes=available_scenes['Training'],
                         validation_scenes=available_scenes['Validation'])


@app.route('/load_scene', methods=['POST'])
def load_scene_route():
    """Load a scene from the provided split and scene ID."""
    try:
        split = request.json.get('split')
        scene_id = request.json.get('scene_id')
        
        if not split or not scene_id:
            return jsonify({'error': 'Split and scene_id are required'}), 400
        
        if split not in ['Training', 'Validation']:
            return jsonify({'error': 'Invalid split. Must be Training or Validation'}), 400
        
        if scene_id not in available_scenes[split]:
            return jsonify({'error': f'Scene {scene_id} not found in {split}'}), 400
        
        scene_path = os.path.join(data_root, split, scene_id)
        
        if not os.path.exists(scene_path):
            return jsonify({'error': f'Scene path does not exist: {scene_path}'}), 400
        
        is_valid, info = load_scene(scene_path)
        
        return jsonify({
            'success': True,
            'scene_path': current_scene_path,
            'split': split,
            'scene_id': scene_id,
            'total_frames': len(file_triplets),
            'info': info
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_frame/<int:frame_idx>')
def get_frame(frame_idx):
    """Get RGB and depth images for a specific frame index."""
    try:
        if not file_triplets or frame_idx >= len(file_triplets):
            return jsonify({'error': 'Invalid frame index'}), 400
        
        rgb_path, depth_path, intrinsics_path = file_triplets[frame_idx]
        
        # Load RGB image
        rgb_image = cv2.imread(rgb_path)
        if rgb_image is None:
            return jsonify({'error': f'Could not load RGB image: {rgb_path}'}), 500
        rgb_image = cv2.cvtColor(rgb_image, cv2.COLOR_BGR2RGB)
        
        # Load depth image
        depth_image = cv2.imread(depth_path, cv2.IMREAD_UNCHANGED)
        if depth_image is None:
            return jsonify({'error': f'Could not load depth image: {depth_path}'}), 500
        
        # Convert depth from millimeters to meters
        depth_meters = depth_image.astype(np.float32) / 1000.0
        
        # Create depth colormap
        depth_colored, min_depth, max_depth = create_depth_colormap_simple(depth_meters)
        
        # Load camera intrinsics
        intrinsics = load_camera_intrinsics(intrinsics_path)
        
        # Get file information
        rgb_filename = Path(rgb_path).name
        depth_filename = Path(depth_path).name
        timestamp = rgb_filename.split('_')[-1].replace('.png', '')
        
        # Convert images to base64
        rgb_b64 = array_to_base64(rgb_image)
        depth_b64 = array_to_base64(depth_colored)
        
        return jsonify({
            'rgb_image': rgb_b64,
            'depth_image': depth_b64,
            'intrinsics': intrinsics,
            'frame_info': {
                'index': frame_idx,
                'total': len(file_triplets),
                'timestamp': timestamp,
                'rgb_filename': rgb_filename,
                'depth_filename': depth_filename,
                'rgb_shape': rgb_image.shape,
                'depth_shape': depth_image.shape,
                'depth_range': {
                    'min': float(min_depth),
                    'max': float(max_depth),
                    'unit': 'meters'
                }
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/scene_info')
def get_scene_info():
    """Get information about the currently loaded scene."""
    if not current_scene_path:
        return jsonify({'error': 'No scene loaded'}), 400
    
    return jsonify({
        'scene_path': current_scene_path,
        'total_frames': len(file_triplets),
        'scene_info': scene_info
    })


@app.route('/get_scenes')
def get_scenes():
    """Get available scenes for each split."""
    return jsonify({
        'training_scenes': available_scenes['Training'],
        'validation_scenes': available_scenes['Validation']
    })


if __name__ == '__main__':
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='ARKitScenes Dataset Viewer - Browse Training and Validation scenes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python viewer.py ./data
    python viewer.py /path/to/arkitscenes/data
        """
    )
    parser.add_argument('data_root', help='Path to data root directory containing Training and Validation folders')
    parser.add_argument('--port', type=int, default=5000, help='Port to run Flask server on (default: 5000)')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind Flask server to (default: 0.0.0.0)')
    
    args = parser.parse_args()
    
    # Validate data structure
    try:
        training_scenes, validation_scenes = validate_data_structure(args.data_root)
        available_scenes['Training'] = training_scenes
        available_scenes['Validation'] = validation_scenes
        data_root = args.data_root
        print(f"✅ Validated data structure at: {args.data_root}")
        print(f"📊 Training scenes: {len(training_scenes)}")
        print(f"📊 Validation scenes: {len(validation_scenes)}")
    except Exception as e:
        print(f"❌ Error validating data structure: {e}")
        sys.exit(1)
    
    print("🌐 Starting Flask server...")
    print(f"📱 Open http://localhost:{args.port} in your browser")
    print(f"🛑 Press Ctrl+C to stop the server")
    app.run(debug=True, host=args.host, port=args.port)