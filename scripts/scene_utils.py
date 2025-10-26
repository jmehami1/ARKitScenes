#!/usr/bin/env python3
"""
Utility functions for loading and verifying ARKitScenes data.
"""

import os
from pathlib import Path
from typing import List, Tuple, Optional


def extract_timestamp_from_filename(filename: str) -> Optional[str]:
    """Extract timestamp from ARKitScenes filename."""
    if filename.startswith('.'):
        return None
    
    # Remove extension
    name_without_ext = filename.rsplit('.', 1)[0]
    
    # Extract timestamp part (everything after the last underscore)
    if '_' in name_without_ext:
        timestamp = name_without_ext.split('_')[-1]
        return timestamp
    return None


def verify_scene_integrity(scene_path: str) -> Tuple[bool, dict]:
    """
    Verify that a scene has matching RGB, depth, and intrinsics files.
    
    Args:
        scene_path: Path to the scene directory
        
    Returns:
        Tuple of (is_valid, info_dict)
        - is_valid: True if all files match
        - info_dict: Dictionary with file counts and mismatch details
    """
    scene_path_obj = Path(scene_path)
    
    # Define directories
    rgb_dir = scene_path_obj / "ultrawide"
    depth_dir = scene_path_obj / "highres_depth" 
    intrinsics_dir = scene_path_obj / "ultrawide_intrinsics"
    
    info = {
        'rgb_count': 0,
        'depth_count': 0,
        'intrinsics_count': 0,
        'matched_count': 0,
        'missing_dirs': [],
        'unmatched_timestamps': []
    }
    
    # Check if directories exist
    for dir_path, name in [(rgb_dir, "ultrawide"), (depth_dir, "highres_depth"), (intrinsics_dir, "ultrawide_intrinsics")]:
        if not dir_path.exists():
            info['missing_dirs'].append(name)
    
    if info['missing_dirs']:
        return False, info
    
    # Get timestamps from each directory
    rgb_timestamps = set()
    depth_timestamps = set()
    intrinsics_timestamps = set()
    
    # RGB files
    for file in rgb_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.png':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                rgb_timestamps.add(timestamp)
    
    # Depth files  
    for file in depth_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.png':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                depth_timestamps.add(timestamp)
    
    # Intrinsics files
    for file in intrinsics_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.pincam':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                intrinsics_timestamps.add(timestamp)
    
    # Update counts
    info['rgb_count'] = len(rgb_timestamps)
    info['depth_count'] = len(depth_timestamps)
    info['intrinsics_count'] = len(intrinsics_timestamps)
    
    # Find common timestamps
    common_timestamps = rgb_timestamps & depth_timestamps & intrinsics_timestamps
    info['matched_count'] = len(common_timestamps)
    
    # Find unmatched timestamps
    all_timestamps = rgb_timestamps | depth_timestamps | intrinsics_timestamps
    unmatched = all_timestamps - common_timestamps
    info['unmatched_timestamps'] = sorted(list(unmatched))
    
    is_valid = len(unmatched) == 0
    return is_valid, info


def get_matched_file_triplets(scene_path: str) -> List[Tuple[str, str, str]]:
    """
    Get list of matching (RGB, depth, intrinsics) file triplets.
    
    Args:
        scene_path: Path to the scene directory
        
    Returns:
        List of tuples (rgb_file, depth_file, intrinsics_file)
    """
    scene_path_obj = Path(scene_path)
    
    # Define directories
    rgb_dir = scene_path_obj / "ultrawide"
    depth_dir = scene_path_obj / "highres_depth" 
    intrinsics_dir = scene_path_obj / "ultrawide_intrinsics"
    
    # Get timestamps and files
    rgb_files = {}
    depth_files = {}
    intrinsics_files = {}
    
    # RGB files
    for file in rgb_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.png':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                rgb_files[timestamp] = str(file)
    
    # Depth files  
    for file in depth_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.png':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                depth_files[timestamp] = str(file)
    
    # Intrinsics files
    for file in intrinsics_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.pincam':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                intrinsics_files[timestamp] = str(file)
    
    # Find common timestamps
    common_timestamps = set(rgb_files.keys()) & set(depth_files.keys()) & set(intrinsics_files.keys())
    
    # Create triplets
    triplets = []
    for timestamp in sorted(common_timestamps):
        triplets.append((
            rgb_files[timestamp],
            depth_files[timestamp], 
            intrinsics_files[timestamp]
        ))
    
    return triplets


def load_camera_intrinsics(intrinsics_file: str) -> dict:
    """
    Load camera intrinsics from .pincam file.
    
    Args:
        intrinsics_file: Path to .pincam file
        
    Returns:
        Dictionary with intrinsics parameters
    """
    with open(intrinsics_file, 'r') as f:
        line = f.readline().strip()
        values = line.split()
        
        if len(values) != 6:
            raise ValueError(f"Invalid intrinsics file format: {intrinsics_file}")
        
        return {
            'width': int(values[0]),
            'height': int(values[1]),
            'fx': float(values[2]),
            'fy': float(values[3]),
            'cx': float(values[4]),
            'cy': float(values[5])
        }


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python scene_utils.py <scene_path>")
        sys.exit(1)
    
    scene_path = sys.argv[1]
    
    # Verify scene integrity
    is_valid, info = verify_scene_integrity(scene_path)
    
    print(f"Scene: {scene_path}")
    print(f"Valid: {is_valid}")
    print(f"RGB files: {info['rgb_count']}")
    print(f"Depth files: {info['depth_count']}")
    print(f"Intrinsics files: {info['intrinsics_count']}")
    print(f"Matched triplets: {info['matched_count']}")
    
    if info['missing_dirs']:
        print(f"Missing directories: {info['missing_dirs']}")
    
    if info['unmatched_timestamps']:
        print(f"Unmatched timestamps: {len(info['unmatched_timestamps'])}")
        if len(info['unmatched_timestamps']) <= 10:
            print(f"  {info['unmatched_timestamps']}")
        else:
            print(f"  {info['unmatched_timestamps'][:5]} ... {info['unmatched_timestamps'][-5:]}")
    
    # Show sample triplets
    if is_valid:
        triplets = get_matched_file_triplets(scene_path)
        print(f"\nSample file triplets:")
        for i, (rgb, depth, intrinsics) in enumerate(triplets[:3]):
            print(f"  {i+1}. RGB: {Path(rgb).name}")
            print(f"     Depth: {Path(depth).name}")
            print(f"     Intrinsics: {Path(intrinsics).name}")