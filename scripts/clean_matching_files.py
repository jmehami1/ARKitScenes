#!/usr/bin/env python3
"""
Script to ensure matching RGB, depth, and intrinsics files.
Removes any files that don't have corresponding matches across all three modalities.
"""

import os
import sys
from pathlib import Path


def extract_timestamp_from_filename(filename):
    """Extract timestamp from filename (e.g., '47333462_57352.271.png' -> '57352.271')"""
    if filename.startswith('.'):
        return None
    
    # Remove extension
    name_without_ext = filename.rsplit('.', 1)[0]
    
    # Extract timestamp part (everything after the last underscore)
    if '_' in name_without_ext:
        timestamp = name_without_ext.split('_')[-1]
        return timestamp
    return None


def find_matching_files(scene_path):
    """
    Find matching files across RGB, depth, and intrinsics directories.
    Returns sets of timestamps for each modality.
    """
    scene_path = Path(scene_path)
    
    # Define directories
    rgb_dir = scene_path / "lowres_wide"
    depth_dir = scene_path / "lowres_depth" 
    intrinsics_dir = scene_path / "lowres_wide_intrinsics"
    
    # Check if directories exist
    for dir_path, name in [(rgb_dir, "RGB"), (depth_dir, "depth"), (intrinsics_dir, "intrinsics")]:
        if not dir_path.exists():
            print(f"ERROR: {name} directory not found: {dir_path}")
            return None, None, None, None
    
    # Get timestamps from each directory
    rgb_timestamps = set()
    depth_timestamps = set()
    intrinsics_timestamps = set()
    
    # RGB files
    rgb_files = {}
    for file in rgb_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.png':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                rgb_timestamps.add(timestamp)
                rgb_files[timestamp] = file
    
    # Depth files  
    depth_files = {}
    for file in depth_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.png':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                depth_timestamps.add(timestamp)
                depth_files[timestamp] = file
    
    # Intrinsics files
    intrinsics_files = {}
    for file in intrinsics_dir.iterdir():
        if file.is_file() and file.suffix.lower() == '.pincam':
            timestamp = extract_timestamp_from_filename(file.name)
            if timestamp:
                intrinsics_timestamps.add(timestamp)
                intrinsics_files[timestamp] = file
    
    return rgb_timestamps, depth_timestamps, intrinsics_timestamps, (rgb_files, depth_files, intrinsics_files)


def clean_scene(scene_path, dry_run=True):
    """
    Clean a scene by removing unmatched files.
    
    Args:
        scene_path: Path to the scene directory
        dry_run: If True, only report what would be deleted without actually deleting
    """
    print(f"\n{'='*60}")
    print(f"Processing scene: {scene_path}")
    print(f"{'='*60}")
    
    # Find matching files
    rgb_timestamps, depth_timestamps, intrinsics_timestamps, file_maps = find_matching_files(scene_path)
    
    if rgb_timestamps is None or depth_timestamps is None or intrinsics_timestamps is None or file_maps is None:
        return False
    
    rgb_files, depth_files, intrinsics_files = file_maps
    
    # Find common timestamps (intersection of all three sets)
    common_timestamps = rgb_timestamps & depth_timestamps & intrinsics_timestamps
    
    # Find files to remove
    rgb_to_remove = rgb_timestamps - common_timestamps
    depth_to_remove = depth_timestamps - common_timestamps  
    intrinsics_to_remove = intrinsics_timestamps - common_timestamps
    
    # Report statistics
    print(f"RGB files: {len(rgb_timestamps)}")
    print(f"Depth files: {len(depth_timestamps)}")
    print(f"Intrinsics files: {len(intrinsics_timestamps)}")
    print(f"Common timestamps: {len(common_timestamps)}")
    print(f"Files to remove - RGB: {len(rgb_to_remove)}, Depth: {len(depth_to_remove)}, Intrinsics: {len(intrinsics_to_remove)}")
    
    if len(rgb_to_remove) == 0 and len(depth_to_remove) == 0 and len(intrinsics_to_remove) == 0:
        print("✅ All files already match! No cleanup needed.")
        return True
    
    # Remove unmatched files
    total_removed = 0
    
    # Remove RGB files without matches
    for timestamp in rgb_to_remove:
        file_path = rgb_files[timestamp]
        if dry_run:
            print(f"[DRY RUN] Would remove RGB: {file_path}")
        else:
            print(f"Removing RGB: {file_path}")
            file_path.unlink()
        total_removed += 1
    
    # Remove depth files without matches
    for timestamp in depth_to_remove:
        file_path = depth_files[timestamp]
        if dry_run:
            print(f"[DRY RUN] Would remove Depth: {file_path}")
        else:
            print(f"Removing Depth: {file_path}")
            file_path.unlink()
        total_removed += 1
    
    # Remove intrinsics files without matches
    for timestamp in intrinsics_to_remove:
        file_path = intrinsics_files[timestamp]
        if dry_run:
            print(f"[DRY RUN] Would remove Intrinsics: {file_path}")
        else:
            print(f"Removing Intrinsics: {file_path}")
            file_path.unlink()
        total_removed += 1
    
    action = "Would remove" if dry_run else "Removed"
    print(f"\n{action} {total_removed} unmatched files")
    print(f"Remaining matched sets: {len(common_timestamps)}")
    
    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python clean_matching_files.py <scene_path> [--execute]")
        print("")
        print("Examples:")
        print("  # Dry run (default)")
        print("  python clean_matching_files.py ~/arkitscenes_data/raw/Training/47333462/")
        print("")
        print("  # Actually delete unmatched files")
        print("  python clean_matching_files.py ~/arkitscenes_data/raw/Training/47333462/ --execute")
        print("")
        print("  # Process all scenes in a directory")
        print("  python clean_matching_files.py ~/arkitscenes_data/raw/Training/ --execute")
        sys.exit(1)
    
    scene_path = Path(sys.argv[1]).expanduser().resolve()
    dry_run = "--execute" not in sys.argv
    
    if dry_run:
        print("🔍 DRY RUN MODE - No files will be deleted")
        print("Add --execute flag to actually delete unmatched files")
    else:
        print("⚠️  EXECUTE MODE - Files will be permanently deleted!")
    
    # Check if it's a single scene or directory of scenes
    if (scene_path / "lowres_wide").exists():
        # Single scene
        clean_scene(scene_path, dry_run)
    else:
        # Directory of scenes
        print(f"Processing all scenes in: {scene_path}")
        scene_dirs = [d for d in scene_path.iterdir() if d.is_dir() and (d / "lowres_wide").exists()]
        
        if not scene_dirs:
            print(f"No scene directories found in {scene_path}")
            sys.exit(1)
        
        success_count = 0
        for scene_dir in sorted(scene_dirs):
            if clean_scene(scene_dir, dry_run):
                success_count += 1
        
        print(f"\n{'='*60}")
        print(f"Processed {success_count}/{len(scene_dirs)} scenes successfully")


if __name__ == "__main__":
    main()