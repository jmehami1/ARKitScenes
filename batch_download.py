#!/usr/bin/env python3
"""
Simple batch processing script for ARKitScenes with multiprocessing support.
Downloads scenes and applies subsampling using multiple processes.

Usage:
    python batch_download.py --subsample 10 --execute
    python batch_download.py --subsample 10 --processes 4 --execute
"""

import os
import sys
import subprocess
import csv
import time
import zipfile
import signal
import threading
import logging
import multiprocessing as mp
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime


def setup_logging(log_file=None, verbose=False):
    """Set up logging for background execution."""
    # Detect if we're running under nohup or in background
    is_background = not sys.stdout.isatty() or os.getenv('NOHUP') is not None
    
    if log_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"arkitscenes_batch_{timestamp}.log"
    
    # Set up logging
    log_level = logging.DEBUG if verbose else logging.INFO
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Set up logger
    logger = logging.getLogger('arkitscenes_batch')
    logger.setLevel(log_level)
    
    # Remove any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # File handler (always)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Console handler (only if not in background or if verbose)
    if not is_background or verbose:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger, log_file, is_background


def log_and_print(logger, message, level=logging.INFO, force_print=False):
    """Log message and optionally print to console."""
    logger.log(level, message)
    if force_print and not logger.handlers[0].__class__.__name__ == 'StreamHandler':
        print(message, flush=True)


class ProgressTracker:
    """Thread-safe progress tracker for multiprocessing."""
    
    def __init__(self, total_scenes, update_interval=2.0, logger=None, is_background=False):
        self.total_scenes = total_scenes
        self.update_interval = update_interval
        self.start_time = time.time()
        self.logger = logger
        self.is_background = is_background
        
        # Thread-safe counters
        self.lock = threading.Lock()
        self.completed = 0
        self.success_count = 0
        self.skipped_count = 0
        self.failed_downloads = []
        self.failed_processing = []
        self.successful_scenes = []  # List of (video_id, split) tuples
        
        # Progress display
        self.last_update = 0
        self.last_log_time = 0
        self.display_thread = None
        self.stop_display = False
        
        # Recent completions for rate calculation
        self.recent_completions = []
        
        # Background mode settings
        self.log_interval = 300 if is_background else 30  # 5 min vs 30 sec
        
    def start_display(self):
        """Start the progress display thread."""
        self.display_thread = threading.Thread(target=self._display_progress, daemon=True)
        self.display_thread.start()
        
    def stop_display_thread(self):
        """Stop the progress display thread."""
        self.stop_display = True
        if self.display_thread:
            self.display_thread.join(timeout=1)
    
    def update(self, result, split=None):
        """Thread-safe update of progress."""
        with self.lock:
            self.completed += 1
            self.recent_completions.append(time.time())
            
            # Keep only recent completions for rate calculation
            cutoff_time = time.time() - 60  # Last minute
            self.recent_completions = [t for t in self.recent_completions if t > cutoff_time]
            
            phase = result.get('phase', 'unknown')
            video_id = result['video_id']
            
            if result['success']:
                if phase in ['skipped', 'skipped_no_highres', 'removed_no_highres']:
                    self.skipped_count += 1
                else:
                    self.success_count += 1
                    if split:
                        self.successful_scenes.append((video_id, split))
            else:
                # Handle different failure types
                if phase in ['removed', 'removed_missing_intrinsics', 'redownload_failed', 'removal_failed']:
                    # These are special cases where scene was removed
                    self.failed_processing.append(f"{video_id} ({phase})")
                elif phase == 'download':
                    self.failed_downloads.append(video_id)
                else:
                    self.failed_processing.append(video_id)
                
                # Log failures immediately in background mode
                if self.is_background and self.logger:
                    error_msg = result.get('error', 'Unknown error')
                    self.logger.warning(f"Failed {phase}: {video_id} - {error_msg}")
    
    def _display_progress(self):
        """Background thread that updates progress display."""
        while not self.stop_display:
            current_time = time.time()
            
            # Regular progress updates
            if current_time - self.last_update >= self.update_interval:
                if not self.is_background:
                    self._print_progress()
                self.last_update = current_time
            
            # Periodic logging in background mode
            if (self.is_background and self.logger and 
                current_time - self.last_log_time >= self.log_interval):
                self._log_progress()
                self.last_log_time = current_time
            
            time.sleep(0.5)
    
    def _print_progress(self):
        """Print current progress (called by display thread)."""
        with self.lock:
            elapsed = time.time() - self.start_time
            
            # Calculate rate from recent completions
            if len(self.recent_completions) >= 2:
                recent_time_span = self.recent_completions[-1] - self.recent_completions[0]
                if recent_time_span > 0:
                    rate = (len(self.recent_completions) - 1) / recent_time_span * 60
                else:
                    rate = 0
            else:
                rate = self.completed / elapsed * 60 if elapsed > 0 else 0
            
            remaining = self.total_scenes - self.completed
            eta_seconds = remaining / rate * 60 if rate > 0 else 0
            eta_minutes = eta_seconds / 60
            
            # Progress bar
            progress_width = 30
            filled = int(progress_width * self.completed / self.total_scenes)
            bar = '█' * filled + '▒' * (progress_width - filled)
            
            percentage = self.completed / self.total_scenes * 100
            
            # Clear line and print progress
            print(f"\r\033[K📊 [{bar}] {percentage:5.1f}% | "
                  f"{self.completed:4d}/{self.total_scenes} | "
                  f"✅ {self.success_count} ⏭️ {self.skipped_count} ❌ {len(self.failed_downloads + self.failed_processing)} | "
                  f"{rate:5.1f}/min | ETA: {eta_minutes:4.0f}m", 
                  end='', flush=True)
    
    def _log_progress(self):
        """Log progress to file (for background mode)."""
        with self.lock:
            if not self.logger:
                return
                
            elapsed = time.time() - self.start_time
            rate = self.completed / elapsed * 60 if elapsed > 0 else 0
            percentage = self.completed / self.total_scenes * 100
            
            self.logger.info(
                f"Progress: {percentage:.1f}% ({self.completed}/{self.total_scenes}) | "
                f"Success: {self.success_count}, Skipped: {self.skipped_count}, "
                f"Failed: {len(self.failed_downloads + self.failed_processing)} | "
                f"Rate: {rate:.1f}/min"
            )
    
    def print_final_summary(self, interrupted=False):
        """Print final summary."""
        with self.lock:
            total_time = time.time() - self.start_time
            
            summary_lines = []
            summary_lines.append("=" * 80)
            if interrupted:
                summary_lines.append("🛑 PROCESSING INTERRUPTED")
            else:
                summary_lines.append("🏁 PROCESSING COMPLETE")
            summary_lines.append("=" * 80)
            summary_lines.append(f"Total time: {total_time/60:.1f} minutes")
            summary_lines.append(f"Scenes processed: {self.completed}")
            summary_lines.append(f"Successful: {self.success_count}")
            summary_lines.append(f"Skipped (already complete): {self.skipped_count}")
            summary_lines.append(f"Failed downloads: {len(self.failed_downloads)}")
            summary_lines.append(f"Failed processing: {len(self.failed_processing)}")
            
            actual_processed = self.success_count + self.skipped_count + len(self.failed_downloads) + len(self.failed_processing)
            if actual_processed > 0:
                summary_lines.append(f"Success rate: {(self.success_count + self.skipped_count)/actual_processed*100:.1f}%")
                if actual_processed > self.skipped_count:
                    summary_lines.append(f"Average time per scene: {total_time/(actual_processed - self.skipped_count):.1f} seconds")
            
            if self.failed_downloads:
                summary_lines.append(f"\n❌ Failed downloads: {', '.join(self.failed_downloads[:10])}")
                if len(self.failed_downloads) > 10:
                    summary_lines.append(f"   ... and {len(self.failed_downloads) - 10} more")
            
            if self.failed_processing:
                summary_lines.append(f"\n❌ Failed processing: {', '.join(self.failed_processing[:10])}")
                if len(self.failed_processing) > 10:
                    summary_lines.append(f"   ... and {len(self.failed_processing) - 10} more")
            
            # Print and log the summary
            summary_text = "\n".join(summary_lines)
            print(summary_text)
            
            if self.logger:
                for line in summary_lines:
                    if line.strip():
                        self.logger.info(line.strip())
    
    def get_stats(self):
        """Get current statistics."""
        with self.lock:
            return {
                'completed': self.completed,
                'success_count': self.success_count,
                'skipped_count': self.skipped_count,
                'failed_downloads': self.failed_downloads.copy(),
                'failed_processing': self.failed_processing.copy(),
                'successful_scenes': self.successful_scenes.copy()
            }


def validate_scene_download(scene_path, assets):
    """
    Validate that all required files for a scene are downloaded and intact.
    Returns (status, details) where status is one of:
    - 'complete': All required files present
    - 'missing_intrinsics': Has depth/wide but missing intrinsics
    - 'missing_other': Missing other required files
    - 'corrupted': Has corrupted files
    """
    scene_path = Path(scene_path)
    missing_files = []
    corrupted_files = []
    
    # Check for required asset directories/files
    expected_items = {
        'highres_depth': 'directory',
        'ultrawide': 'directory', 
        'ultrawide_intrinsics': 'directory'
    }
    
    # Add other assets if specified
    for asset in assets:
        if asset not in expected_items:
            if asset in ['confidence', 'highres_depth', 'ultrawide', 'ultrawide_intrinsics', 
                        'vga_wide', 'vga_wide_intrinsics']:
                expected_items[asset] = 'directory'
    
    for item_name, item_type in expected_items.items():
        item_path = scene_path / item_name
        
        if not item_path.exists():
            missing_files.append(item_name)
            continue
            
        if item_type == 'directory':
            # Check if directory has reasonable number of files
            if item_name in ['highres_depth', 'ultrawide']:
                files = list(item_path.glob('*.png'))
                if len(files) < 10:  # Arbitrary minimum
                    missing_files.append(f"{item_name} (only {len(files)} files)")
            elif item_name == 'ultrawide_intrinsics':
                files = list(item_path.glob('*.pincam'))
                if len(files) < 10:  # Arbitrary minimum
                    missing_files.append(f"{item_name} (only {len(files)} files)")
    
    # Check for zip files that might be corrupted
    for zip_file in scene_path.glob('*.zip'):
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                # Test the zip file
                bad_file = zf.testzip()
                if bad_file:
                    corrupted_files.append(f"{zip_file.name} (bad file: {bad_file})")
        except (zipfile.BadZipFile, zipfile.LargeZipFile):
            corrupted_files.append(zip_file.name)
    
    # Determine status
    if corrupted_files:
        return 'corrupted', {'corrupted_files': corrupted_files, 'missing_files': missing_files}
    
    if missing_files:
        # Check if only intrinsics are missing but we have depth and wide
        has_depth = (scene_path / 'highres_depth').exists()
        has_wide = (scene_path / 'ultrawide').exists()
        missing_intrinsics = 'ultrawide_intrinsics' in missing_files
        
        if has_depth and has_wide and missing_intrinsics and len(missing_files) == 1:
            return 'missing_intrinsics', {'missing_files': missing_files}
        else:
            return 'missing_other', {'missing_files': missing_files}
    
    return 'complete', {}


def should_skip_scene(video_id, split, download_dir, assets, subsample_n, quiet=True):
    """
    Check if a scene should be skipped because it's already complete.
    Returns (action, reason) where action is one of:
    - 'skip': Scene is complete, skip processing
    - 'skip_no_highres': Scene doesn't have highres_depth available, skip
    - 'redownload': Scene has depth/wide but missing intrinsics, redownload
    - 'remove': Scene is missing highres_depth (should be deleted)
    - 'process': Scene needs processing for other reasons
    """
    scene_path = Path(download_dir) / "raw" / split / video_id
    
    # First check if this scene has highres_depth available
    if 'highres_depth' in assets and not has_highres_depth_available(video_id, download_dir):
        if scene_path.exists():
            return 'remove', "Scene doesn't have highres_depth available - will be removed"
        else:
            return 'skip_no_highres', "Scene doesn't have highres_depth available"
    
    if not scene_path.exists():
        return 'process', "Scene directory doesn't exist"
    
    # Check if download is complete
    status, details = validate_scene_download(scene_path, assets)
    
    if status == 'corrupted':
        return 'process', f"Corrupted files: {', '.join(details['corrupted_files'])}"
    
    if status == 'missing_intrinsics':
        return 'redownload', "Has depth/wide but missing intrinsics - will redownload"
    
    if status == 'missing_other':
        # Check if highres_depth is missing - if so, this scene shouldn't have been downloaded
        if 'highres_depth' in assets and 'highres_depth' in details['missing_files']:
            return 'remove', f"Scene missing required highres_depth - will be removed"
        else:
            return 'process', f"Missing: {', '.join(details['missing_files'])}"
    
    # Scene is complete, but check if subsampling has been applied
    if subsample_n > 1:
        # Check if we have the expected number of subsampled files
        for dir_name in ["highres_depth", "ultrawide", "ultrawide_intrinsics"]:
            dir_path = scene_path / dir_name
            if not dir_path.exists():
                continue
                
            if dir_name == "ultrawide_intrinsics":
                files = list(dir_path.glob('*.pincam'))
            else:
                files = list(dir_path.glob('*.png'))
            
            # If we have a lot of files, subsampling probably hasn't been applied
            if len(files) > 1000:  # Arbitrary threshold
                return 'process', f"Subsampling not applied to {dir_name}"
    
    return 'skip', "Scene is complete"


def process_single_scene(args_tuple):
    """
    Process a single scene: download, clean, and subsample.
    This function is designed to be called by multiprocessing workers.
    """
    video_id, split, download_dir, assets, subsample_n, execute, skip_download, force_reprocess, quiet, redownload_attempt = args_tuple
    
    try:
        scene_path = Path(download_dir) / "raw" / split / video_id
        
        # Check if we should skip this scene
        if not force_reprocess and redownload_attempt == 0:
            action, reason = should_skip_scene(video_id, split, download_dir, assets, subsample_n, quiet)
            
            if action == 'skip':
                return {
                    'video_id': video_id,
                    'success': True,
                    'error': None,
                    'phase': 'skipped',
                    'reason': reason
                }
            elif action == 'skip_no_highres':
                return {
                    'video_id': video_id,
                    'success': True,
                    'error': None,
                    'phase': 'skipped_no_highres',
                    'reason': reason
                }
            elif action == 'remove':
                # This scene doesn't have highres_depth but was already downloaded - remove it
                if remove_scene_directory(scene_path, quiet):
                    return {
                        'video_id': video_id,
                        'success': True,
                        'error': None,
                        'phase': 'removed_no_highres',
                        'reason': reason
                    }
                else:
                    return {
                        'video_id': video_id,
                        'success': False,
                        'error': 'Failed to remove scene directory',
                        'phase': 'removal_failed'
                    }
            elif action == 'redownload':
                # This scene has depth/wide but missing intrinsics - redownload
                if not quiet:
                    print(f"🔄 Redownloading {video_id}: {reason}")
            elif action == 'remove':
                # This scene is missing intrinsics after redownload attempt - remove it
                # if remove_scene_directory(scene_path, quiet):
                #     return {
                #         'video_id': video_id,
                #         'success': False,
                #         'error': 'Scene removed - missing intrinsics after redownload',
                #         'phase': 'removed'
                #     }
                # else:
                #     return {
                #         'video_id': video_id,
                #         'success': False,
                #         'error': 'Failed to remove scene directory',
                #         'phase': 'removal_failed'
                #     }
                return {
                    'video_id': video_id,
                    'success': False,
                    'error': 'Scene has missing intrinsics after redownload - not removed',
                    'phase': 'missing_intrinsics'
                }
        
        # Download phase (always download for redownload_attempt > 0 or when not skipping)
        if not skip_download or redownload_attempt > 0:
            # Remove existing scene directory before redownload
            # if redownload_attempt > 0 and scene_path.exists():
            #     remove_scene_directory(scene_path, quiet)
            
            download_success = run_download(video_id, split, download_dir, assets, quiet)
            if not download_success:
                if redownload_attempt > 0:
                    # Second download failed - remove the scene
                    # remove_scene_directory(scene_path, quiet)
                    return {
                        'video_id': video_id,
                        'success': False,
                        'error': 'Redownload failed - scene not removed',
                        'phase': 'redownload_failed'
                    }
                else:
                    return {
                        'video_id': video_id,
                        'success': False,
                        'error': 'Download failed',
                        'phase': 'download'
                    }
            
            # After successful download, check if intrinsics are now present
            if redownload_attempt > 0:
                status, details = validate_scene_download(scene_path, assets)
                if status == 'missing_intrinsics':
                    # Still missing intrinsics after redownload - remove the scene
                    # remove_scene_directory(scene_path, quiet)
                    return {
                        'video_id': video_id,
                        'success': False,
                        'error': 'Scene has missing intrinsics after redownload - not removed',
                        'phase': 'removed_missing_intrinsics'
                    }
        
        # Processing phase
        process_success = run_clean_subsample(scene_path, subsample_n, execute, quiet)
        if not process_success:
            return {
                'video_id': video_id,
                'success': False,
                'error': 'Processing failed',
                'phase': 'processing'
            }
        
        return {
            'video_id': video_id,
            'success': True,
            'error': None,
            'phase': 'completed'
        }
        
    except Exception as e:
        return {
            'video_id': video_id,
            'success': False,
            'error': str(e),
            'phase': 'exception'
        }


def run_download(video_id, split, download_dir, assets, quiet=True):
    """Download a scene using the download script with parallel asset downloads."""
    # Use threading to download assets in parallel within the scene
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    def download_asset(asset):
        cmd = [
            sys.executable, "download_data.py",
            "--split", split,
            "--video_id", video_id, 
            "--download_dir", download_dir,
            "--raw_dataset_assets", asset
        ]
        
        try:
            if quiet:
                # Suppress output
                result = subprocess.run(cmd, timeout=900,  # 15 min per asset
                                      stdout=subprocess.DEVNULL, 
                                      stderr=subprocess.DEVNULL)
            else:
                result = subprocess.run(cmd, timeout=900)
            return result.returncode == 0
        except subprocess.TimeoutExpired:
            return False
    
    # Download assets in parallel using threads
    with ThreadPoolExecutor(max_workers=min(len(assets), 4)) as executor:  # Limit to 4 concurrent downloads per scene
        futures = {executor.submit(download_asset, asset): asset for asset in assets}
        results = []
        for future in as_completed(futures):
            asset = futures[future]
            try:
                success = future.result()
                results.append(success)
                if not quiet and not success:
                    print(f"Failed to download {asset} for {video_id}")
            except Exception as e:
                if not quiet:
                    print(f"Exception downloading {asset} for {video_id}: {e}")
                results.append(False)
    
    # Return True only if all assets downloaded successfully
    return all(results)


def run_clean_subsample(scene_path, subsample_n, execute=False, quiet=True):
    """Clean and subsample a scene."""
    # First, clean up directories and ensure matching files
    if not clean_scene_directories(scene_path, execute, quiet):
        return False
    
    # Now subsample if needed
    if subsample_n > 1:
        return subsample_scene_files(scene_path, subsample_n, execute, quiet)
    
    return True


def subsample_scene_files(scene_path, subsample_n, execute=False, quiet=True):
    """Keep every Nth file in the kept directories."""
    scene_path = Path(scene_path)
    
    # Only subsample the image directories, not intrinsics
    for dir_name in ["highres_depth", "ultrawide"]:
        dir_path = scene_path / dir_name
        if not dir_path.exists():
            continue
        
        # Get sorted files
        files = sorted([f for f in dir_path.iterdir() if f.suffix == '.png'])
        
        # Keep every Nth file
        files_to_keep = files[::subsample_n]
        files_to_remove = [f for f in files if f not in files_to_keep]
        
        if not quiet:
            print(f"{dir_name}: {len(files)} files -> keeping {len(files_to_keep)} (1/{subsample_n})")
        
        # Remove files
        for file_path in files_to_remove:
            if execute:
                file_path.unlink()
            elif not quiet:
                print(f"[DRY] Would remove: {file_path.name}")
    
    # For intrinsics, keep matching files (already handled by clean_scene_directories)
    # But if subsampling, we need to subsample intrinsics too to match
    intrinsics_dir = scene_path / "ultrawide_intrinsics"
    if intrinsics_dir.exists() and subsample_n > 1:
        intrinsics_files = sorted([f for f in intrinsics_dir.iterdir() if f.suffix == '.pincam'])
        intrinsics_to_keep = intrinsics_files[::subsample_n]
        intrinsics_to_remove = [f for f in intrinsics_files if f not in intrinsics_to_keep]
        
        if not quiet:
            print(f"ultrawide_intrinsics: {len(intrinsics_files)} files -> keeping {len(intrinsics_to_keep)} (1/{subsample_n})")
        
        for file_path in intrinsics_to_remove:
            if execute:
                file_path.unlink()
            elif not quiet:
                print(f"[DRY] Would remove: {file_path.name}")
    
    return True


def clean_scene_directories(scene_path, execute=False, quiet=True):
    """Clean scene directories: keep only highres_depth, ultrawide, ultrawide_intrinsics, and ensure matching files."""
    scene_path = Path(scene_path)
    
    # Directories to keep
    keep_dirs = {'highres_depth', 'ultrawide', 'ultrawide_intrinsics'}
    
    # Remove unwanted directories
    for item in scene_path.iterdir():
        if item.is_dir() and item.name not in keep_dirs:
            if execute:
                import shutil
                shutil.rmtree(item)
                if not quiet:
                    print(f"🗑️  Removed directory: {item.name}")
            elif not quiet:
                print(f"[DRY] Would remove directory: {item.name}")
    
    # Check if all required directories exist
    missing_dirs = []
    for dir_name in keep_dirs:
        if not (scene_path / dir_name).exists():
            missing_dirs.append(dir_name)
    
    if missing_dirs:
        if not quiet:
            print(f"⚠️  Missing required directories: {', '.join(missing_dirs)}")
        return False
    
    # Get file sets for each directory
    file_sets = {}
    for dir_name in keep_dirs:
        dir_path = scene_path / dir_name
        if dir_name == 'ultrawide_intrinsics':
            files = {f.stem for f in dir_path.glob('*.pincam')}
        else:
            files = {f.stem for f in dir_path.glob('*.png')}
        file_sets[dir_name] = files
    
    # Find common filenames across all directories
    common_files = set.intersection(*file_sets.values())
    
    if not quiet:
        print(f"📊 File counts - highres_depth: {len(file_sets['highres_depth'])}, "
              f"ultrawide: {len(file_sets['ultrawide'])}, "
              f"ultrawide_intrinsics: {len(file_sets['ultrawide_intrinsics'])}")
        print(f"✅ Common files across all directories: {len(common_files)}")
    
    # Remove files that don't have matches in all directories
    for dir_name in keep_dirs:
        dir_path = scene_path / dir_name
        files_to_remove = []
        
        for file_path in dir_path.iterdir():
            if file_path.is_file():
                if file_path.stem not in common_files:
                    files_to_remove.append(file_path)
        
        if files_to_remove:
            if not quiet:
                print(f"🗑️  Removing {len(files_to_remove)} unmatched files from {dir_name}")
            
            for file_path in files_to_remove:
                if execute:
                    file_path.unlink()
                elif not quiet:
                    print(f"[DRY] Would remove: {file_path.name}")
    
    return True


def check_scene_subfolders_empty(scene_path, assets):
    """
    Check if any required subfolders are empty.
    Returns list of empty directory names.
    """
    scene_path = Path(scene_path)
    empty_dirs = []
    
    for asset in assets:
        if asset in ['highres_depth', 'ultrawide', 'ultrawide_intrinsics']:
            dir_path = scene_path / asset
            if dir_path.exists():
                files = list(dir_path.glob('*'))
                if not files:
                    empty_dirs.append(asset)
    
    return empty_dirs


def has_highres_depth_available(video_id, download_dir):
    """Check if a scene has high-resolution depth data available."""
    metadata_file = Path(download_dir) / "raw" / "metadata.csv"
    if not metadata_file.exists():
        return True  # Can't check, assume available (will be downloaded)
    
    try:
        import csv
        with open(metadata_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['video_id'] == str(video_id):
                    # is_in_upsampling indicates if the scene has highres_depth
                    return row['is_in_upsampling'].lower() == 'true'
        
        # Video ID not found in metadata
        return False
    except Exception:
        # If we can't parse the metadata, assume not available to be safe
        return False


def remove_scene_directory(scene_path, quiet=True):
    """Remove a scene directory."""
    scene_path = Path(scene_path)
    if scene_path.exists():
        try:
            import shutil
            shutil.rmtree(scene_path)
            if not quiet:
                print(f"🗑️  Removed scene directory: {scene_path.name}")
            return True
        except Exception as e:
            if not quiet:
                print(f"❌ Failed to remove scene directory {scene_path.name}: {e}")
            return False
    return True


def load_scenes_csv(csv_file, target_split=None):
    """Load scenes from CSV file."""
    scenes = []
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if target_split is None or row['fold'] == target_split:
                scenes.append((row['video_id'], row['fold']))
    return scenes


# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle Ctrl+C gracefully."""
    global shutdown_requested
    print(f"\n🛑 Received signal {signum}. Requesting graceful shutdown...")
    print("⏳ Waiting for current processes to finish...")
    print("💡 Press Ctrl+C again to force quit (may leave processes running)")
    shutdown_requested = True
    
def force_signal_handler(signum, frame):
    """Force quit on second Ctrl+C."""
    print(f"\n💥 Force quit requested. Exiting immediately.")
    sys.exit(1)


def main():
    import argparse
    global shutdown_requested
    
    parser = argparse.ArgumentParser(
        description="Batch process ARKitScenes with multiprocessing support"
    )
    parser.add_argument("--subsample", type=int, default=10)
    parser.add_argument("--download_dir", default="./data")
    parser.add_argument("--split", choices=["Training", "Validation"], 
                       help="Specific split to process (if not provided, processes both Training and Validation)")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--count", type=int)
    parser.add_argument("--skip_download", action='store_true')
    parser.add_argument("--execute", action='store_true')
    parser.add_argument("--force_reprocess", action='store_true',
                       help="Force reprocessing even if scene appears complete")
    parser.add_argument("--validate_only", action='store_true',
                       help="Only validate existing downloads without processing")
    parser.add_argument("--verbose", "-v", action='store_true',
                       help="Show detailed output from subprocesses")
    parser.add_argument("--update_interval", type=float, default=2.0,
                       help="Progress update interval in seconds (default: 2.0)")
    parser.add_argument("--log_file", type=str, default=None,
                       help="Log file path (default: auto-generated)")
    parser.add_argument("--assets", nargs='+', 
                       default=['highres_depth', 'ultrawide', 'ultrawide_intrinsics'])
    parser.add_argument("--processes", type=int, default=None,
                       help="Number of processes to use (default: all CPU cores)")
    
    args = parser.parse_args()
    
    # Set up logging and detect background mode
    logger, log_file, is_background = setup_logging(args.log_file, args.verbose)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    # Determine number of processes
    if args.processes is None:
        num_processes = mp.cpu_count()
    else:
        num_processes = min(args.processes, mp.cpu_count())
    
    quiet = not args.verbose
    
    # Initial logging
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info("="*60)
    logger.info(f"ARKitScenes Batch Processing Started: {start_time}")
    logger.info("="*60)
    logger.info(f"Command: {' '.join(sys.argv)}")
    logger.info(f"Log file: {log_file}")
    logger.info(f"Background mode: {is_background}")
    logger.info(f"Processes: {num_processes}")
    logger.info(f"Subsample: 1/{args.subsample}")
    
    if not args.execute and not args.validate_only:
        log_and_print(logger, "🔍 DRY RUN MODE - Add --execute to actually process")
    
    if args.validate_only:
        log_and_print(logger, "🔍 VALIDATION MODE - Only checking existing downloads")
        args.skip_download = True
        args.execute = False
    
    if quiet:
        log_and_print(logger, "🔇 Quiet mode enabled - subprocess output suppressed")
        if not is_background:
            print("   Use --verbose to see detailed output")
    
    log_and_print(logger, f"🚀 Using {num_processes} processes for parallel processing")
    
    if is_background:
        log_and_print(logger, f"� Running in background mode - progress logged to {log_file}")
        log_and_print(logger, "�💡 Use 'tail -f' to monitor progress:")
        log_and_print(logger, f"   tail -f {log_file}")
    else:
        log_and_print(logger, "💡 Press Ctrl+C for graceful shutdown")
    
    # Load scenes
    csv_file = "raw/raw_train_val_splits.csv"
    if not os.path.exists(csv_file):
        logger.error(f"CSV file not found: {csv_file}")
        sys.exit(1)
    
    if args.split:
        # Process specific split
        all_scenes = load_scenes_csv(csv_file, args.split)
        split_info = args.split
    else:
        # Process both splits
        training_scenes = load_scenes_csv(csv_file, "Training")
        validation_scenes = load_scenes_csv(csv_file, "Validation")
        all_scenes = training_scenes + validation_scenes
        split_info = "Training+Validation"
    
    # Select subset
    end_idx = args.start + args.count if args.count else len(all_scenes)
    scenes = all_scenes[args.start:end_idx]
    
    log_and_print(logger, f"Processing {len(scenes)} scenes (subsample 1/{args.subsample})")
    if args.force_reprocess:
        log_and_print(logger, "⚠️  Force reprocess enabled - will reprocess all scenes")
    
    logger.info(f"Scene range: {args.start} to {end_idx-1}")
    logger.info(f"Split: {split_info}")
    logger.info(f"Assets: {', '.join(args.assets)}")
    
    os.makedirs(args.download_dir, exist_ok=True)
    
    # Initialize progress tracker
    progress = ProgressTracker(len(scenes), args.update_interval, logger, is_background)
    progress.start_display()
    
    # Prepare arguments for worker processes
    scene_args = []
    for video_id, split in scenes:
        scene_args.append((
            video_id, split, args.download_dir, args.assets, 
            args.subsample, args.execute, args.skip_download, args.force_reprocess, quiet, 0  # redownload_attempt = 0
        ))
    
    # First pass: process all scenes
    scenes_needing_redownload = []
    
    try:
        if num_processes == 1:
            # Single process mode for debugging
            logger.info("Running in single process mode...")
            for i, scene_arg in enumerate(scene_args):
                if shutdown_requested:
                    logger.warning(f"Shutdown requested. Stopping at scene {i+1}/{len(scenes)}")
                    break
                    
                result = process_single_scene(scene_arg)
                
                # Check if this scene needs redownload
                if result.get('phase') == 'removed_missing_intrinsics':
                    scenes_needing_redownload.append(scene_arg[:4])  # (video_id, split, download_dir, assets)
                
                progress.update(result, scene_arg[1])
        else:
            # Multiprocess mode
            logger.info(f"Running in multiprocess mode with {num_processes} workers...")
            with ProcessPoolExecutor(max_workers=num_processes) as executor:
                # Submit all tasks
                future_to_scene = {
                    executor.submit(process_single_scene, scene_arg): (scene_arg[0], scene_arg[1]) 
                    for scene_arg in scene_args
                }
                
                # Process completed tasks
                cancelled_count = 0
                
                for future in as_completed(future_to_scene):
                    video_id, split = future_to_scene[future]
                    
                    # Check for shutdown request
                    if shutdown_requested:
                        logger.warning("Shutdown requested. Cancelling remaining tasks...")
                        # Cancel remaining futures
                        for remaining_future in future_to_scene:
                            if not remaining_future.done():
                                cancelled = remaining_future.cancel()
                                if cancelled:
                                    cancelled_count += 1
                        
                        # Change signal handler to force quit on second Ctrl+C
                        signal.signal(signal.SIGINT, force_signal_handler)
                        logger.info(f"Waiting for {len(future_to_scene) - progress.completed - cancelled_count} running processes to complete...")
                    
                    try:
                        result = future.result(timeout=1 if shutdown_requested else None)
                        
                        # Check if this scene needs redownload
                        if result.get('phase') == 'removed_missing_intrinsics':
                            # Find the original scene args
                            for scene_arg in scene_args:
                                if scene_arg[0] == video_id:
                                    scenes_needing_redownload.append(scene_arg[:4])
                                    break
                        
                        progress.update(result, split)
                    except Exception as e:
                        if not shutdown_requested:
                            logger.error(f"Exception processing {video_id}: {e}")
                        # Create a failure result for progress tracking
                        failure_result = {
                            'video_id': video_id,
                            'success': False,
                            'error': str(e),
                            'phase': 'exception'
                        }
                        progress.update(failure_result, split)
                    
                    # Break if shutdown requested and we've processed/cancelled everything
                    if shutdown_requested and progress.completed + cancelled_count >= len(future_to_scene):
                        break
                
                if cancelled_count > 0:
                    logger.info(f"Cancelled {cancelled_count} pending tasks")
    
    except KeyboardInterrupt:
        logger.warning("Force interrupted!")
        progress.stop_display_thread()
        sys.exit(1)
    
    finally:
        # Stop progress display
        progress.stop_display_thread()
    
    # Retry phase: retry failed downloads
    failed_scenes = []
    stats = progress.get_stats()
    for video_id in stats['failed_downloads']:
        # Find the original scene args for this failed scene
        for scene_arg in scene_args:
            if scene_arg[0] == video_id:
                failed_scenes.append(scene_arg)
                break
    
    if failed_scenes and not shutdown_requested:
        log_and_print(logger, f"🔄 Retrying {len(failed_scenes)} failed downloads...")
        
        # Update progress tracker for retry phase
        retry_progress = ProgressTracker(len(failed_scenes), args.update_interval, logger, is_background)
        retry_progress.start_display()
        
        # Prepare retry arguments (force redownload)
        retry_args = []
        for scene_arg in failed_scenes:
            retry_args.append((
                scene_arg[0], scene_arg[1], scene_arg[2], scene_arg[3], 
                scene_arg[4], scene_arg[5], False, True, scene_arg[8], 1  # skip_download=False, force_reprocess=True, redownload_attempt=1
            ))
        
        try:
            if num_processes == 1:
                for i, scene_arg in enumerate(retry_args):
                    if shutdown_requested:
                        break
                    result = process_single_scene(scene_arg)
                    retry_progress.update(result, scene_arg[1])
            else:
                with ProcessPoolExecutor(max_workers=min(num_processes, len(retry_args))) as executor:
                    future_to_scene = {
                        executor.submit(process_single_scene, scene_arg): (scene_arg[0], scene_arg[1]) 
                        for scene_arg in retry_args
                    }
                    
                    for future in as_completed(future_to_scene):
                        if shutdown_requested:
                            break
                        video_id, split = future_to_scene[future]
                        try:
                            result = future.result(timeout=1 if shutdown_requested else None)
                            retry_progress.update(result, split)
                        except Exception as e:
                            logger.error(f"Exception retrying {video_id}: {e}")
                            failure_result = {
                                'video_id': video_id,
                                'success': False,
                                'error': str(e),
                                'phase': 'exception'
                            }
                            retry_progress.update(failure_result, split)
            
        except KeyboardInterrupt:
            logger.warning("Retry interrupted!")
        finally:
            retry_progress.stop_display_thread()
        
        # Check for scenes that failed again and remove them
        retry_stats = retry_progress.get_stats()
        permanently_failed = retry_stats['failed_downloads'] + [item.split(' ')[0] for item in retry_stats['failed_processing']]
        
        if permanently_failed:
            log_and_print(logger, f"🗑️  Removing {len(permanently_failed)} scenes that failed retry...")
            for video_id in permanently_failed:
                # Find the scene path
                for scene_arg in failed_scenes:
                    if scene_arg[0] == video_id:
                        scene_path = Path(scene_arg[2]) / "raw" / scene_arg[1] / video_id
                        remove_scene_directory(scene_path, quiet)
                        break
    
    # Second pass: redownload scenes that were missing intrinsics
    if scenes_needing_redownload and not shutdown_requested:
        log_and_print(logger, f"🔄 Redownloading {len(scenes_needing_redownload)} scenes missing intrinsics...")
        
        # Update progress tracker for redownload phase
        redownload_progress = ProgressTracker(len(scenes_needing_redownload), args.update_interval, logger, is_background)
        redownload_progress.start_display()
        
        # Prepare redownload arguments
        redownload_args = []
        for video_id, split, download_dir, assets in scenes_needing_redownload:
            redownload_args.append((
                video_id, split, download_dir, assets, 
                args.subsample, args.execute, False, args.force_reprocess, quiet, 1  # redownload_attempt = 1
            ))
        
        try:
            if num_processes == 1:
                for i, scene_arg in enumerate(redownload_args):
                    if shutdown_requested:
                        break
                    result = process_single_scene(scene_arg)
                    redownload_progress.update(result, scene_arg[1])
            else:
                with ProcessPoolExecutor(max_workers=min(num_processes, len(redownload_args))) as executor:
                    future_to_scene = {
                        executor.submit(process_single_scene, scene_arg): scene_arg[0] 
                        for scene_arg in redownload_args
                    }
                    
                    for future in as_completed(future_to_scene):
                        if shutdown_requested:
                            break
                        video_id, split = future_to_scene[future]
                        try:
                            result = future.result(timeout=1 if shutdown_requested else None)
                            redownload_progress.update(result, split)
                        except Exception as e:
                            logger.error(f"Exception redownloading {video_id}: {e}")
                            failure_result = {
                                'video_id': video_id,
                                'success': False,
                                'error': str(e),
                                'phase': 'exception'
                            }
                            redownload_progress.update(failure_result, split)
            
        except KeyboardInterrupt:
            logger.warning("Redownload interrupted!")
        finally:
            redownload_progress.stop_display_thread()
    
    # Final report
    progress.print_final_summary(interrupted=shutdown_requested)
    
    if failed_scenes:
        log_and_print(logger, f"🔄 Retry phase: {len(failed_scenes)} scenes retried")
    
    if scenes_needing_redownload:
        log_and_print(logger, f"🔄 Redownload phase: {len(scenes_needing_redownload)} scenes reprocessed")
    
    if args.validate_only:
        stats = progress.get_stats()
        validation_msg = (f"📊 VALIDATION SUMMARY: "
                         f"Complete: {stats['success_count'] + stats['skipped_count']}, "
                         f"Incomplete: {len(stats['failed_downloads']) + len(stats['failed_processing'])}")
        log_and_print(logger, validation_msg)
    
    # Check for empty subfolders in successful scenes and handle them
    if not shutdown_requested and not args.validate_only:
        stats = progress.get_stats()
        successful_scenes = stats['successful_scenes']
        scenes_with_empty_dirs = []
        
        for video_id, split in successful_scenes:
            scene_path = Path(args.download_dir) / "raw" / split / video_id
            empty_dirs = check_scene_subfolders_empty(scene_path, args.assets)
            if empty_dirs:
                scenes_with_empty_dirs.append((video_id, split, empty_dirs))
        
        if scenes_with_empty_dirs:
            log_and_print(logger, f"🔄 Found {len(scenes_with_empty_dirs)} scenes with empty subfolders. Attempting redownload...")
            
            # Redownload these scenes
            redownload_args = []
            for video_id, split, empty_dirs in scenes_with_empty_dirs:
                redownload_args.append((
                    video_id, split, args.download_dir, args.assets,
                    args.subsample, args.execute, False, True, quiet, 2  # redownload_attempt=2
                ))
            
            # Process redownloads
            empty_redownload_progress = ProgressTracker(len(redownload_args), args.update_interval, logger, is_background)
            empty_redownload_progress.start_display()
            
            try:
                if num_processes == 1:
                    for scene_arg in redownload_args:
                        if shutdown_requested:
                            break
                        result = process_single_scene(scene_arg)
                        empty_redownload_progress.update(result, scene_arg[1])
                else:
                    with ProcessPoolExecutor(max_workers=min(num_processes, len(redownload_args))) as executor:
                        future_to_scene = {executor.submit(process_single_scene, scene_arg): (scene_arg[0], scene_arg[1]) for scene_arg in redownload_args}
                        
                        for future in as_completed(future_to_scene):
                            if shutdown_requested:
                                break
                            video_id, split = future_to_scene[future]
                            try:
                                result = future.result(timeout=1 if shutdown_requested else None)
                                empty_redownload_progress.update(result, split)
                            except Exception as e:
                                logger.error(f"Exception redownloading {video_id}: {e}")
                                failure_result = {
                                    'video_id': video_id,
                                    'success': False,
                                    'error': str(e),
                                    'phase': 'exception'
                                }
                                empty_redownload_progress.update(failure_result, split)
            
            finally:
                empty_redownload_progress.stop_display_thread()
            
            # Check which ones are still empty and remove them
            empty_redownload_stats = empty_redownload_progress.get_stats()
            successful_redownloads = empty_redownload_stats['successful_scenes']
            still_empty = []
            
            for video_id, split, original_empty_dirs in scenes_with_empty_dirs:
                if (video_id, split) not in [(vid, spl) for vid, spl in successful_redownloads]:
                    # Redownload failed
                    still_empty.append((video_id, split))
                else:
                    # Check if still empty
                    scene_path = Path(args.download_dir) / "raw" / split / video_id
                    empty_dirs = check_scene_subfolders_empty(scene_path, args.assets)
                    if empty_dirs:
                        still_empty.append((video_id, split))
            
            if still_empty:
                log_and_print(logger, f"🗑️ Removing {len(still_empty)} scenes that remain empty after redownload...")
                for video_id, split in still_empty:
                    scene_path = Path(args.download_dir) / "raw" / split / video_id
                    remove_scene_directory(scene_path, quiet)
    
    if shutdown_requested:
        log_and_print(logger, "💡 To resume processing, run the same command again.")
        log_and_print(logger, "   The script will automatically skip completed scenes.")
        sys.exit(130)  # Standard exit code for Ctrl+C
    
    # Final logging
    end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Processing completed: {end_time}")
    logger.info("="*60)


if __name__ == "__main__":
    main()