#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Query Generation and Transformation Pipeline

This script provides a complete pipeline for generating queries from XES event logs
and transforming them into JSON format with random symbol assignments.

Features:
    - Generate query patterns from XES log files
    - Extract sequences of specified lengths from log traces
    - Transform queries to JSON format with symbol assignments
    - Configurable query lengths and sample sizes

Usage:
    # Generate queries from XES files and convert to JSON
    python3 generate_queries.py --mode both --input-dir input --lengths 3,10 --samples 100
    
    # Only generate .q query files
    python3 generate_queries.py --mode generate --input-dir input --lengths 3,10 --samples 100
    
    # Only convert existing .q files to JSON
    python3 generate_queries.py --mode transform --query-dir queries --output-dir queries_json

Arguments:
    --mode: Operation mode ('generate', 'transform', or 'both')
    --input-dir: Directory containing .xes files (default: 'input')
    --query-dir: Directory for .q query files (default: 'queries')
    --output-dir: Directory for .jsonl output files (default: 'queries_json')
    --lengths: Comma-separated list of query lengths (default: '3,10')
    --samples: Number of samples per length (default: 100)

Output Formats:
    .q files: LENGTH,EVENT1,EVENT2,...
    .jsonl files: {"log_name": "...", "pattern": {"eventsWithSymbols": [...]}}
"""

import argparse
import json
import os
import random
from typing import List, Tuple
from statistics import mean, stdev

try:
    from pm4py.objects.log.importer.xes import importer as xes_import_factory
    PM4PY_AVAILABLE = True
except ImportError:
    PM4PY_AVAILABLE = False
    print("Warning: pm4py not available. Query generation from XES files will not work.")


# Symbol options for query transformation
SYMBOLS = ["_", "*", "||", "+"]


# ============================================================================
# QUERY GENERATION FROM XES FILES
# ============================================================================

def generate_query_file(lengths: List[int], samples_per_length: int, log, log_filename: str, output_dir: str):
    """
    Generate query file from an XES event log.
    
    For each specified length, extracts 'samples_per_length' random sequences
    from traces in the log that are at least that long.
    
    Args:
        lengths: List of sequence lengths to generate
        samples_per_length: Number of query samples to generate per length
        log: Parsed XES log object from pm4py
        log_filename: Name of the source log file (used for output naming)
        output_dir: Directory where .q files will be written
    
    Returns:
        Path to the generated query file
    """
    os.makedirs(output_dir, exist_ok=True)
    query_file_path = os.path.join(output_dir, log_filename + ".q")
    
    total_queries = 0
    with open(query_file_path, "w", encoding="utf-8") as file:
        for length in lengths:
            queries_generated = 0
            attempts = 0
            max_attempts = samples_per_length * 100  # Prevent infinite loops
            
            while queries_generated < samples_per_length and attempts < max_attempts:
                attempts += 1
                # Pick a random trace
                trace_idx = random.randint(0, len(log) - 1)
                trace = log[trace_idx]
                
                # Check if trace is long enough
                if len(trace) >= length:
                    # Extract event names for the first 'length' events
                    event_names = [event["concept:name"] for event in trace][:length]
                    # Write to file: LENGTH,EVENT1,EVENT2,...
                    file.write(str(length) + "," + ",".join(event_names) + "\n")
                    queries_generated += 1
                    total_queries += 1
            
            if queries_generated < samples_per_length:
                print(f"  Warning: Only generated {queries_generated}/{samples_per_length} queries "
                      f"for length {length} (insufficient long traces)")
    
    return query_file_path, total_queries


def generate_queries_from_xes_files(input_dir: str, query_dir: str, lengths: List[int], 
                                    samples_per_length: int, force: bool = False):
    """
    Process all XES files in input directory and generate query files.
    
    Args:
        input_dir: Directory containing .xes files
        query_dir: Directory where .q query files will be written
        lengths: List of query lengths to generate
        samples_per_length: Number of samples per length
        force: If True, regenerate even if query file already exists
    
    Returns:
        Number of query files generated
    """
    if not PM4PY_AVAILABLE:
        print("Error: pm4py is required for query generation. Install with: pip install pm4py")
        return 0
    
    if not os.path.isdir(input_dir):
        print(f"Input directory does not exist: {input_dir}")
        return 0
    
    xes_files = [f for f in os.listdir(input_dir) if f.endswith('.xes')]
    if not xes_files:
        print(f"No .xes files found in {input_dir}")
        return 0
    
    print(f"Found {len(xes_files)} XES file(s) in {input_dir}")
    print(f"Generating queries with lengths {lengths}, {samples_per_length} samples per length\n")
    
    files_generated = 0
    for xes_file in sorted(xes_files):
        query_file_path = os.path.join(query_dir, xes_file + ".q")
        
        if os.path.exists(query_file_path) and not force:
            print(f"Skipping {xes_file} (query file already exists)")
            continue
        
        print(f"Processing: {xes_file}")
        try:
            # Import XES log
            log_path = os.path.join(input_dir, xes_file)
            log = xes_import_factory.apply(log_path)
            print(f"  Loaded log with {len(log)} traces")
            
            # Generate queries
            output_path, total_queries = generate_query_file(
                lengths, samples_per_length, log, xes_file, query_dir
            )
            print(f"  Generated {total_queries} queries -> {output_path}\n")
            files_generated += 1
            
        except Exception as e:
            print(f"  Error processing {xes_file}: {e}\n")
            continue
    
    return files_generated


# ============================================================================
# QUERY TRANSFORMATION TO JSON
# ============================================================================

def transform_query_line(line: str) -> dict:
    """
    Parse a single query line and return the pattern object with symbols.
    
    Input format: LENGTH,EVENT1,EVENT2,...
    We use the events for ordering/positions and assign random symbols.
    
    Symbol assignment rules:
    - Majority of events get "_" (underscore)
    - At most 2 events get non-underscore symbols ("+", "*", "||")
    - Ensures semantic variety in query patterns
    
    Args:
        line: A line from a .q file
    
    Returns:
        Dictionary with "eventsWithSymbols" list, or None if line is invalid
    """
    line = line.strip()
    if not line:
        return None
    
    parts = line.split(",")
    if len(parts) < 2:
        return None
    
    # First part is length, rest are event names
    events = parts[1:]
    
    # Build initial pattern with all "_" symbols
    pattern_events = []
    for idx, event_name in enumerate(events):
        event_name = event_name.strip()
        if event_name == "":
            continue
        pattern_events.append({
            "name": event_name,
            "position": idx,
            "symbol": "_",
        })
    
    if not pattern_events:
        return None
    
    # Assign non-underscore symbols to at most 2 events
    # Ensure majority remain "_"
    num_events = len(pattern_events)
    max_non_underscore = min(2, (num_events - 1) // 2)
    
    if max_non_underscore > 0:
        # Randomly select 1 to max_non_underscore positions
        num_to_change = random.randint(1, max_non_underscore)
        positions_to_change = random.sample(range(num_events), num_to_change)
        non_underscore_symbols = [s for s in SYMBOLS if s != "_"]
        
        for pos in positions_to_change:
            pattern_events[pos]["symbol"] = random.choice(non_underscore_symbols)
    
    return {"eventsWithSymbols": pattern_events}


def transform_query_file(src_path: str, dst_path: str) -> int:
    """
    Transform a single .q query file into .jsonl format.
    
    Args:
        src_path: Path to input .q file
        dst_path: Path to output .jsonl file
    
    Returns:
        Number of queries written
    """
    # Derive log_name from filename
    basename = os.path.basename(src_path)
    # Strip .q extension
    log_name = basename[:-2] if basename.endswith('.q') else basename
    # Also strip .xes if present (e.g., 'bpi_2017.xes.q' -> 'bpi_2017')
    if log_name.endswith('.xes'):
        log_name = log_name[:-4]
    
    written = 0
    with open(src_path, "r", encoding="utf-8") as src, \
         open(dst_path, "w", encoding="utf-8") as dst:
        
        for line in src:
            pattern = transform_query_line(line)
            if pattern is None:
                continue
            
            # Create JSON object
            obj = {
                "log_name": log_name,
                "pattern": pattern,
            }
            dst.write(json.dumps(obj) + "\n")
            written += 1
    
    return written


def transform_all_query_files(query_dir: str, output_dir: str) -> Tuple[int, int]:
    """
    Transform all .q files in query directory to .jsonl format.
    
    Args:
        query_dir: Directory containing .q files
        output_dir: Directory where .jsonl files will be written
    
    Returns:
        Tuple of (files_processed, total_queries_transformed)
    """
    if not os.path.isdir(query_dir):
        print(f"Query directory does not exist: {query_dir}")
        return 0, 0
    
    os.makedirs(output_dir, exist_ok=True)
    
    q_files = [f for f in os.listdir(query_dir) if f.endswith('.q')]
    if not q_files:
        print(f"No .q files found in {query_dir}")
        return 0, 0
    
    print(f"Found {len(q_files)} .q file(s) in {query_dir}")
    print(f"Transforming to JSON format with random symbols\n")
    
    files_processed = 0
    total_queries = 0
    
    for q_file in sorted(q_files):
        src_path = os.path.join(query_dir, q_file)
        dst_filename = q_file + ".jsonl"
        dst_path = os.path.join(output_dir, dst_filename)
        
        try:
            written = transform_query_file(src_path, dst_path)
            print(f"Transformed {written} queries: {q_file} -> {dst_filename}")
            files_processed += 1
            total_queries += written
        except Exception as e:
            print(f"Error processing {q_file}: {e}")
            continue
    
    return files_processed, total_queries


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Generate queries from XES logs and transform to JSON format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full pipeline: generate and transform
  python3 generate_queries.py --mode both --input-dir input --lengths 3,10 --samples 100
  
  # Only generate .q files from XES logs
  python3 generate_queries.py --mode generate --input-dir input
  
  # Only transform existing .q files to JSON
  python3 generate_queries.py --mode transform --query-dir queries
  
  # Custom configuration
  python3 generate_queries.py --mode both --lengths 3,5,10,15 --samples 50 --force
        """
    )
    
    parser.add_argument(
        '--mode',
        choices=['generate', 'transform', 'both'],
        default='both',
        help='Operation mode: generate .q files, transform to JSON, or both (default: both)'
    )
    parser.add_argument(
        '--input-dir',
        default='input',
        help='Directory containing .xes files (default: input)'
    )
    parser.add_argument(
        '--query-dir',
        default='queries',
        help='Directory for .q query files (default: queries)'
    )
    parser.add_argument(
        '--output-dir',
        default='queries_json',
        help='Directory for .jsonl output files (default: queries_json)'
    )
    parser.add_argument(
        '--lengths',
        default='3,10',
        help='Comma-separated list of query lengths (default: 3,10)'
    )
    parser.add_argument(
        '--samples',
        type=int,
        default=100,
        help='Number of query samples per length (default: 100)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Force regeneration of existing query files'
    )
    
    args = parser.parse_args()
    
    # Parse lengths
    try:
        lengths = [int(x.strip()) for x in args.lengths.split(',')]
    except ValueError:
        print(f"Error: Invalid lengths format '{args.lengths}'. Use comma-separated integers.")
        return 1
    
    # Convert to absolute paths
    input_dir = os.path.abspath(args.input_dir)
    query_dir = os.path.abspath(args.query_dir)
    output_dir = os.path.abspath(args.output_dir)
    
    print("=" * 70)
    print("QUERY GENERATION AND TRANSFORMATION PIPELINE")
    print("=" * 70)
    print(f"Mode: {args.mode}")
    print(f"Input directory: {input_dir}")
    print(f"Query directory: {query_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Query lengths: {lengths}")
    print(f"Samples per length: {args.samples}")
    print("=" * 70)
    print()
    
    # Execute based on mode
    if args.mode in ['generate', 'both']:
        print("STEP 1: Generating queries from XES files")
        print("-" * 70)
        files_generated = generate_queries_from_xes_files(
            input_dir, query_dir, lengths, args.samples, args.force
        )
        print(f"Summary: Generated query files from {files_generated} XES log(s)")
        print()
    
    if args.mode in ['transform', 'both']:
        print("STEP 2: Transforming queries to JSON format")
        print("-" * 70)
        files_processed, total_queries = transform_all_query_files(query_dir, output_dir)
        print()
        print(f"Summary: Processed {files_processed} file(s), transformed {total_queries} queries")
        print()
    
    print("=" * 70)
    print("Pipeline completed successfully!")
    print("=" * 70)
    
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
