import asyncio
import os
import sys
import json
import yaml
import re # Added for sanitization
import pandas as pd
from datetime import datetime, timezone, timedelta # Added timezone
from telethon import TelegramClient, events, types
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, DocumentAttributeFilename
from tqdm import tqdm

# Load Config
def load_config():
    with open('../config.yaml', 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def print_banner():
    print(r"""
  ____                      _                 _ _____ ____ 
 |  _ \  _____      ___ __ | | ___   __ _  __| |_   _/ ___|
 | | | |/ _ \ \ /\ / / '_ \| |/ _ \ / _` |/ _` | | || |  _ 
 | |_| | (_) \ V  V /| | | | | (_) | (_| | (_| | | || |_| |
 |____/ \___/ \_/\_/ |_| |_|_|\___/ \__,_|\__,_| |_| \____|
                   
 DownloadTG - Refactored
 Author: @ikun245 / Copilot
    """)

config = load_config()

# Proxy Setup
proxy = None
if config['proxy']['enable']:
    p_type = config['proxy']['type']
    import socks
    if p_type == 'socks5':
        proxy = (socks.SOCKS5, config['proxy']['address'].split(':')[0], int(config['proxy']['address'].split(':')[1]), True, config['proxy']['user'], config['proxy']['password'])
    elif p_type == 'http':
        proxy = (socks.HTTP, config['proxy']['address'].split(':')[0], int(config['proxy']['address'].split(':')[1]), True, config['proxy']['user'], config['proxy']['password'])

# Initialize Client
api_id = config['app_id']
api_hash = config['app_hash']
phone = config['phone_number']
session_file = '../output/anon'

client = TelegramClient(session_file, api_id, api_hash, proxy=proxy)

def sanitize_filename(name):
    """Sanitize directory/file names by removing illegal characters."""
    if not name:
        return ""
    # Replace invalid characters for Windows/Linux filesystems
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Remove control characters
    name = re.sub(r'[\x00-\x1f]', '', name)
    # Limit length to avoid path parsing errors (max 255 usually, use safer limit)
    return name[:100].strip()

async def process_media_group(client, messages, base_output_dir, semaphore):
    """
    Process a group of messages (album or single).
    Creates a folder based on description and downloads content.
    """
    if not messages:
        return

    # 1. Determine Description and Date
    # Sort messages by ID to ensure consistency
    messages.sort(key=lambda m: m.id)
    main_msg = messages[0] # taking the first one for ID/Date reference
    
    # Combine text from all messages in the group or prioritize the longest/first
    description_text = ""
    for m in messages:
        if m.message:
            description_text = m.message
            break # usually only one caption in an album, or we take the first found
            
    # Fallback description if empty
    safe_desc = sanitize_filename(description_text)
    if not safe_desc:
        safe_desc = f"{main_msg.date.strftime('%Y-%m-%d')}_Msg{main_msg.id}"
    
    # Create valid folder name: "Text..."
    # Ensure uniqueness? If messages are strictly processed, ID overlap isn't an issue 
    # but description might be same for different days.
    # User asked: "every day video group (message) create a new folder named description info"
    # We will use the sanitized description.
    
    group_folder = os.path.join(base_output_dir, safe_desc)
    
    # Handle duplicate folder names by appending ID if it exists and is not this group
    # (Simplified: just append ID to folder name to be safe if desired, but user specifically asked for "named description info")
    # A safe compromise: Description_ID if duplicates occur? 
    # Let's simple check if exists. The user might want to merge, but let's assume separate events.
    # To be "safe" and distinct:
    # group_folder = os.path.join(base_output_dir, f"{safe_desc}_{main_msg.id}") 
    # But user asked strictly for "naming description info". 
    # I will attempt to use description.
    
    if not os.path.exists(group_folder):
        os.makedirs(group_folder, exist_ok=True)
        
    # 2. Save Description
    if description_text:
        try:
            with open(os.path.join(group_folder, "description.txt"), "w", encoding="utf-8") as f:
                f.write(description_text)
        except Exception as e:
            print(f"Error saving description: {e}")

    # 3. Download Media
    tasks = []
    
    async def _download_one(msg):
        if not msg.media:
            return
            
        async with semaphore:
            try:
                # Construct filename
                # If it's a file, try msg.file.name, else derived
                fname = "media"
                if msg.file:
                    if hasattr(msg.file, 'name') and msg.file.name:
                        fname = msg.file.name
                    elif hasattr(msg.file, 'ext'):
                        fname = f"{msg.id}{msg.file.ext}"
                    else:
                        fname = f"{msg.id}.unknown" # fallback
                else:
                    fname = f"{msg.id}.unknown"
                
                # Sanitize fname
                fname = sanitize_filename(fname)
                if not fname:
                    fname = f"{msg.id}.bin"
                    
                out_path = os.path.join(group_folder, fname)

                # Get expected size
                expected_size = msg.file.size if (msg.file and hasattr(msg.file, 'size')) else 0
                
                # Check existing file
                if os.path.exists(out_path):
                    stat = os.stat(out_path)
                    if expected_size > 0 and stat.st_size == expected_size:
                        # Already downloaded fully
                        return
                    else:
                        # Incomplete or different size, remove and redownload
                        # print(f"Redownloading {fname} (Size mismatch: {stat.st_size}/{expected_size})")
                        try:
                            os.remove(out_path)
                        except OSError:
                            pass

                # Retry loop
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        desc_str = f"Msg {msg.id}"
                        # Ensure directory exists before downloading
                        os.makedirs(os.path.dirname(out_path), exist_ok=True)
                        
                        with tqdm(total=expected_size, unit='B', unit_scale=True, desc=desc_str, leave=False) as pbar:
                            def progress_callback(current, total):
                                pbar.total = total
                                pbar.update(current - pbar.n)

                            await client.download_media(
                                msg, 
                                file=out_path,
                                progress_callback=progress_callback
                            )
                        
                        # Verify size after download
                        if os.path.exists(out_path):
                            stat = os.stat(out_path)
                            if expected_size > 0 and stat.st_size == expected_size:
                                break # Success
                            elif expected_size == 0 and stat.st_size > 0:
                                break # Success (we didn't know expected size)
                            else:
                                if attempt < max_retries - 1:
                                    # Only log warning if we are going to retry
                                    # print(f"\n[Warn] Size mismatch after download {fname}. Retrying...")
                                    try: os.remove(out_path)
                                    except: pass
                        else:
                             if attempt < max_retries - 1:
                                 pass # Retry if file not found
                        
                    except Exception as e:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2) # Wait before retry
                        else:
                            print(f"\n[Error] Failed Msg {msg.id} after {max_retries} attempts: {e}")

            except Exception as e:
                print(f"[Error] Critical error processing Msg {msg.id}: {e}")

    for m in messages:
        tasks.append(_download_one(m))
        
    if tasks:
        await asyncio.gather(*tasks)

async def main():
    print("Connecting to Telegram via Python...")
    await client.start(phone=phone)
    print("Successfully logged in!")

    # 1. Input: Link
    target_link = input("\nEnter Target Telegram Link (e.g. https://t.me/channel_name or username): ").strip()
    
    # 2. Input: Dates
    start_date_str = input("Enter Start Date (YYYY-MM-DD): ").strip()
    end_date_str = input("Enter End Date (YYYY-MM-DD): ").strip()
    
    start_date = None
    end_date = None
    
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        # End date: end of that day
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return

    # 3. Resolve Entity
    print(f"Resolving {target_link}...")
    try:
        entity = await client.get_entity(target_link)
        print(f"Target found: {entity.title if hasattr(entity, 'title') else entity.username} (ID: {entity.id})")
    except Exception as e:
        print(f"Error resolving entity: {e}")
        return

    # 4. Fetch Messages
    print(f"\nFetching messages from {start_date.date()} to {end_date.date()}...")
    
    found_messages = []
    # Using reverse=True means iterating from oldest to newest. 
    # offset_date in iter_messages behavior:
    # If reverse=True, offset_date is the starting point in time (going forward).
    
    try:
        async for msg in client.iter_messages(entity, offset_date=start_date, reverse=True):
            if msg.date > end_date:
                break
            
            # Simple check to ensure we are >= start_date (offset_date handles this mostly, but good to be safe)
            if msg.date < start_date: 
                continue 
                
            found_messages.append(msg)
            if len(found_messages) % 100 == 0:
                print(f"Found {len(found_messages)}...", end='\r')
                
    except Exception as e:
        print(f"\nError fetching messages: {e}")
        return

    print(f"\nTotal messages found in range: {len(found_messages)}")
    if not found_messages:
        print("No messages found in the specified range.")
        return

    # 5. Grouping Logic
    # Group messages by grouped_id (Albums)
    # Messages without grouped_id are treated as single-item groups
    
    groups = {} # grouped_id -> [msg]
    singles = [] # list of [msg] (each is a list of 1)
    
    for msg in found_messages:
        if msg.grouped_id:
            if msg.grouped_id not in groups:
                groups[msg.grouped_id] = []
            groups[msg.grouped_id].append(msg)
        else:
            singles.append([msg])
            
    all_groups = list(groups.values()) + singles
    # Sort all groups by the date of their first message
    all_groups.sort(key=lambda g: g[0].date)
    
    print(f"Identified {len(all_groups)} content groups (Albums + Singles).")

    # Selection Logic
    print("\n--- Found Content Groups ---")
    for idx, grp in enumerate(all_groups):
        # Determine description for display
        desc = ""
        for m in grp:
            if m.message:
                desc = m.message.replace('\n', ' ')[:60] # Preview
                break
        if not desc:
            desc = "<No Description>"
        
        date_str = grp[0].date.strftime('%Y-%m-%d %H:%M')
        file_count = len([m for m in grp if m.media])
        print(f"{idx+1}. [{date_str}] {desc} ({file_count} files)")

    selection = input("\nEnter numbers to download (e.g. 1,3,5-7) or 'all' [default: all]: ").strip()
    
    selected_groups = []
    if not selection or selection.lower() == 'all':
        selected_groups = all_groups
    else:
        try:
            indices = set()
            parts = selection.split(',')
            for p in parts:
                p = p.strip()
                if '-' in p:
                    start, end = map(int, p.split('-'))
                    indices.update(range(start, end + 1))
                elif p.isdigit():
                    indices.add(int(p))
            
            selected_groups = [all_groups[i-1] for i in indices if 0 < i <= len(all_groups)]
            # Restore order
            selected_groups.sort(key=lambda g: all_groups.index(g))
        except ValueError:
            print("Invalid input format. Downloading all.")
            selected_groups = all_groups

    if not selected_groups:
        print("No groups selected using current filter. Exiting.")
        return

    print(f"\nQueuing download for {len(selected_groups)} groups...")

    # 6. Download
    dl_cfg = config['download_settings']
    concurrency = dl_cfg.get('max_concurrent_downloads', 3)
    base_path = dl_cfg.get('download_path', './output/downloads')
    
    print(f"Starting download to {base_path} (Concurrency: {concurrency})...")
    
    sem = asyncio.Semaphore(concurrency)
    
    # Process groups sequentially? Or parallelize groups?
    # Parallelizing files within groups is handled in process_media_group
    # Let's process groups sequentially to keep log readable, or maybe limit group parallelism.
    # Given the requirements, downloading in parallel is key.
    # We pass the semaphore down to the file downloads. 
    # We can kick off all groups, but let the semaphore limit active file downloads.
    
    tasks = [process_media_group(client, grp, base_path, sem) for grp in selected_groups]
    
    # Use tqdm for overall progress
    for f in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Processing Groups"):
        await f

    print("\nAll operations completed!")

if __name__ == '__main__':
    print_banner()
    with client:
        client.loop.run_until_complete(main())

