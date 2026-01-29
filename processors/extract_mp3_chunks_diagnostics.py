import os
import subprocess 
import sys # For placeholder logging/getTempFile if NiFi utilities aren't auto-injected
import tempfile # For placeholder getTempFile

# --- Required Imports/Placeholders for NiFi Context ---
# Define the required utility functions/classes for the ExecuteDocumentPython environment
try:
    from idolnifi import *
except ImportError:
    # Placeholder definitions for a standard Python environment
    def logInfo(*args, **kwargs):
        print("INFO:", *args, **kwargs, file=sys.stderr)
    def logError(*args, **kwargs):
        print("ERROR:", *args, **kwargs, file=sys.stderr)
    def logWarn(*args, **kwargs):
        print("WARN:", *args, **kwargs, file=sys.stderr)
    def getTempFile(prefix:str = '', extn:str = '.tmp'):
        # Note: This placeholder creates a file and returns the path, but does not auto-delete.
        return tempfile.NamedTemporaryFile(prefix=prefix, suffix=extn, delete=False).name

# --- Start of the actual handler function ---
def handler(context, session, flowfile):
    logInfo("Starting optimized MPG to 30-second MP3 chunks extraction process (Diagnostic Bypass Active)...")

    # 1. Check if the flowfile is valid and has content
    if flowfile is None:
        logError("Input FlowFile is None.")
        # If flowfile is None, session.transfer will raise an error, so we just return.
        return

    # Check for the attribute containing the original source link
    source_link = flowfile.getAttribute('idol.reference')
    if not source_link:
        logError("Required attribute 'idol.reference' (source link) is missing.")
        # Attempt to use the filename as a fallback identifier
        source_link = flowfile.getAttribute('filename') or flowfile.getUuid()

    # --- CRITICAL CHANGE: DIAGNOSTIC BYPASS ---
    # We use the known-good, working source file path directly.
    # The FlowFile content reading (session.read) is skipped.
    temp_mpg_path = source_link
    
    # Use the original filename attribute for naming the output files later
    original_filename = flowfile.getAttribute('filename')
    base_filename = original_filename.rsplit('.', 1)[0] if original_filename and '.' in original_filename else flowfile.getUuid()
    
    logInfo(f"DIAGNOSTIC BYPASS: Using source file directly: {temp_mpg_path}")
    
    # Remove the original FlowFile immediately as we are not reading its content,
    # and we want to transfer the new segments instead.
    # IMPORTANT: The original FlowFile is now gone; any failure must handle cleanup.
    session.remove(flowfile)
    
    # --- END DIAGNOSTIC BYPASS ---
    
    CHUNK_DURATION = 30.0 # seconds
    segment_index = 0 
    
    while True:
        start_time = segment_index * CHUNK_DURATION
        
        # Get a temporary file path for the output MP3 segment
        temp_mp3_path = getTempFile(
            prefix=f"segment-{segment_index:03d}", 
            extn='.mp3'
        )

        try:
            logInfo(f"Executing FFmpeg for segment {segment_index} (Start: {start_time:.1f}s, Duration: {CHUNK_DURATION}s)")
            
            # FFmpeg Command: Uses the FAST seek method (-ss before -i)
            ffmpeg_command = [
                'ffmpeg', 
                '-ss', str(start_time),         # FAST SEEK: Place BEFORE -i
                '-i', temp_mpg_path,            # Input is the source MPG path
                '-t', str(CHUNK_DURATION),      # Duration of the chunk
                '-vn',                          # No video
                '-acodec', 'libmp3lame',        # MP3 encoder
                '-q:a', '2',                    # VBR Quality 2 (High)
                '-y',                           # Overwrite output
                temp_mp3_path
            ]
            
            result = subprocess.run(
                ffmpeg_command, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE, 
                check=False
            )

            if result.returncode != 0:
                # If FFmpeg fails (exit code 1): Check if a valid last segment was created.
                if os.path.exists(temp_mp3_path) and os.path.getsize(temp_mp3_path) > 1024:
                    logInfo("FFmpeg returned non-zero, but final file segment created. Assuming end of stream.")
                    # Process this final segment before breaking
                elif segment_index == 0:
                    # If the first segment failed completely, raise a critical error
                    logError(f"FFmpeg failed on the first segment. Error: {result.stderr.decode()}")
                    raise Exception(f"FFmpeg failed on first segment with exit code {result.returncode}")
                
                break # Exit the loop (end of file reached or unrecoverable error)

            # Check if a file was created and is large enough to be valid
            if not os.path.exists(temp_mp3_path) or os.path.getsize(temp_mp3_path) < 1024:
                 logWarn(f"Segment {segment_index} extraction failed (file too small/non-existent). Breaking loop.")
                 break
                 
            logInfo(f"Segment {segment_index} successfully extracted. MP3 file size: {os.path.getsize(temp_mp3_path)} bytes")
            
            # 5. Create a NEW FlowFile for the MP3 content
            mp3_flowfile = session.create()  
            
            # 6. Set attributes for the new FlowFile
            chunk_filename = f"{base_filename}_chunk_{segment_index:03d}.mp3"
            
            mp3_flowfile = session.putAttribute(mp3_flowfile, 'filename', chunk_filename)
            mp3_flowfile = session.putAttribute(mp3_flowfile, 'mime.type', 'audio/mp3')
            mp3_flowfile = session.putAttribute(mp3_flowfile, 'original.link', source_link)
            mp3_flowfile = session.putAttribute(mp3_flowfile, 'audio.chunk.start', str(start_time))
            mp3_flowfile = session.putAttribute(mp3_flowfile, 'audio.chunk.duration', str(CHUNK_DURATION))
            
            # 7. Write the MP3 content to the new FlowFile
            def write_mp3_content(outputstream):
                with open(temp_mp3_path, 'rb') as f:
                    outputstream.writeFromReadable(f) 

            session.write(mp3_flowfile, write_mp3_content)
            
            # Clean up the temporary MP3 file for the current segment
            if os.path.exists(temp_mp3_path): os.remove(temp_mp3_path)
            
            # --- CRITICAL CHANGE: IMMEDIATE TRANSFER ---
            # Transfer the segment IMMEDIATELY to success relation
            logInfo(f"Segment {segment_index} transferred to 'success' relation.")
            session.transfer(mp3_flowfile, 'success')
            
            segment_index += 1
            
        except Exception as e:
            logError(f"Critical Error during segment extraction: {e}")
            
            # Clean up temporary files on failure
            if os.path.exists(temp_mp3_path): os.remove(temp_mp3_path)
            
            # Since the original flowfile was removed, there is no failure relation for it.
            # Already successfully transferred segments remain on the success queue.
            # We simply halt the process.
            return
            
    # --- FINAL STEPS: Completion ---
    
    if segment_index == 0:
        logError("No MP3 segments were successfully created and transferred.")
    else:
        logInfo(f"MP3 chunking complete. Total of {segment_index} FlowFiles transferred to success.")
        
    logInfo("Process finished.")