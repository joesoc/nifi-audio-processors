# NiFi Audio Processors

Python-based custom processors for **Apache NiFi**, specialized in audio/media ingestion and transformation pipelines.

## Overview

This repository contains reusable Python scripts designed for NiFi's **ExecuteDocumentPython** processor (or similar scripted processors). The focus is on handling long-form audio/video files — extracting, chunking, converting, and enriching with metadata — to prepare content for transcription, search, AI analysis, or archiving.

Current highlight: A robust MPG → 30-second MP3 chunk extractor with diagnostic bypass for troubleshooting large-file/content issues.

## Key Features

- Efficient FFmpeg-based audio extraction
- Fixed-duration chunking (configurable)
- High-quality MP3 output (VBR)
- Rich FlowFile attributes for downstream routing/metadata
- Diagnostic modes for real-world NiFi deployment issues
- Clean temporary file handling and immediate transfer of results

## Processors

### Extract MP3 Chunks from MPG (Diagnostic Bypass)

**File:** `processors/extract_mp3_chunks_diagnostic.py`

Converts a single MPEG file into sequential 30-second MP3 audio segments.

- Bypasses FlowFile content reading (uses direct file path from `idol.reference`)
- Ideal for large files or when NiFi content claiming is problematic
- Outputs one FlowFile per chunk with attributes like start time, original source, etc.

[Full detailed documentation](processors/extract_mp3_chunks_diagnostic.py) (inline comments + usage notes in the file)

## Requirements

- Apache NiFi
- FFmpeg installed on NiFi host(s)
- ExecuteDocumentPython processor configured

## Getting Started

1. Clone this repo
2. Copy the Python script to your NiFi script directory or load directly
3. Configure an ExecuteDocumentPython processor with this script as the handler
4. Ensure input FlowFiles have required attributes (`idol.reference`, `filename`)

## Future Plans

- Normal (non-bypass) mode version
- Configurable chunk duration/quality via attributes
- Additional processors: metadata enrichment, format validation, silence trimming
- Example NiFi flow templates

Contributions welcome! Open issues or PRs for new processors or improvements.

## License

MIT License — see [LICENSE](LICENSE) file.

---
Maintained by Vinay (@josepheternity) · Melbourne, Australia
