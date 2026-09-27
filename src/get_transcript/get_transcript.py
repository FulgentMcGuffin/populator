#!/usr/bin/env python3
"""
YouTube Transcript Extractor
Simple script to extract transcripts from YouTube videos
Run: python get_transcript.py
"""

import subprocess
import sys

def install_package(package):
    """Install a package using pip"""
    subprocess.check_call([sys.executable, "-m", "pip", "install", package, "-q"])

def main():
    print("=" * 70)
    print("YouTube Transcript Extractor")
    print("=" * 70)
    print()

    # Try to import, install if needed
    try:
        from youtube_transcript_api import YouTubeTranscriptApi
    except ImportError:
        print("Installing youtube-transcript-api...")
        install_package("youtube-transcript-api")
        from youtube_transcript_api import YouTubeTranscriptApi

    # Video URL
    video_url = "https://www.youtube.com/watch?v=f0s-uvvXvWg"
    video_id = "f0s-uvvXvWg"

    print(f"Video URL: {video_url}")
    print(f"Video ID: {video_id}")
    print()

    try:
        print("Fetching transcript...")
        api = YouTubeTranscriptApi()
        transcript = api.fetch(video_id)

        # Convert to text - FetchedTranscript has different structure
        if hasattr(transcript, 'entries'):
            # It's a FetchedTranscript object
            full_text = " ".join([entry.text for entry in transcript.entries])
        else:
            # Fallback: try as iterable
            full_text = " ".join([str(entry) for entry in transcript])

        # Save to file
        output_file = "transcript.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(full_text)

        print(f"✅ SUCCESS!")
        print(f"Transcript saved to: {output_file}")
        print(f"Length: {len(full_text)} characters")
        print()
        print("First 300 characters:")
        print(full_text[:300])
        print()

    except Exception as e:
        print(f"❌ ERROR: {e}")
        print()
        print("This video may not have a transcript available.")
        print("Possible reasons:")
        print("  - No captions were uploaded by the creator")
        print("  - Captions are disabled for this video")
        print("  - The video is private or restricted")
        sys.exit(1)

if __name__ == "__main__":
    main()
