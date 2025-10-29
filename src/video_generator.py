import os
from datetime import datetime, timedelta
from moviepy.editor import VideoFileClip, TextClip, CompositeVideoClip
from database_setup import get_db_connection

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")

# --- Configuration for the Video Report ---
SCORE_THRESHOLD = 85  # The score below which inspections are included
VIDEO_RESULT_LIMIT = 5  # The number of results to show in the video

def create_weekly_report():
    """
    Generates a video report of the 5 lowest scores from the past 7 days.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True) # Use dictionary cursor for easy column access

    # Query for the 5 worst scores in the last 7 days
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    cursor.execute("""
        SELECT r.name, i.score
        FROM inspections i
        JOIN restaurants r ON i.establishment_id = r.establishment_id AND i.establishment_path = r.path
        WHERE i.score < %s AND i.inspection_date >= %s
        ORDER BY i.score ASC, i.inspection_date DESC
        LIMIT %s
    """, (SCORE_THRESHOLD, seven_days_ago, VIDEO_RESULT_LIMIT))
    
    results = cursor.fetchall()
    conn.close()

    if not results:
        print("No recent low scores found to generate a video.")
        return

    # --- Video Generation ---
    # NOTE: You must have 'template.mp4' in your data/ directory.
    template_path = os.path.join(DATA_DIR, "template.mp4")
    background_clip = VideoFileClip(template_path).subclip(0, 15) # Use first 15 seconds
    
    clips = [background_clip]
    start_time = 1 # Start text after 1 second

    title_clip = TextClip("This Week's Lowest Scores", fontsize=50, color='white', font='Arial-Bold').set_pos('center').set_duration(2).set_start(start_time)
    clips.append(title_clip)
    start_time += 3

    for row in results:
        text = f"{row['name']}: {row['score']}"
        txt_clip = TextClip(text, fontsize=40, color='yellow', font='Arial')
        txt_clip = txt_clip.set_pos('center').set_duration(2).set_start(start_time)
        clips.append(txt_clip)
        start_time += 2.5 # Each score appears for 2s with a 0.5s gap

    output_path = os.path.join(DATA_DIR, "weekly_report.mp4")
    final_video = CompositeVideoClip(clips)
    final_video.write_videofile(output_path, fps=24)

    print(f"Video report '{output_path}' has been generated.")

if __name__ == "__main__":
    create_weekly_report()