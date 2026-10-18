// B1: one consolidated record per video, matches process_dataset.py.
CREATE CONSTRAINT video_id_unique IF NOT EXISTS
FOR (v:Video) REQUIRE v.id IS UNIQUE;
CREATE RANGE INDEX video_category IF NOT EXISTS FOR (v:Video) ON (v.category);
CREATE RANGE INDEX video_views IF NOT EXISTS FOR (v:Video) ON (v.views);
CREATE RANGE INDEX video_file_size IF NOT EXISTS FOR (v:Video) ON (v.file_size_bytes);
CREATE RANGE INDEX video_length IF NOT EXISTS FOR (v:Video) ON (v.length);
