import pandas as pd
import data_extraction

# Converting albums,artists and tracks data into dataframes
albums_df = pd.DataFrame(data_extraction.albums_data)
artists_df = pd.DataFrame(data_extraction.artists_data)
tracks_df = pd.DataFrame(data_extraction.tracks_data)

# converting date columns in albums and tracks data into datetime objects
albums_df['release_date'] = pd.to_datetime(albums_df['release_date'])
tracks_df['track_added'] = pd.to_datetime(tracks_df['track_added'])

# Dropping duplicates under artists dataframe
artists_df.drop_duplicates(['artist_id'],inplace=True)
