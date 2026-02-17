import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import mysql.connector


def scrape_spotify_charts():
    print("Scraping Spotify charts from Kworb...")

    url = "https://kworb.net/spotify/country/in_daily.html"
    r = requests.get(url, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table", class_="sortable")
    rows = table.find_all("tr")[1:]  # skip header

    records = []
    scrape_date = datetime.today().date()

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 6:
            continue

        # Correct columns
        chart_rank = int(cols[0].text.strip())
        artist_title = cols[2].text.strip()
        streams_raw = cols[5].text.strip().replace(",", "")

        # Split artist & track
        if " - " in artist_title:
            artist, track_name = artist_title.split(" - ", 1)
        else:
            artist = artist_title
            track_name = "Unknown"

        # Safe stream parsing
        try:
            daily_streams = int(streams_raw)
        except ValueError:
            daily_streams = 0

        records.append([
            chart_rank,
            track_name,
            artist,
            daily_streams,
            "India",
            scrape_date
        ])

    df = pd.DataFrame(records, columns=[
        "chart_rank",
        "track_name",
        "artist",
        "daily_streams",
        "country",
        "scrape_date"
    ])

    df.to_csv("spotify_raw.csv", index=False)
    print("Raw data saved: spotify_raw.csv")
    print(df.head())

    return df


def clean_data(df):
    print("Cleaning data...")

    df["track_name"].fillna("Unknown", inplace=True)
    df["artist"].fillna("Unknown", inplace=True)
    df["daily_streams"] = df["daily_streams"].astype(int)

    df.to_csv("spotify_clean.csv", index=False)
    print("Clean data saved: spotify_clean.csv")

    return df


def load_into_mysql(df):
    print("Loading data into MySQL...")

    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Root@123",
        database="music_analytics"
    )
    cursor = conn.cursor()

    # Drop old table to avoid schema mismatch
    cursor.execute("DROP TABLE IF EXISTS spotify_charts")

    cursor.execute("""
        CREATE TABLE spotify_charts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            chart_rank INT,
            track_name VARCHAR(255),
            artist VARCHAR(255),
            daily_streams BIGINT,
            country VARCHAR(50),
            scrape_date DATE
        )
    """)

    insert_sql = """
        INSERT INTO spotify_charts
        (chart_rank, track_name, artist, daily_streams, country, scrape_date)
        VALUES (%s, %s, %s, %s, %s, %s)
    """

    cursor.executemany(insert_sql, df.values.tolist())
    conn.commit()

    cursor.close()
    conn.close()
    print("Data successfully loaded into MySQL")


def main():
    df_raw = scrape_spotify_charts()
    df_clean = clean_data(df_raw)
    load_into_mysql(df_clean)


if __name__ == "__main__":
    main()

