import os
import psycopg
from datetime import datetime, timezone
from mattermostdriver import Driver
from dotenv import load_dotenv
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

load_dotenv()

# Mattermost config
MM_URL = os.getenv("MM_URL")
MM_USER = os.getenv("MM_USER")
MM_PASSWORD = os.getenv("MM_PASSWORD")

# PostgreSQL config
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DB = os.getenv("PG_DB")

# Configurable API delay
API_DELAY = float(os.getenv("API_DELAY", "0.001"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100")) # Add BATCH_SIZE configuration

def get_mattermost_driver():
    return Driver({
        'url': MM_URL,
        'login_id': MM_USER,
        'password': MM_PASSWORD,
        'scheme': 'https',
        'port': 443,
        'verify': True,
        'debug': False,
        'timeout': 30,
    })

def get_pg_conn():
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )

def ensure_table_exists(conn):
    with conn.cursor() as cur:
        # Ensure TimescaleDB extension is available
        # Note: Creating the extension might require superuser privileges
        # and might be better handled as a one-time manual setup in the database.
        try:
            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
            logging.info("TimescaleDB extension ensured.")
        except psycopg.Error as e:
            logging.warning(f"Could not ensure TimescaleDB extension (this might be fine if already enabled or due to permissions): {e}")
            # If the extension creation fails due to permissions, we might need to rollback
            # any transaction started by the CREATE EXTENSION command.
            # However, psycopg2/3 in autocommit mode (default for DDL like CREATE EXTENSION)
            # might not require this, or if not in autocommit, the 'with conn:' block handles it.
            # For safety, if an error occurs, we can try to rollback.
            try:
                conn.rollback() # Attempt to rollback if in a transaction block
            except psycopg.Error as rb_e:
                logging.warning(f"Rollback attempt failed: {rb_e}")


        cur.execute("""
        CREATE TABLE IF NOT EXISTS channel_user_status (
            id SERIAL,
            timestamp TIMESTAMPTZ NOT NULL,
            team_id TEXT NOT NULL,
            team_name TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            channel_name TEXT NOT NULL,
            user_id TEXT NOT NULL,
            username TEXT NOT NULL,
            status TEXT NOT NULL,
            PRIMARY KEY (id, timestamp)
        );
        """)
        logging.info("Table 'channel_user_status' schema ensured.")

        # Convert to hypertable if not already one
        try:
            cur.execute("SELECT create_hypertable('channel_user_status', 'timestamp', if_not_exists => TRUE);")
            logging.info("Table 'channel_user_status' converted to hypertable or already is one.")
        except psycopg.Error as e:
            # Error code '42P07' is for 'duplicate_table' which create_hypertable can return if already a hypertable
            # Error code '0LP01' (undefined_object) might mean timescaledb extension is not active in this session/db
            if e.diag.sqlstate == '42P07': # table is already a hypertable
                 logging.info("Table 'channel_user_status' is already a hypertable.")
            elif 'already a hypertable' in str(e).lower(): # Check string for safety
                 logging.info("Table 'channel_user_status' is already a hypertable (detected by error string).")
            else:
                logging.error(f"Error converting table to hypertable: {e}")
                conn.rollback() # Rollback on error
                raise # Re-raise the exception to stop further processing if hypertable conversion fails

        # Set compression settings
        # Segment by columns that are often filtered together or define logical groups.
        # Order by timestamp for efficient compression and querying of recent (uncompressed) data.
        try:
            cur.execute("""
            ALTER TABLE channel_user_status
            SET (
                timescaledb.compress = 'on',
                timescaledb.compress_segmentby = 'team_id, channel_id, user_id',
                timescaledb.compress_orderby = 'timestamp DESC'
            );
            """)
            logging.info("Compression settings applied to 'channel_user_status'.")
        except psycopg.Error as e:
            # It's possible this fails if already set or if there's an issue with the columns.
            # TimescaleDB might not raise an error if settings are already identical.
            logging.warning(f"Could not apply compression settings (this might be fine if already set): {e}")
            conn.rollback() # Rollback on error

        # Add compression policy to compress data older than, for example, 7 days
        # Adjust the interval as needed for your use case.
        try:
            # Check if a policy already exists to avoid errors on re-runs.
            # This is a bit more complex as there isn't a simple IF NOT EXISTS for add_compression_policy.
            # We'll attempt to add it; if it fails because it exists, we can log it as a warning.
            cur.execute("""
            SELECT add_compression_policy('channel_user_status', INTERVAL '1 days', if_not_exists => TRUE);
            """)
            logging.info("Compression policy added or already exists for 'channel_user_status'.")
        except psycopg.Error as e:
            # A common error if the policy exists is '42710' (duplicate_object)
            if e.diag.sqlstate == '42710' or 'already has a compression policy' in str(e).lower():
                logging.info(f"Compression policy for 'channel_user_status' already exists.")
            else:
                logging.warning(f"Could not add compression policy: {e}")
                conn.rollback() # Rollback on error

        conn.commit()

def get_all_channel_members(driver, channel_id):
    page = 0
    per_page = 200
    all_members = []
    while True:
        members = driver.channels.get_channel_members(channel_id, params={'page': page, 'per_page': per_page})
        if not members:
            break
        all_members.extend(members)
        page += 1
        time.sleep(API_DELAY)  # Configurable delay
    return all_members

def main():
    logging.info("Starting Mattermost online user collection script.")
    driver = get_mattermost_driver()
    driver.login()
    logging.info("Logged in to Mattermost as %s", MM_USER)
    teams = driver.teams.get_user_teams(driver.client.userid)
    logging.info("Found %d teams.", len(teams))

    now = datetime.now(timezone.utc)

    all_user_ids_globally = set()
    records_to_process_later = []

    for team in teams:
        team_id = team['id']
        team_name = team['name']
        logging.info(f"Processing team: {team_name} ({team_id})")
        try:
            channels = driver.channels.get_channels_for_user(driver.client.userid, team_id)
            logging.info(f"Found {len(channels)} channels in team {team_name}.")
        except Exception as e:
            logging.error(f"Error fetching channels for team {team_id} ({team_name}): {e}")
            continue # Skip to next team if channels can't be fetched

        for channel in channels:
            channel_id = channel['id']
            # Use display_name if available, otherwise name, finally 'unknown'
            channel_name = channel.get('display_name') or channel.get('name', 'unknown')
            logging.debug(f"Processing channel: {channel_name} ({channel_id}) in team {team_name}")
            try:
                members = get_all_channel_members(driver, channel_id)
                channel_user_ids = {member['user_id'] for member in members}

                if not channel_user_ids:
                    logging.debug(f"No members found in channel {channel_name} ({channel_id}).")
                    continue

                all_user_ids_globally.update(channel_user_ids)
                for user_id in channel_user_ids:
                    records_to_process_later.append(
                        (now, team_id, team_name, channel_id, channel_name, user_id)
                    )
            except Exception as e:
                logging.error(f"Error processing channel {channel_id} ({channel_name}): {e}")
                continue # Skip to next channel

    user_id_to_username = {}
    user_id_to_status = {}

    if all_user_ids_globally:
        user_ids_list = list(all_user_ids_globally)
        logging.info(f"Fetching statuses and user details for {len(user_ids_list)} unique users.")
        try:
            statuses_list = driver.status.get_user_statuses_by_id(user_ids_list)
            time.sleep(API_DELAY)  # Respect API delay between global calls
            users_list = driver.users.get_users_by_ids(user_ids_list)

            user_id_to_username = {user['id']: user['username'] for user in users_list}
            user_id_to_status = {status['user_id']: status['status'] for status in statuses_list}
            logging.info("Successfully fetched statuses and user details.")
        except Exception as e:
            logging.error(f"Error fetching global user statuses/details: {e}")
            # Continue with empty maps, so 'unknown' will be used

    with get_pg_conn() as conn:
        ensure_table_exists(conn)
        with conn.cursor() as cur:
            processed_count = 0
            logging.info(f"Preparing to insert {len(records_to_process_later)} records.")
            for record_data in records_to_process_later:
                r_now, r_team_id, r_team_name, r_channel_id, r_channel_name, r_user_id = record_data

                username = user_id_to_username.get(r_user_id, 'unknown')
                status = user_id_to_status.get(r_user_id, 'unknown')

                cur.execute(
                    "INSERT INTO channel_user_status (timestamp, team_id, team_name, channel_id, channel_name, user_id, username, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (r_now, r_team_id, r_team_name, r_channel_id, r_channel_name, r_user_id, username, status)
                )
                processed_count += 1
                if processed_count % BATCH_SIZE == 0:
                    conn.commit()
                    logging.info(f"Committed batch of {BATCH_SIZE} records. Total processed: {processed_count}")

            if processed_count > 0 and processed_count % BATCH_SIZE != 0:
                conn.commit()
                logging.info(f"Committed remaining {processed_count % BATCH_SIZE} records. Total processed: {processed_count}")
            elif processed_count == 0:
                logging.info("No records were processed or inserted.")

    driver.logout()
    logging.info("Mattermost online user collection script finished.")


if __name__ == "__main__":
    main()
