# Mattermost User Status Tracking

This project tracks user status across all Mattermost teams and channels, storing the data in a PostgreSQL database for analysis and visualization.

## Features

- Tracks online status of all users in all visible teams and channels
- Stores data in PostgreSQL with timestamps for time-series analysis
- Includes a comprehensive Grafana dashboard for visualization
- Optimized for API efficiency with caching and batch operations

## Setup

### Prerequisites

- Python 3.6+
- PostgreSQL 15 database
- Mattermost account with access to the teams you want to monitor
- Grafana (for dashboard visualization)

### Installation

1. Clone this repository:
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file with your configuration:
   ```
   # Mattermost config
   MM_URL=https://your-mattermost-url.com
   MM_USER=your-mattermost-username
   MM_PASSWORD=your-mattermost-password
   
   # PostgreSQL config
   PG_HOST=localhost
   PG_PORT=5432
   PG_USER=your_pg_user
   PG_PASSWORD=your_pg_password
   PG_DB=your_pg_db
   
   # Optional: API request delay in seconds (default: 0.001)
   API_DELAY=0.001
   ```

## Usage

Run the script to collect user status data:

```bash
python main.py
```

For automated collection, set up a scheduled task or cron job to run the script at your desired interval.

### Example Cron Job (Linux/macOS)

To run every 5 minutes:

```
*/5 * * * * cd /path/to/project && /path/to/python main.py >> /path/to/logfile.log 2>&1
```

## Database Schema

The script creates a `channel_user_status` table with the following structure:

```sql
CREATE TABLE IF NOT EXISTS channel_user_status (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    team_id TEXT NOT NULL,
    team_name TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    channel_name TEXT NOT NULL,
    user_id TEXT NOT NULL,
    username TEXT NOT NULL,
    status TEXT NOT NULL
);
```