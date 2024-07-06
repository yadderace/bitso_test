# Directories
INPUT_DIR = 'data/input'
OUTPUT_DIR = 'data/output'
SANDBOX_DIR = 'data/sandbox'

# URLs Sources
CSV_URLs = ['https://raw.githubusercontent.com/IMARVI/sr_de_challenge/main/event_sample_data.csv',
           'https://raw.githubusercontent.com/IMARVI/sr_de_challenge/main/user_id_sample_data.csv',
           'https://raw.githubusercontent.com/IMARVI/sr_de_challenge/main/withdrawals_sample_data.csv']
ZIP_URLs = ['https://github.com/IMARVI/sr_de_challenge/raw/main/deposit_sample_data.csv.zip']

# Source Tables
DEPOSIT_EVENTS = 'deposit_sample_data.csv'
WITHDRAWAL_EVENTS = 'withdrawals_sample_data.csv'
EVENTS_SAMPLE = 'event_sample_data.csv'
USER_SAMPLE = 'user_id_sample_data.csv'

# Final Tables
DIMENSION_USER = 'dim_users.csv'
FACT_ACTIVE_USERS = 'fact_active_users.csv'
FACT_SYSTEM_ACTIVITY = 'fact_system_activity.csv'


# API URL
API_ORDER_BOOK = 'https://stage.bitso.com/api/v3/order_book/'