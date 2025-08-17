# Finreal Transaction Manager

A Python-based tool for managing banking transactions in a Supabase database. This tool provides functionality for ingesting transactions from various banks (BBVA, Caixa, Ruralvia) and managing transaction categories.

## Project Structure

```
.
├── src/
│   ├── db/
│   │   ├── models.py                    # Pydantic models for data structures
│   │   ├── supabase.py                  # Supabase database connection utility
│   │   ├── transaction_cleaner.py       # Transaction deletion utility
│   │   ├── transaction_ingester.py      # Transaction ingestion utility
│   │   └── historical_transaction_ingester.py # Historical data ingestion
│   ├── scrapers/
│   │   ├── bbva_scraper.py             # BBVA bank scraper
│   │   ├── caixa_scraper.py            # Caixa bank scraper
│   │   └── ruralvia_scraper.py         # Ruralvia bank scraper
│   ├── manual/
│   │   ├── process_manual_files_bbva.py      # Manual BBVA file processing
│   │   ├── process_manual_files_caixa.py     # Manual Caixa file processing
│   │   ├── process_manual_files_ruralvia.py  # Manual Ruralvia file processing
│   │   ├── run_historical_ingestion.py       # Historical data ingestion runner
│   │   ├── run_historical_ingestion_caixa.py # Caixa historical ingestion
│   │   └── run_transaction_cleaner.py        # Transaction cleaner runner
│   ├── run_bbva_scraper.py             # BBVA scraper runner
│   ├── run_caixa_scraper.py            # Caixa scraper runner
│   ├── run_ruralvia_scraper.py         # Ruralvia scraper runner
│   ├── run_update_database.py          # Database update script
│   └── dev_runner_test.py              # Development testing utility
├── data/                               # Directory for bank transaction exports
├── .env                                # Environment variables (create from .env.example)
├── .env.example                        # Example environment variables
├── requirements.txt                     # Python dependencies
├── setup.py                            # Package setup configuration
└── README.md                           # This file
```

## Ignore Patterns

The following directories and files are ignored:

- `# personal info` - Contains sensitive personal information
- `data/` - Contains transaction data exports
- `src/edge_profile/` - Edge profile related functionality

## Setup

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Copy `.env.example` to `.env` and fill in your credentials:
   ```bash
   cp .env.example .env
   ```

4. Configure your environment variables in `.env`:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase service role key
   - `USER_ID`: Your user ID in the system
   - Bank-specific credentials (BBVA, Caixa, Ruralvia)

## Usage

### Bank Scrapers

Run individual bank scrapers:
```bash
python src/run_bbva_scraper.py
python src/run_caixa_scraper.py
python src/run_ruralvia_scraper.py
```

### Manual File Processing

Process manual transaction files:
```bash
python src/manual/process_manual_files_bbva.py
python src/manual/process_manual_files_caixa.py
python src/manual/process_manual_files_ruralvia.py
```

### Historical Data Ingestion

Run historical data ingestion:
```bash
python src/manual/run_historical_ingestion.py
python src/manual/run_historical_ingestion_caixa.py
```

### Updating Database with New Transactions

1. Place your bank transaction exports (CSV files) in the `data/exports` directory
2. Run the update script:
   ```bash
   python src/run_update_database.py
   ```

### Cleaning Transactions

To delete all transactions and categories for the configured user:
```bash
python src/manual/run_transaction_cleaner.py
```

The transaction cleaner uses SQL optimization to delete records efficiently in a single operation rather than batch processing. This makes the deletion process significantly faster and more reliable.

## Database Schema

The system uses the following main tables:
- `banks`: Stores bank information
- `accounts`: Stores account information linked to banks
- `transactions`: Stores transaction records
- `transaction_categories`: Stores transaction categorization
- `categories`: Stores category definitions
- `subcategories`: Stores subcategory definitions
- `transaction_rules`: Stores rules for automatic categorization

## Security Notes

- Never commit your `.env` file
- Store sensitive credentials securely
- Use service role key for database operations
- Implement proper error handling and logging
- The `data/` directory and `src/edge_profile/` contain sensitive information and should not be committed

## Contributing

1. Create a new branch for your feature
2. Implement your changes
3. Write tests if applicable
4. Submit a pull request

## License

[Your chosen license]
