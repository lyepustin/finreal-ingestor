# Finreal Transaction Manager

A Python-based tool for managing banking transactions in a Supabase database. This tool provides functionality for ingesting transactions from various banks and managing transaction categories.

## Project Structure

```
.
├── src/
│   ├── db/
│   │   ├── models.py              # Pydantic models for data structures
│   │   ├── supabase.py           # Supabase database connection utility
│   │   ├── transaction_cleaner.py # Transaction deletion utility
│   │   └── transaction_ingester.py # Transaction ingestion utility
│   ├── run_transaction_cleaner.py # Script to clean transactions
│   └── run_update_database.py     # Script to update database with new transactions
├── data/
│   └── exports/                   # Directory for bank transaction exports
├── .env                          # Environment variables (create from .env.example)
├── .env.example                  # Example environment variables
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

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
   - Bank-specific credentials (BBVA, Ruralvia, Santander)

## Usage

### Updating Database with New Transactions

1. Place your bank transaction exports (CSV files) in the `data/exports` directory
2. Run the update script:
   ```bash
   python src/run_update_database.py
   ```

### Cleaning Transactions

To delete all transactions and categories for the configured user:
```bash
python src/run_transaction_cleaner.py
```

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

## Contributing

1. Create a new branch for your feature
2. Implement your changes
3. Write tests if applicable
4. Submit a pull request

## License

[Your chosen license]
