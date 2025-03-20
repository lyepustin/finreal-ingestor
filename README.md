# Banking Transaction Scraper

A Python-based automation tool for extracting banking transactions from various web portals using Selenium. This tool automates the process of logging into banking systems, scraping transaction data, and storing it in a Supabase database.

## Project Structure

```
.
├── src/
│   ├── models.py           # Pydantic models for data structures
│   ├── scrapers/
│   │   ├── base.py        # Base scraper class
│   │   └── banks/         # Bank-specific scraper implementations
│   └── db/
│       └── supabase.py    # Supabase database connection utility
├── .env                    # Environment variables (create from .env.example)
├── .env.example           # Example environment variables
├── requirements.txt       # Python dependencies
└── README.md             # This file
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

3. Copy `.env.example` to `.env` and fill in your Supabase credentials:
   ```bash
   cp .env.example .env
   ```

4. Configure your environment variables in `.env`:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase anonymous key
   - `HEADLESS`: Set to "true" for production, "false" for development
   - `CHROME_DRIVER_PATH`: Optional custom ChromeDriver path

## Usage

To implement a new bank scraper:

1. Create a new file in `src/scrapers/banks/` for your bank
2. Inherit from `BaseBankScraper` and implement the required methods:
   - `login(credentials: dict) -> bool`
   - `fetch_transactions(from_date=None, to_date=None) -> List[Transaction]`

Example:
```python
from ..base import BaseBankScraper
from ...models import Transaction

class MyBankScraper(BaseBankScraper):
    def login(self, credentials: dict) -> bool:
        # Implement login logic
        pass

    def fetch_transactions(self, from_date=None, to_date=None) -> List[Transaction]:
        # Implement transaction fetching logic
        pass
```

## Security Notes

- Never commit your `.env` file
- Store sensitive credentials securely
- Use headless mode in production
- Implement proper error handling and logging
- Consider rate limiting to avoid being blocked

## Contributing

1. Create a new branch for your feature
2. Implement your changes
3. Write tests if applicable
4. Submit a pull request

## License

[Your chosen license]
