# UPSC Topper Scraper

This scraper automates the enrichment of UPSC topper profiles with detailed bios, strategies, and insights using LLM and Google search data.

## Features
- Fetches research snippets from Google using Serper API
- Generates rich, story-format strategy and bio using LLM (Groq API)
- Stores results in MongoDB with proper schema mapping
- Handles markdown formatting for frontend rendering
- Validates and logs errors for debugging

## Usage
1. **Install dependencies:**
   ```sh
   pnpm install
   ```
2. **Configure environment:**
   Create a `.env` file in the `scraper/` directory with:
   ```env
   MONGO_URL=your_mongo_url
   GROQ_API_KEY=your_groq_api_key
   GROQ_API_URL=your_groq_api_url
   SERPER_API_KEY=your_serper_api_key
   DB_NAME=toppersjournal
   COLLECTION=toppers
   CONCURRENCY=2
   TEST_LIMIT=10
   MODEL_NAME=llama3-8b
   ```
3. **Run the scraper:**
   ```sh
   node scraper.js
   ```

## Output
- Updates MongoDB documents with `bio`, `strategy` (markdown), and `insights` fields
- Logs errors and raw LLM output for debugging

## Customization
- Adjust prompt and validation logic in `scraper.js` for different output styles
- Change concurrency and limits in `.env` for performance tuning

## Troubleshooting
- Check logs for invalid JSON or API errors
- Ensure all API keys and URLs are correct
- Validate MongoDB connection and schema

## License
MIT
