# 🚗 Drom.ru Car Listings Parser

> High-performance asynchronous web scraper for Russian automotive marketplace Drom.ru

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Async](https://img.shields.io/badge/Async-aiohttp-green.svg)](https://docs.aiohttp.org/)
[![Data](https://img.shields.io/badge/Data-196K%2B%20listings-orange.svg)](#)

## 📋 Overview

A production-ready web scraping system that extracts, processes, and structures automotive listing data from Drom.ru. The project demonstrates advanced skills in asynchronous programming, data cleaning, error handling, and large-scale data processing.

### Key Features

- ⚡ **Asynchronous Architecture** - Built with `aiohttp` for concurrent HTTP requests (5-10 concurrent connections)
- 🔄 **Automatic Retry Logic** - Smart error recovery with exponential backoff (up to 3 attempts)
- 📊 **Large-Scale Processing** - Successfully parsed **196,056+ car listings** with detailed specifications
- 🧹 **Data Cleaning** - Robust data validation, normalization, and deduplication
- 💾 **Progress Tracking** - JSON-based checkpoint system for resumable operations
- 📈 **Real-time Monitoring** - Shell scripts for live progress tracking and status monitoring

## 🎯 Technical Highlights

### Async Programming
- Implemented concurrent request handling using `asyncio` and `aiohttp`
- Connection pooling with `TCPConnector` for optimal resource utilization
- Batch processing with configurable concurrency limits

### Web Scraping Expertise
- DOM parsing with BeautifulSoup4
- Dynamic HTML structure adaptation (handled site layout changes)
- Rate limiting and polite crawling practices
- User-agent rotation and request headers management

### Data Engineering
- Structured data extraction into pandas DataFrames
- Multi-file dataset management (chunked into 50K row files)
- Data quality validation and missing data handling
- Excel export with proper encoding (Windows-1251 for Cyrillic)

### Error Handling & Reliability
- Graceful failure recovery with detailed error logging
- Failed request tracking and selective retry
- Progress persistence for crash recovery
- **Success rate: 99.9%** on production run

## 📂 Project Structure

```
drom_ru_parser/
├── src/
│   ├── database_parser.py           # Initial brand/model database parser
│   ├── full_parser_with_retry.py    # Main parser with automatic retry logic
│   ├── parse_skipped_models.py      # Handler for previously failed models
│   ├── monitor_progress.py          # Real-time progress monitoring
│   └── extract_404_models.py        # 404 error handler and validator
├── data/
│   ├── raw/                         # Input data (brand/model lists)
│   └── processed/                   # Output datasets (196K+ listings)
├── scripts/
│   ├── check_retry_status.sh        # Parser status monitoring
│   └── check_skipped_status.sh      # Skipped models tracking
├── examples/                        # Sample outputs and screenshots
└── README.md
```

## 🚀 Usage

### Basic Usage

```python
# Example: Parse car listings with retry logic
import asyncio
from src.full_parser_with_retry import main

# Run the parser
asyncio.run(main())
```

### Advanced Configuration

```python
# Customize concurrency and retry behavior
CONCURRENT_REQUESTS = 5      # Parallel requests
MAX_RETRY_ATTEMPTS = 3       # Retry failed requests
TIMEOUT = 30                 # Request timeout (seconds)
```

### Monitor Progress

```bash
# Check parser status in real-time
./scripts/check_retry_status.sh

# Output example:
# ✓ Successfully processed: 187,636
# ✗ Current errors: 156
# Progress: 95.7%
# Success Rate: 99.9%
```

## 📊 Data Schema

### Extracted Fields (33 columns)

**Basic Information:**
- Brand, Model, Year, Price, Currency, URL

**Technical Specifications:**
- Engine type & volume, Power (hp), Transmission
- Drive type, Body type, Color, Mileage

**Vehicle Details:**
- VIN number, Generation, Trim level
- Previous owners, Steering wheel position

**Listing Metadata:**
- Full description, Location, Exchange availability
- Bulletin ID, Post date, View count

## 🎓 Skills Demonstrated

### Python Programming
- Object-oriented design patterns
- Async/await concurrency model
- Context managers and decorators
- Type hints and documentation

### Data Processing
- pandas for large dataset manipulation
- Data cleaning and normalization
- Missing data imputation strategies
- Excel I/O with proper encoding

### DevOps & Tools
- Git version control
- Bash scripting for automation
- Progress monitoring and logging
- Error tracking and debugging

### Problem Solving
- Adapted to website structure changes
- Resolved encoding issues (UTF-8/Windows-1251)
- Fixed critical bugs (variable scope, selector updates)
- Optimized performance bottlenecks

## 📈 Results

| Metric | Value |
|--------|-------|
| Total Listings Processed | 196,056+ |
| Unique Car Models | 707 |
| Success Rate | 99.9% |
| Data Completeness | 95-100% |
| Processing Time | ~2 hours |
| Data Quality | Production-ready |

### Sample Output Statistics

```
Engine:         99.4% filled
Power:          98.9% filled
Transmission:   99.3% filled
Drive:          99.1% filled
VIN:            89.5% filled (normal for marketplace data)
Description:    97.2% filled
```

## 🛠️ Technologies

- **Python 3.9+** - Core language
- **aiohttp** - Async HTTP client
- **asyncio** - Asynchronous I/O
- **BeautifulSoup4** - HTML parsing
- **pandas** - Data manipulation
- **openpyxl** - Excel file handling

## 📝 Requirements

```txt
aiohttp>=3.8.0
beautifulsoup4>=4.11.0
pandas>=1.5.0
openpyxl>=3.0.0
lxml>=4.9.0
```

## 🏆 Key Achievements

1. **Scaled to 196K+ records** - Efficient processing of large datasets
2. **99.9% success rate** - Robust error handling and retry logic
3. **Self-healing system** - Automatic recovery from failures
4. **Production quality** - Clean, maintainable, well-documented code
5. **Data integrity** - Comprehensive validation and quality checks

## 📧 Contact

For questions or collaboration opportunities, please reach out via GitHub.

---

**Note:** This project is for educational and portfolio demonstration purposes. Always respect website terms of service and robots.txt when web scraping.
