# 🔍 Web Scraping & Data Engineering Portfolio

> Production-ready web scrapers and data processing pipelines for marketplace analysis

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://www.python.org/)
[![Async](https://img.shields.io/badge/Async-aiohttp-green.svg)](https://docs.aiohttp.org/)
[![Data Processing](https://img.shields.io/badge/Data-196K%2B%20records-orange.svg)](#projects)

## 👋 About

This repository contains production-grade web scraping projects demonstrating expertise in:

- **Asynchronous Programming** - High-performance concurrent data extraction
- **Large-Scale Data Processing** - Handling 196K+ records with 99.9% success rate
- **Data Engineering** - ETL pipelines, data cleaning, and validation
- **Error Handling** - Robust retry logic and failure recovery
- **Production Code** - Clean, maintainable, well-documented solutions

## 📁 Projects

### [🚗 Drom.ru Car Listings Parser](./drom_ru_parser/)

High-performance asynchronous scraper for Russian automotive marketplace Drom.ru

**Key Achievements:**
- ⚡ Parsed **196,056+ car listings** with detailed specifications
- 🎯 **99.9% success rate** with automatic retry logic
- 📊 **33 data fields** including VIN, specs, pricing, descriptions
- ⏱️ ~2 hours processing time for entire dataset
- 🔄 Self-healing system with checkpoint recovery

**Tech Stack:**
```python
aiohttp      # Async HTTP client
asyncio      # Concurrency framework
BeautifulSoup4  # HTML parsing
pandas       # Data manipulation (196K+ rows)
openpyxl     # Excel export
```

**Skills Demonstrated:**
- Async/await patterns with connection pooling
- Dynamic HTML selector adaptation
- Multi-stage data pipeline (extraction → cleaning → export)
- Progress tracking and monitoring
- Batch processing with configurable concurrency

[→ View detailed project README](./drom_ru_parser/README.md)

## 🎯 Technical Skills

### Web Scraping
- ✅ Asynchronous request handling (aiohttp, asyncio)
- ✅ HTML/DOM parsing (BeautifulSoup4, lxml)
- ✅ Dynamic content extraction
- ✅ Rate limiting and polite crawling
- ✅ User-agent rotation and headers management
- ✅ Error recovery and retry mechanisms

### Data Engineering
- ✅ Large-scale data processing (100K+ records)
- ✅ ETL pipeline design and implementation
- ✅ Data validation and quality assurance
- ✅ Missing data handling and imputation
- ✅ Multi-format export (Excel, CSV, JSON)
- ✅ Data chunking and memory optimization

### Software Engineering
- ✅ Production-ready code architecture
- ✅ Comprehensive error handling
- ✅ Progress tracking and monitoring
- ✅ Logging and debugging
- ✅ Git version control
- ✅ Professional documentation

### DevOps & Tools
- ✅ Bash scripting for automation
- ✅ Process monitoring and management
- ✅ Performance optimization
- ✅ Resource management (memory, connections)

## 📊 Project Statistics

| Metric | Value |
|--------|-------|
| Total Records Processed | 196,056+ |
| Success Rate | 99.9% |
| Data Fields Extracted | 33 |
| Lines of Code | 2,112+ |
| Processing Speed | ~27 records/second |
| Data Completeness | 95-100% |

## 💼 Code Quality

- **Clean Architecture** - Well-organized, modular code structure
- **Error Handling** - Comprehensive exception handling and recovery
- **Documentation** - Detailed README files and inline comments
- **Version Control** - Professional git workflow with semantic commits
- **Best Practices** - Following Python PEP 8 and async best practices

## 🚀 Quick Start

### Prerequisites
```bash
Python 3.9+
pip install -r requirements.txt
```

### Run Example Parser
```bash
cd drom_ru_parser
python src/full_parser_with_retry.py
```

### Monitor Progress
```bash
./scripts/check_retry_status.sh
```

## 📈 Sample Output

```python
# Example: Car listing data structure
{
    'car_name': 'Toyota Camry',
    'year': 2020,
    'price': 2500000,
    'currency': 'RUB',
    'engine': 'бензин, 2.5 л',
    'power': '181 л.с.',
    'transmission': 'автомат',
    'drive': 'передний',
    'mileage': 45000,
    'vin': 'JTNBK40K***',
    'url': 'https://auto.drom.ru/...',
    # ... 20+ additional fields
}
```

## 🎓 Learning & Development

This portfolio demonstrates continuous learning and problem-solving:

- **Adapting to Changes** - Fixed selectors when website structure changed
- **Performance Optimization** - Improved from sequential to async processing
- **Error Recovery** - Implemented retry logic achieving 99.9% success
- **Data Quality** - Added validation ensuring 95-100% completeness
- **Scalability** - Designed for processing millions of records

## 📧 Contact

For collaboration opportunities or questions:
- GitHub: [@raimbekovm](https://github.com/raimbekovm)

---

**Note:** Projects are for educational and portfolio demonstration purposes. All scraping follows respectful practices and complies with website terms of service.

## 📄 License

This repository is available for portfolio review. For commercial use, please contact the author.
