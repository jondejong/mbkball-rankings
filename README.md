# NCAA Men's Basketball Rankings Combiner

A Python application that fetches and combines NCAA NET Rankings and KenPom ratings into a single CSV file for analysis.

## Description

This tool scrapes data from two popular college basketball ranking systems:
- **NET Rankings** - NCAA's official ranking system (from NCAA.com)
- **KenPom Ratings** - Advanced analytics and efficiency ratings (from KenPom.com)

It intelligently matches teams between both sources, handling name variations and abbreviations, then outputs a combined CSV file with:
- Rankings from both systems
- Team records and conference information
- KenPom advanced metrics (Net Rating, Offensive Rating, Defensive Rating, Adjusted Tempo, Strength of Schedule)
- NET quadrant records (Q1, Q2, Q3, Q4)

## Prerequisites

- Python 3.14 or higher
- pyenv (for Python version management)
- pipenv (for dependency management)

## Installation

1. Clone the repository:
```bash
git clone git@github.com:jondejong/mbkball-rankings.git
cd mbkball-rankings
```

2. Set the Python version using pyenv:
```bash
pyenv local 3.14.2
```

3. Install dependencies using pipenv:
```bash
pipenv install
```

## Usage

Run the script using pipenv:

```bash
pipenv run python ncaa_rankings_combiner.py
```

Or activate the virtual environment first:

```bash
pipenv shell
python ncaa_rankings_combiner.py
```

The script will:
1. Fetch current NET Rankings from NCAA.com
2. Fetch current KenPom ratings from KenPom.com
3. Match teams between both sources
4. Generate `ncaa_combined_rankings.csv` with the combined data

## Output

The generated CSV file includes the following columns:
- **KenPom_Rank** - KenPom ranking
- **NET_Rank** - NCAA NET ranking
- **Team** - Team name
- **Conference** - Conference abbreviation
- **Record** - Win-loss record
- **Net_Rating** - KenPom net efficiency rating
- **Off_Rating** - Offensive efficiency rating
- **Def_Rating** - Defensive efficiency rating
- **Adj_Tempo** - Adjusted tempo
- **SOS** - Strength of schedule
- **Q1, Q2, Q3, Q4** - NET quadrant records

The CSV is formatted to open correctly in Excel, with win-loss records displayed as text (not dates).

## Dependencies

- `requests` - HTTP library for fetching web data
- `beautifulsoup4` - HTML parsing library

See `Pipfile` for complete dependency list.