#!/usr/bin/env python3
"""
NCAA Basketball Rankings Combiner
Fetches NET Rankings and KenPom ratings, matches teams, and outputs a combined CSV.

Usage:
    python ncaa_rankings_combiner.py

Output:
    ncaa_combined_rankings.csv - Combined rankings with data from both sources
"""

import requests
from bs4 import BeautifulSoup
import csv
import re
from difflib import SequenceMatcher
from datetime import datetime

# Headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Known name mappings: NET name -> KenPom name
# These handle the systematic differences between the two sources
NAME_MAPPINGS = {
    # State qualifiers in parentheses
    "St. John's (NY)": "St. John's",
    "Miami (FL)": "Miami FL",
    "Saint Mary's (CA)": "Saint Mary's",
    "Miami (OH)": "Miami OH",
    "St. Thomas (MN)": "St. Thomas",
    "LMU (CA)": "Loyola Marymount",
    
    # Southern California
    "Southern California": "USC",
    
    # Common abbreviations
    "UConn": "Connecticut",
    "UNI": "Northern Iowa",
    "USC": "USC",  # Same
    "UCF": "UCF",  # Same
    "UNLV": "UNLV",  # Same
    "UTEP": "UTEP",  # Same
    "UTSA": "UTSA",  # Same
    "SMU": "SMU",  # Same
    "TCU": "TCU",  # Same
    "BYU": "BYU",  # Same
    "LSU": "LSU",  # Same
    "VCU": "VCU",  # Same
    "UAB": "UAB",  # Same
    "FIU": "FIU",  # Same
    "LIU": "LIU",  # Same
    "NJIT": "NJIT",  # Same
    "SIUE": "SIUE",  # Same
    "ETSU": "East Tennessee St.",
    "NIU": "Northern Illinois",
    "SFA": "Stephen F. Austin",
    "UIW": "Incarnate Word",
    "FGCU": "Florida Gulf Coast",
    "UNCW": "UNC Wilmington",
    "UMBC": "UMBC",  # Same
    "UIC": "Illinois Chicago",
    
    # Abbreviated state names
    "Fla. Atlantic": "Florida Atlantic",
    "South Fla.": "South Florida",
    "Middle Tenn.": "Middle Tennessee",
    "Western Ky.": "Western Kentucky",
    "Southern Ill.": "Southern Illinois",
    "Southern Miss.": "Southern Miss",
    "Northern Colo.": "Northern Colorado",
    "Northern Ariz.": "Northern Arizona",
    "Eastern Mich.": "Eastern Michigan",
    "Western Mich.": "Western Michigan",
    "Central Mich.": "Central Michigan",
    "Eastern Wash.": "Eastern Washington",
    "Eastern Ky.": "Eastern Kentucky",
    "Eastern Ill.": "Eastern Illinois",
    "Western Ill.": "Western Illinois",
    "Southern Ind.": "Southern Indiana",
    "Northern Ky.": "Northern Kentucky",
    "Western Caro.": "Western Carolina",
    "Central Ark.": "Central Arkansas",
    "Southeast Mo. St.": "Southeast Missouri",
    "North Ala.": "North Alabama",
    "West Ga.": "West Georgia",
    "Ga. Southern": "Georgia Southern",
    "Central Conn. St.": "Central Connecticut",
    "Southern U.": "Southern",
    
    # St. vs Saint variations
    "St. Bonaventure": "St. Bonaventure",  # Same
    "Saint Louis": "Saint Louis",  # Same
    "Saint Joseph's": "Saint Joseph's",  # Same
    "Saint Peter's": "Saint Peter's",  # Same
    "Saint Francis": "Saint Francis",  # Same
    
    # Michigan St. etc.
    "Michigan St.": "Michigan St.",  # Same
    "Iowa St.": "Iowa St.",  # Same
    "Ohio St.": "Ohio St.",  # Same
    "Penn St.": "Penn St.",  # Same
    "Kansas St.": "Kansas St.",  # Same
    "Oklahoma St.": "Oklahoma St.",  # Same
    "Arizona St.": "Arizona St.",  # Same
    "Oregon St.": "Oregon St.",  # Same
    "Washington St.": "Washington St.",  # Same
    "Colorado St.": "Colorado St.",  # Same
    "Utah St.": "Utah St.",  # Same
    "Boise St.": "Boise St.",  # Same
    "Fresno St.": "Fresno St.",  # Same
    "San Diego St.": "San Diego St.",  # Same
    "San Jose St.": "San Jose St.",  # Same
    "Idaho St.": "Idaho St.",  # Same
    "Portland St.": "Portland St.",  # Same
    "Montana St.": "Montana St.",  # Same
    "Weber St.": "Weber St.",  # Same
    "Sacramento St.": "Sacramento St.",  # Same
    "Murray St.": "Murray St.",  # Same
    "Wichita St.": "Wichita St.",  # Same
    "Illinois St.": "Illinois St.",  # Same
    "Indiana St.": "Indiana St.",  # Same
    "Missouri St.": "Missouri St.",  # Same
    "Kent St.": "Kent St.",  # Same
    "Ball St.": "Ball St.",  # Same
    "Cleveland St.": "Cleveland St.",  # Same
    "Youngstown St.": "Youngstown St.",  # Same
    "Wright St.": "Wright St.",  # Same
    "Jacksonville St.": "Jacksonville St.",  # Same
    "Kennesaw St.": "Kennesaw St.",  # Same
    "Georgia St.": "Georgia St.",  # Same
    "Arkansas St.": "Arkansas St.",  # Same
    "Texas St.": "Texas St.",  # Same
    "Tarleton St.": "Tarleton St.",  # Same
    "Morehead St.": "Morehead St.",  # Same
    "Tennessee St.": "Tennessee St.",  # Same
    "Alabama St.": "Alabama St.",  # Same
    "Alcorn St.": "Alcorn St.",  # Alcorn
    "Grambling St.": "Grambling St.",  # Grambling
    "Jackson St.": "Jackson St.",  # Same
    "Norfolk St.": "Norfolk St.",  # Same
    "Coppin St.": "Coppin St.",  # Same
    "Morgan St.": "Morgan St.",  # Same
    "Delaware St.": "Delaware St.",  # Same
    "Chicago St.": "Chicago St.",  # Same
    
    # Shortened names
    "NC State": "N.C. State",
    "N.C. A&T": "North Carolina A&T",
    "N.C. Central": "North Carolina Central",
    "App State": "Appalachian St.",
    "Ole Miss": "Mississippi",
    "Pitt": "Pittsburgh",
    "UMass": "Massachusetts",
    
    # Sam Houston variations
    "Sam Houston": "Sam Houston St.",
    
    # Col. of Charleston
    "Col. of Charleston": "Charleston",
    
    # A&M variations  
    "Texas A&M": "Texas A&M",  # Same
    "A&M-Corpus Christi": "Texas A&M Corpus Chris",
    "Florida A&M": "Florida A&M",  # Same
    "Alabama A&M": "Alabama A&M",  # Same
    "East Texas A&M": "East Texas A&M",  # Same
    "Prairie View": "Prairie View A&M",
    
    # Arkansas-Pine Bluff
    "Ark.-Pine Bluff": "Arkansas Pine Bluff",
    
    # Mississippi Valley
    "Mississippi Val.": "Mississippi Valley St.",
    
    # UT variations
    "UT Martin": "Tennessee Martin",
    "UT Arlington": "UT Arlington",  # Same
    "UTRGV": "UT Rio Grande Valley",
    
    # Cal State variations
    "Cal St. Fullerton": "Cal St. Fullerton",  # Same
    "Cal St. Bakersfield": "Cal St. Bakersfield",  # Same - this might be "CSU Bakersfield" in KenPom
    "CSUN": "CSUN",  # Same - Cal State Northridge
    
    # UC variations
    "UC San Diego": "UC San Diego",  # Same
    "UC Irvine": "UC Irvine",  # Same
    "UC Davis": "UC Davis",  # Same
    "UC Santa Barbara": "UC Santa Barbara",  # Same
    "UC Riverside": "UC Riverside",  # Same
    
    # Long Beach St.
    "Long Beach St.": "Long Beach St.",  # Same
    
    # Boston U.
    "Boston U.": "Boston University",
    
    # Army/Navy
    "Army West Point": "Army",
    
    # UNC variations
    "UNC Asheville": "UNC Asheville",  # Same
    "UNC Greensboro": "UNC Greensboro",  # Same
    
    # Southeastern La.
    "Southeastern La.": "Southeastern Louisiana",
    
    # Northwestern St.
    "Northwestern St.": "Northwestern St.",  # Same
    
    # ULM
    "ULM": "Louisiana Monroe",
    
    # UAlbany
    "UAlbany": "Albany",
    
    # UMass Lowell
    "UMass Lowell": "UMass Lowell",  # Same
    
    # FDU
    "FDU": "Fairleigh Dickinson",
    
    # Cal Baptist
    "California Baptist": "Cal Baptist",
    
    # USC Upstate
    "USC Upstate": "USC Upstate",  # Same
    
    # Charleston So.
    "Charleston So.": "Charleston Southern",
    
    # IU Indy
    "IU Indy": "IU Indy",  # Same
    
    # North Dakota St.
    "North Dakota St.": "North Dakota St.",  # Same
    
    # South Dakota St.
    "South Dakota St.": "South Dakota St.",  # Same
    
    # Purdue Fort Wayne
    "Purdue Fort Wayne": "Purdue Fort Wayne",  # Same
    
    # Detroit Mercy
    "Detroit Mercy": "Detroit Mercy",  # Same
    
    # Robert Morris
    "Robert Morris": "Robert Morris",  # Same
    
    # Mount St. Mary's
    "Mount St. Mary's": "Mount St. Mary's",  # Same
    
    # Le Moyne
    "Le Moyne": "Le Moyne",  # Same
    
    # Loyola Maryland
    "Loyola Maryland": "Loyola MD",
    
    # Loyola Chicago
    "Loyola Chicago": "Loyola Chicago",  # Same
    
    # Alcorn
    "Alcorn": "Alcorn St.",
    
    # UMES
    "UMES": "Maryland Eastern Shore",
    
    # Grambling
    "Grambling": "Grambling St.",
    
    # Citadel
    "The Citadel": "The Citadel",  # Same
    
    # SC State
    "South Carolina St.": "South Carolina St.",  # Same
    
    # Cal Poly
    "Cal Poly": "Cal Poly",  # Same
    
    # New Mexico St.
    "New Mexico St.": "New Mexico St.",  # Same
    
    # Lamar University
    "Lamar University": "Lamar",
    
    # Kansas City
    "Kansas City": "Kansas City",  # Same
    
    # Nebraska Omaha
    "Omaha": "Nebraska Omaha",
    
    # Queens (NC)
    "Queens (NC)": "Queens",
    
    # Mississippi St.
    "Mississippi St.": "Mississippi St.",  # Same
}

# Conference mappings: NET conference -> KenPom conference
CONF_MAPPINGS = {
    "Big Ten": "B10",
    "Big 12": "B12",
    "SEC": "SEC",
    "ACC": "ACC",
    "Big East": "BE",
    "WCC": "WCC",
    "Mountain West": "MWC",
    "American": "Amer",
    "Atlantic 10": "A10",
    "MVC": "MVC",
    "CAA": "CAA",
    "Ivy League": "Ivy",
    "MAC": "MAC",
    "CUSA": "CUSA",
    "Sun Belt": "SB",
    "WAC": "WAC",
    "Horizon": "Horz",
    "Big West": "BW",
    "Southland": "Slnd",
    "Big Sky": "BSky",
    "SoCon": "SC",
    "ASUN": "ASun",
    "Summit League": "Sum",
    "MAAC": "MAAC",
    "Patriot": "PL",
    "OVC": "OVC",
    "NEC": "NEC",
    "Big South": "BSth",
    "America East": "AE",
    "SWAC": "SWAC",
    "MEAC": "MEAC",
}


def normalize_name(name):
    """Normalize a team name for comparison."""
    # Remove common suffixes/prefixes and punctuation
    normalized = name.lower()
    normalized = re.sub(r'[.\'\-]', '', normalized)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized


def similarity_score(name1, name2):
    """Calculate similarity between two names."""
    return SequenceMatcher(None, normalize_name(name1), normalize_name(name2)).ratio()


def fetch_net_rankings():
    """Fetch NET Rankings from NCAA website."""
    url = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"
    print(f"Fetching NET Rankings from {url}...")
    
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table')
    
    if not table:
        raise Exception("Could not find rankings table on NCAA website")
    
    teams = []
    rows = table.find_all('tr')[1:]  # Skip header
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 13:
            team = {
                'net_rank': int(cells[0].get_text(strip=True)),
                'net_team': cells[1].get_text(strip=True),
                'net_record': cells[2].get_text(strip=True),
                'net_conf': cells[3].get_text(strip=True),
                'net_road': cells[4].get_text(strip=True),
                'net_neutral': cells[5].get_text(strip=True),
                'net_home': cells[6].get_text(strip=True),
                'net_q1': cells[9].get_text(strip=True),
                'net_q2': cells[10].get_text(strip=True),
                'net_q3': cells[11].get_text(strip=True),
                'net_q4': cells[12].get_text(strip=True),
            }
            teams.append(team)
    
    print(f"  Found {len(teams)} teams in NET Rankings")
    return teams


def fetch_kenpom_ratings():
    """Fetch KenPom ratings."""
    url = "https://kenpom.com/"
    print(f"Fetching KenPom ratings from {url}...")
    
    response = requests.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the main table - KenPom uses id="ratings-table" or we look for table with specific structure
    table = soup.find('table', {'id': 'ratings-table'})
    if not table:
        # Try finding any table with team data
        tables = soup.find_all('table')
        for t in tables:
            rows = t.find_all('tr')
            if len(rows) > 100:  # Main table should have many rows
                table = t
                break
    
    if not table:
        raise Exception("Could not find ratings table on KenPom website")
    
    teams = []
    rows = table.find_all('tr')
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 4:
            # First cell is rank, second is team name (with link), third is conference
            rank_text = cells[0].get_text(strip=True)
            if not rank_text.isdigit():
                continue
                
            team_link = cells[1].find('a')
            team_name = team_link.get_text(strip=True) if team_link else cells[1].get_text(strip=True)
            
            # Get conference - it's in a link within the cell
            conf_cell = cells[2]
            conf_link = conf_cell.find('a')
            conf = conf_link.get_text(strip=True) if conf_link else conf_cell.get_text(strip=True)
            
            # Record is in format "W-L"
            record = cells[3].get_text(strip=True)
            
            # NetRtg is in cells[4]
            net_rtg = cells[4].get_text(strip=True) if len(cells) > 4 else ""
            
            # ORtg is in cells[5]
            ortg = cells[5].get_text(strip=True) if len(cells) > 5 else ""
            
            # DRtg is in cells[7]
            drtg = cells[7].get_text(strip=True) if len(cells) > 7 else ""
            
            # AdjT is in cells[9]
            adj_tempo = cells[9].get_text(strip=True) if len(cells) > 9 else ""
            
            # SOS NetRtg is in cells[13]
            sos = cells[13].get_text(strip=True) if len(cells) > 13 else ""
            
            team = {
                'kp_rank': int(rank_text),
                'kp_team': team_name,
                'kp_conf': conf,
                'kp_record': record,
                'kp_net_rtg': net_rtg,
                'kp_ortg': ortg,
                'kp_drtg': drtg,
                'kp_adj_tempo': adj_tempo,
                'kp_sos': sos,
            }
            teams.append(team)
    
    print(f"  Found {len(teams)} teams in KenPom ratings")
    return teams


def match_teams(net_teams, kp_teams):
    """Match NET teams to KenPom teams."""
    print("\nMatching teams between sources...")
    
    # Build lookup dictionaries for KenPom teams
    kp_by_name = {team['kp_team']: team for team in kp_teams}
    kp_by_normalized = {normalize_name(team['kp_team']): team for team in kp_teams}
    
    # Group KenPom teams by conference for fallback matching
    kp_by_conf = {}
    for team in kp_teams:
        conf = team['kp_conf']
        if conf not in kp_by_conf:
            kp_by_conf[conf] = []
        kp_by_conf[conf].append(team)
    
    matched = []
    unmatched_net = []
    used_kp_teams = set()
    
    for net_team in net_teams:
        net_name = net_team['net_team']
        net_conf = net_team['net_conf']
        kp_team = None
        match_method = None
        
        # 1. Try exact mapping
        if net_name in NAME_MAPPINGS:
            mapped_name = NAME_MAPPINGS[net_name]
            if mapped_name in kp_by_name:
                kp_team = kp_by_name[mapped_name]
                match_method = "mapping"
        
        # 2. Try exact name match
        if not kp_team and net_name in kp_by_name:
            kp_team = kp_by_name[net_name]
            match_method = "exact"
        
        # 3. Try normalized name match
        if not kp_team:
            normalized = normalize_name(net_name)
            if normalized in kp_by_normalized:
                kp_team = kp_by_normalized[normalized]
                match_method = "normalized"
        
        # 4. Try fuzzy matching within same conference
        if not kp_team:
            kp_conf = CONF_MAPPINGS.get(net_conf, net_conf)
            if kp_conf in kp_by_conf:
                best_score = 0
                best_match = None
                for candidate in kp_by_conf[kp_conf]:
                    if candidate['kp_team'] in used_kp_teams:
                        continue
                    score = similarity_score(net_name, candidate['kp_team'])
                    if score > best_score and score > 0.6:  # Threshold
                        best_score = score
                        best_match = candidate
                
                if best_match:
                    kp_team = best_match
                    match_method = f"fuzzy({best_score:.2f})"
        
        # 5. Global fuzzy matching as last resort
        if not kp_team:
            best_score = 0
            best_match = None
            for candidate in kp_teams:
                if candidate['kp_team'] in used_kp_teams:
                    continue
                score = similarity_score(net_name, candidate['kp_team'])
                if score > best_score and score > 0.7:  # Higher threshold for global
                    best_score = score
                    best_match = candidate
            
            if best_match:
                kp_team = best_match
                match_method = f"global_fuzzy({best_score:.2f})"
        
        if kp_team:
            used_kp_teams.add(kp_team['kp_team'])
            matched.append({
                **net_team,
                **kp_team,
                'match_method': match_method
            })
        else:
            unmatched_net.append(net_team)
    
    # Find unmatched KenPom teams
    unmatched_kp = [team for team in kp_teams if team['kp_team'] not in used_kp_teams]
    
    print(f"  Matched: {len(matched)} teams")
    print(f"  Unmatched NET: {len(unmatched_net)} teams")
    print(f"  Unmatched KenPom: {len(unmatched_kp)} teams")
    
    return matched, unmatched_net, unmatched_kp


def print_unmatched(unmatched_net, unmatched_kp):
    """Print unmatched teams for debugging."""
    if unmatched_net:
        print("\n=== Unmatched NET Teams ===")
        for team in unmatched_net:
            print(f"  {team['net_rank']:3d}. {team['net_team']} ({team['net_conf']})")
    
    if unmatched_kp:
        print("\n=== Unmatched KenPom Teams ===")
        for team in unmatched_kp:
            print(f"  {team['kp_rank']:3d}. {team['kp_team']} ({team['kp_conf']})")


def write_csv(matched_teams, output_file):
    """Write combined rankings to CSV."""
    print(f"\nWriting combined rankings to {output_file}...")
    
    # Sort by KenPom ranking
    matched_teams.sort(key=lambda x: x['kp_rank'])
    
    fieldnames = [
        'KenPom_Rank', 'NET_Rank', 'Team', 'Conference', 'Record',
        'Net_Rating', 'Off_Rating', 'Def_Rating', 'Adj_Tempo', 'SOS',
        'Q1', 'Q2', 'Q3', 'Q4'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        
        for team in matched_teams:
            writer.writerow({
                'KenPom_Rank': team['kp_rank'],
                'NET_Rank': team['net_rank'],
                'Team': team['kp_team'],  # Use KenPom name (usually cleaner)
                'Conference': team['kp_conf'],
                'Record': f'="{team["kp_record"]}"',
                'Net_Rating': team['kp_net_rtg'],
                'Off_Rating': team['kp_ortg'],
                'Def_Rating': team['kp_drtg'],
                'Adj_Tempo': team['kp_adj_tempo'],
                'SOS': team['kp_sos'],
                'Q1': f'="{team["net_q1"]}"',
                'Q2': f'="{team["net_q2"]}"',
                'Q3': f'="{team["net_q3"]}"',
                'Q4': f'="{team["net_q4"]}"',
            })
    
    print(f"  Wrote {len(matched_teams)} teams to CSV")


def main():
    """Main function to run the combiner."""
    print("=" * 60)
    print("NCAA Basketball Rankings Combiner")
    print(f"Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    try:
        # Fetch data from both sources
        net_teams = fetch_net_rankings()
        kp_teams = fetch_kenpom_ratings()
        
        # Match teams
        matched, unmatched_net, unmatched_kp = match_teams(net_teams, kp_teams)
        
        # Show any unmatched teams
        if unmatched_net or unmatched_kp:
            print_unmatched(unmatched_net, unmatched_kp)
        
        # Write output
        output_file = "ncaa_combined_rankings.csv"
        write_csv(matched, output_file)
        
        print("\n" + "=" * 60)
        print("COMPLETE!")
        print(f"Output file: {output_file}")
        print("=" * 60)
        
        return 0
        
    except requests.RequestException as e:
        print(f"\nERROR: Failed to fetch data: {e}")
        return 1
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())