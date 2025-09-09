#!/usr/bin/env python3
"""
NFL Performance Data Collection Script
Collects weekly performance data using nfl_data_py and integrates with Sleeper player database
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from difflib import SequenceMatcher
import pandas as pd

# Third-party imports
try:
    import nfl_data_py as nfl
    import requests
except ImportError as e:
    print(f"Error importing required packages: {e}")
    print("Install with: pip install nfl_data_py requests pandas")
    sys.exit(1)

class NFLPerformanceCollector:
    def __init__(self, data_dir: str = "data"):
        """Initialize the NFL Performance Collector"""
        self.data_dir = data_dir
        self.current_year = 2025
        self.performance_file = os.path.join(data_dir, "season_2025_performances.json")
        self.totals_file = os.path.join(data_dir, "season_2025_totals.json")
        self.players_file = os.path.join(data_dir, "players.json")
        self.adp_file = os.path.join(data_dir, "adp_consolidated_2025.json")
        
        # Load existing data
        self.sleeper_players = self._load_sleeper_players()
        self.fantasy_relevant_players = self._load_fantasy_relevant_players()
        self.performance_data = self._load_existing_performance_data()
        self.totals_data = self._load_existing_totals_data()
        
        print(f"Initialized collector with {len(self.fantasy_relevant_players)} fantasy-relevant players")

    def _load_sleeper_players(self) -> Dict[str, Any]:
        """Load Sleeper player database for ID mapping"""
        try:
            with open(self.players_file, 'r') as f:
                players = json.load(f)
            print(f"Loaded {len(players)} Sleeper players")
            return players
        except Exception as e:
            print(f"Warning: Could not load Sleeper players: {e}")
            return {}

    def _load_fantasy_relevant_players(self) -> List[str]:
        """Load list of fantasy-relevant players from ADP data"""
        try:
            with open(self.adp_file, 'r') as f:
                adp_data = json.load(f)
            
            # Extract player names/IDs from ADP data structure
            if isinstance(adp_data, dict) and 'players' in adp_data:
                players = list(adp_data['players'].keys())
            elif isinstance(adp_data, list):
                players = [p.get('name', '') for p in adp_data if p.get('name')]
            else:
                # Fallback: get player names from top-level keys
                players = [p.get('name', '') for p in adp_data if isinstance(p, dict) and p.get('name')]
            
            print(f"Identified {len(players)} fantasy-relevant players from ADP data")
            return players
        except Exception as e:
            print(f"Warning: Could not load ADP data: {e}")
            return []

    def _load_existing_performance_data(self) -> Dict[str, Any]:
        """Load existing performance data"""
        try:
            with open(self.performance_file, 'r') as f:
                data = json.load(f)
            print(f"Loaded existing performance data with {len(data)} weeks")
            return data
        except FileNotFoundError:
            print("No existing performance data found, starting fresh")
            return {}
        except Exception as e:
            print(f"Error loading performance data: {e}")
            return {}

    def _load_existing_totals_data(self) -> Dict[str, Any]:
        """Load existing season totals data"""
        try:
            with open(self.totals_file, 'r') as f:
                data = json.load(f)
            print(f"Loaded existing totals data with {len(data)} players")
            return data
        except FileNotFoundError:
            print("No existing totals data found, starting fresh")
            return {}
        except Exception as e:
            print(f"Error loading totals data: {e}")
            return {}

    def get_current_nfl_week(self) -> int:
        """Determine current NFL week"""
        # For testing/development, allow manual override
        if 'NFL_WEEK' in os.environ:
            return int(os.environ['NFL_WEEK'])
        
        # Calculate based on current date (approximate)
        # NFL season typically starts first week of September
        now = datetime.now()
        season_start = datetime(self.current_year, 9, 5)  # Approximate start
        
        if now < season_start:
            return 1  # Pre-season
        
        weeks_elapsed = (now - season_start).days // 7 + 1
        return min(max(weeks_elapsed, 1), 18)  # Cap at week 18

    def collect_weekly_stats(self, week: int) -> Optional[pd.DataFrame]:
        """Collect weekly stats from nfl_data_py"""
        try:
            print(f"Collecting weekly stats for week {week}...")
            
            # Try current year first, fallback to previous year for testing
            try:
                weekly_data = nfl.import_weekly_data([self.current_year], columns=[
                    'player_id', 'player_name', 'player_display_name', 'position', 'recent_team',
                    'week', 'season', 'season_type',
                    'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions',
                    'carries', 'rushing_yards', 'rushing_tds', 'targets', 'receptions', 
                    'receiving_yards', 'receiving_tds', 'fantasy_points', 'fantasy_points_ppr'
                ])
            except Exception as e:
                print(f"2025 data not available, using 2024 for testing: {e}")
                weekly_data = nfl.import_weekly_data([2024], columns=[
                    'player_id', 'player_name', 'player_display_name', 'position', 'recent_team',
                    'week', 'season', 'season_type',
                    'completions', 'attempts', 'passing_yards', 'passing_tds', 'interceptions',
                    'carries', 'rushing_yards', 'rushing_tds', 'targets', 'receptions', 
                    'receiving_yards', 'receiving_tds', 'fantasy_points', 'fantasy_points_ppr'
                ])
            
            # Filter to specific week and regular season
            week_data = weekly_data[
                (weekly_data['week'] == week) & 
                (weekly_data['season_type'] == 'REG')
            ].copy()
            
            print(f"Found {len(week_data)} player performances for week {week}")
            return week_data
            
        except Exception as e:
            print(f"Error collecting weekly stats: {e}")
            return None

    def collect_snap_counts(self, week: int) -> Optional[pd.DataFrame]:
        """Collect snap count data"""
        try:
            print(f"Collecting snap counts for week {week}...")
            
            # Use current year, fallback to 2024
            try:
                snap_data = nfl.import_snap_counts([self.current_year])
            except:
                print("Using 2024 snap count data for testing")
                snap_data = nfl.import_snap_counts([2024])
            
            # Filter to specific week
            week_snaps = snap_data[snap_data['week'] == week].copy()
            
            print(f"Found snap count data for {len(week_snaps)} players")
            return week_snaps
            
        except Exception as e:
            print(f"Error collecting snap counts: {e}")
            return None

    def map_player_ids(self, nfl_data: pd.DataFrame) -> pd.DataFrame:
        """Map NFL player names to Sleeper player IDs with robust name matching"""
        
        # Create comprehensive mapping from NFL names to Sleeper IDs
        sleeper_mapping = {}
        
        for sleeper_id, player_info in self.sleeper_players.items():
            if not isinstance(player_info, dict):
                continue
                
            # Get all name variations from Sleeper
            full_name = player_info.get('full_name', '').strip()
            first_name = player_info.get('first_name', '').strip()
            last_name = player_info.get('last_name', '').strip()
            
            if not full_name or not first_name or not last_name:
                continue
                
            # Create multiple name format variations to match against
            name_variations = []
            
            # 1. Full name as-is
            name_variations.append(full_name)
            
            # 2. "First Last" format
            name_variations.append(f"{first_name} {last_name}")
            
            # 3. "F.Last" format (common in NFL data)
            if first_name:
                name_variations.append(f"{first_name[0]}.{last_name}")
                
            # 4. "FirstName LastName" (no middle names/suffixes)
            # Handle cases like "Le'Veon Bell" -> "LeVeon Bell"
            clean_first = first_name.replace("'", "").replace("-", "")
            clean_last = last_name.replace("'", "").replace("-", "")
            name_variations.append(f"{clean_first} {clean_last}")
            if clean_first:
                name_variations.append(f"{clean_first[0]}.{clean_last}")
            
            # 5. Handle Jr/Sr/III suffixes
            if " Jr" in full_name or " Sr" in full_name or " III" in full_name:
                base_name = full_name.split(" Jr")[0].split(" Sr")[0].split(" III")[0]
                name_variations.append(base_name.strip())
                
            # 6. Reverse format "Last, First"
            name_variations.append(f"{last_name}, {first_name}")
            
            # Store all variations (avoid duplicates)
            for name_var in name_variations:
                if name_var and name_var not in sleeper_mapping:
                    sleeper_mapping[name_var] = sleeper_id
        
        print(f"Created {len(sleeper_mapping)} name-to-ID mappings")
        
        # Debug: Show sample mappings
        sample_mappings = list(sleeper_mapping.items())[:5]
        for name, sleeper_id in sample_mappings:
            print(f"  Sample: '{name}' -> {sleeper_id}")
        
        # Add Sleeper ID to NFL data with fuzzy matching
        nfl_data = nfl_data.copy()
        nfl_data['sleeper_id'] = None
        nfl_data['sleeper_id_confidence'] = None
        
        # First pass: Exact matching
        exact_matches = 0
        for idx, row in nfl_data.iterrows():
            nfl_name = str(row['player_name']).strip()
            
            if nfl_name in sleeper_mapping:
                nfl_data.at[idx, 'sleeper_id'] = sleeper_mapping[nfl_name]
                nfl_data.at[idx, 'sleeper_id_confidence'] = 'exact'
                exact_matches += 1
        
        # Second pass: Fuzzy matching for unmapped players
        def similarity(a, b):
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()
        
        fuzzy_matches = 0
        for idx, row in nfl_data.iterrows():
            if pd.notna(row['sleeper_id']):
                continue  # Already mapped
                
            nfl_name = str(row['player_name']).strip()
            best_match = None
            best_score = 0.0
            
            # Try fuzzy matching against all Sleeper names
            for sleeper_name, sleeper_id in sleeper_mapping.items():
                score = similarity(nfl_name, sleeper_name)
                
                # High confidence threshold for fuzzy matching
                if score > 0.85 and score > best_score:
                    best_score = score
                    best_match = sleeper_id
            
            if best_match and best_score > 0.85:
                nfl_data.at[idx, 'sleeper_id'] = best_match
                nfl_data.at[idx, 'sleeper_id_confidence'] = f'fuzzy_{best_score:.2f}'
                fuzzy_matches += 1
        
        # Log mapping success rate
        mapped_count = nfl_data['sleeper_id'].notna().sum()
        total_count = len(nfl_data)
        mapping_rate = mapped_count / total_count * 100 if total_count > 0 else 0
        
        print(f"Mapping Results:")
        print(f"  Exact matches: {exact_matches}")
        print(f"  Fuzzy matches: {fuzzy_matches}")
        print(f"  Total mapped: {mapped_count}/{total_count} ({mapping_rate:.1f}%)")
        
        # Show sample unmapped players for debugging
        unmapped = nfl_data[nfl_data['sleeper_id'].isna()]['player_name'].head(5).tolist()
        if unmapped:
            print(f"  Sample unmapped: {unmapped}")
        
        return nfl_data

    def filter_fantasy_relevant(self, nfl_data: pd.DataFrame) -> pd.DataFrame:
        """Filter to fantasy-relevant players with inclusive criteria"""
        
        # Filter by position first (fantasy relevant positions)
        fantasy_positions = ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
        relevant_data = nfl_data[nfl_data['position'].isin(fantasy_positions)].copy()
        
        print(f"After position filter: {len(relevant_data)} players")
        
        # Apply inclusive activity filter (OR logic instead of AND)
        # Player needs ANY significant fantasy activity
        activity_filter = (
            (relevant_data['fantasy_points'].fillna(0) > 0) |      # Scored any fantasy points
            (relevant_data['fantasy_points_ppr'].fillna(0) > 0) |  # Scored any PPR points
            (relevant_data['targets'].fillna(0) > 0) |             # Had any targets (WR/TE/RB)
            (relevant_data['carries'].fillna(0) > 0) |             # Had any carries (RB/QB)
            (relevant_data['attempts'].fillna(0) > 0) |            # Had any pass attempts (QB)
            (relevant_data['receptions'].fillna(0) > 0)            # Had any receptions (WR/TE/RB)
        )
        
        relevant_data = relevant_data[activity_filter].copy()
        
        print(f"After activity filter: {len(relevant_data)} players")
        
        # Optional: Filter out players with very minimal involvement
        # (This is less aggressive than the original filter)
        minimal_involvement = (
            (relevant_data['fantasy_points_ppr'].fillna(0) >= 0.1) |  # At least 0.1 PPR points
            (relevant_data['targets'].fillna(0) >= 1) |              # At least 1 target
            (relevant_data['carries'].fillna(0) >= 1) |              # At least 1 carry
            (relevant_data['attempts'].fillna(0) >= 1)               # At least 1 pass attempt
        )
        
        relevant_data = relevant_data[minimal_involvement].copy()
        
        print(f"After minimal involvement filter: {len(relevant_data)} players")
        
        # Debug: Show sample players that made it through
        if len(relevant_data) > 0:
            print("Sample players that passed filter:")
            sample_players = relevant_data[['player_name', 'position', 'fantasy_points_ppr', 'targets', 'carries', 'attempts']].head(5)
            for _, player in sample_players.iterrows():
                targets_val = player['targets'] if pd.notna(player['targets']) else 0
                carries_val = player['carries'] if pd.notna(player['carries']) else 0
                attempts_val = player['attempts'] if pd.notna(player['attempts']) else 0
                ppr_val = player['fantasy_points_ppr'] if pd.notna(player['fantasy_points_ppr']) else 0.0
                
                print(f"  {player['player_name']} ({player['position']}): "
                      f"PPR: {ppr_val:.1f}, "
                      f"Targets: {int(targets_val)}, "
                      f"Carries: {int(carries_val)}, "
                      f"Attempts: {int(attempts_val)}")
        
        return relevant_data

    def _calculate_usage_metrics(self, player_row: pd.Series, snap_counts: Optional[pd.DataFrame]) -> Dict[str, Any]:
        """Calculate usage metrics for a player"""
        
        usage = {
            'target_share': None,
            'snap_percentage': None,
            'red_zone_targets': None,
            'air_yards': None
        }
        
        # Calculate target share (requires team data)
        targets = self._safe_int(player_row.get('targets'))
        if targets is not None and targets > 0:
            # This would need team-level target data to calculate properly
            # For now, just store raw targets
            usage['raw_targets'] = targets
        
        # Add snap count data if available
        if snap_counts is not None and not snap_counts.empty:
            player_snaps = snap_counts[
                snap_counts['player'] == player_row['player_name']
            ]
            
            if not player_snaps.empty:
                snap_row = player_snaps.iloc[0]
                usage['snaps_offense'] = self._safe_int(snap_row.get('offense'))
                usage['snaps_defense'] = self._safe_int(snap_row.get('defense'))
                usage['snaps_st'] = self._safe_int(snap_row.get('st'))
        
        return usage

    def _safe_int(self, value) -> Optional[int]:
        """Safely convert value to int"""
        if pd.isna(value) or value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None

    def _safe_float(self, value) -> Optional[float]:
        """Safely convert value to float"""
        if pd.isna(value) or value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_float_rounded(self, value) -> Optional[float]:
        """Safely convert value to float rounded to 2 decimal places"""
        if pd.isna(value) or value is None:
            return None
        try:
            return round(float(value), 2)
        except (ValueError, TypeError):
            return None

    def _calculate_half_ppr(self, player_row: pd.Series) -> Optional[float]:
        """Calculate 0.5 PPR fantasy points"""
        standard = self._safe_float_rounded(player_row.get('fantasy_points'))
        receptions = self._safe_int(player_row.get('receptions'))
        
        if standard is not None and receptions is not None:
            return round(standard + (receptions * 0.5), 2)
        return standard

    def process_week_data(self, week: int) -> Dict[str, Any]:
        """Process all data for a specific week"""
        
        week_key = f"week_{week}"
        week_data = {}
        
        # Collect weekly stats
        weekly_stats = self.collect_weekly_stats(week)
        if weekly_stats is None or len(weekly_stats) == 0:
            print(f"No weekly stats available for week {week}")
            return {}
        
        # Map to Sleeper IDs and filter to fantasy relevant
        mapped_data = self.map_player_ids(weekly_stats)
        relevant_data = self.filter_fantasy_relevant(mapped_data)
        
        # Collect snap counts
        snap_counts = self.collect_snap_counts(week)
        
        # Process each player's performance
        for _, player_row in relevant_data.iterrows():
            player_name = player_row['player_name']
            sleeper_id = player_row.get('sleeper_id')
            
            # Create player performance record
            player_performance = {
                'player_name': player_name,
                'sleeper_id': sleeper_id,
                'position': player_row['position'],
                'team': player_row['recent_team'],
                'week': week,
                'season': int(player_row['season']),
                'stats': {
                    'passing': {
                        'completions': self._safe_int(player_row.get('completions')),
                        'attempts': self._safe_int(player_row.get('attempts')),
                        'yards': self._safe_int(player_row.get('passing_yards')),
                        'touchdowns': self._safe_int(player_row.get('passing_tds')),
                        'interceptions': self._safe_int(player_row.get('interceptions'))
                    },
                    'rushing': {
                        'carries': self._safe_int(player_row.get('carries')),
                        'yards': self._safe_int(player_row.get('rushing_yards')),
                        'touchdowns': self._safe_int(player_row.get('rushing_tds'))
                    },
                    'receiving': {
                        'targets': self._safe_int(player_row.get('targets')),
                        'receptions': self._safe_int(player_row.get('receptions')),
                        'yards': self._safe_int(player_row.get('receiving_yards')),
                        'touchdowns': self._safe_int(player_row.get('receiving_tds'))
                    },
                    'fantasy': {
                        'points_standard': self._safe_float_rounded(player_row.get('fantasy_points')),
                        'points_ppr': self._safe_float_rounded(player_row.get('fantasy_points_ppr')),
                        'points_half_ppr': self._calculate_half_ppr(player_row)
                    }
                },
                'usage': self._calculate_usage_metrics(player_row, snap_counts),
                'last_updated': datetime.now().isoformat()
            }
            
            # Use player name as key (could also use sleeper_id if available)
            key = sleeper_id if sleeper_id else player_name
            week_data[key] = player_performance
        
        print(f"Processed {len(week_data)} player performances for week {week}")
        return {week_key: week_data}

    def update_performance_data(self, week_data: Dict[str, Any]) -> None:
        """Update the performance data file with new week data"""
        
        # Merge with existing data
        self.performance_data.update(week_data)
        
        # Save updated data
        try:
            # Ensure directory exists
            os.makedirs(self.data_dir, exist_ok=True)
            
            # Write to file
            with open(self.performance_file, 'w') as f:
                json.dump(self.performance_data, f, indent=2)
            
            print(f"Updated performance data saved to {self.performance_file}")
            
        except Exception as e:
            print(f"Error saving performance data: {e}")
            raise

    def update_season_totals(self, player_key: str, player_performance: Dict[str, Any]) -> None:
        """Update season totals for a player based on weekly performance."""
        if player_key not in self.totals_data:
            self.totals_data[player_key] = {
                'player_name': player_performance['player_name'],
                'position': player_performance['position'],
                'games_played': 0,
                'season_totals': {
                    'passing': {k: 0 for k in player_performance['stats']['passing'] if k is not None},
                    'rushing': {k: 0 for k in player_performance['stats']['rushing'] if k is not None},
                    'receiving': {k: 0 for k in player_performance['stats']['receiving'] if k is not None},
                    'fantasy': {k: 0.0 for k in player_performance['stats']['fantasy'] if k is not None},
                },
                'metrics': {
                    'average_per_game': 0.0,
                    'average_per_game_half_ppr': 0.0,
                    'average_per_game_standard': 0.0
                }
            }

        totals = self.totals_data[player_key]
        totals['games_played'] += 1

        # Aggregate stats
        for category in ['passing', 'rushing', 'receiving']:
            for stat, value in player_performance['stats'][category].items():
                if value is not None:
                    totals['season_totals'][category][stat] += value
                else:
                    totals['season_totals'][category][stat] += 0  # Handle None as 0

        for stat, value in player_performance['stats']['fantasy'].items():
            if value is not None:
                totals['season_totals']['fantasy'][stat] += value
            else:
                totals['season_totals']['fantasy'][stat] += 0.0

        # Update metrics (averages for all scoring formats)
        if totals['games_played'] > 0:
            totals['metrics']['average_per_game'] = round(
                totals['season_totals']['fantasy'].get('points_ppr', 0.0) / totals['games_played'], 2
            )
            totals['metrics']['average_per_game_half_ppr'] = round(
                totals['season_totals']['fantasy'].get('points_half_ppr', 0.0) / totals['games_played'], 2
            )
            totals['metrics']['average_per_game_standard'] = round(
                totals['season_totals']['fantasy'].get('points_standard', 0.0) / totals['games_played'], 2
            )

    def save_totals_data(self) -> None:
        """Save the updated totals data."""
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            with open(self.totals_file, 'w') as f:
                json.dump(self.totals_data, f, indent=2)
            print(f"Updated totals data saved to {self.totals_file}")
        except Exception as e:
            print(f"Error saving totals data: {e}")
            raise

    def collect_week(self, week: Optional[int] = None) -> bool:
        """Collect data for a specific week"""
        
        if week is None:
            week = self.get_current_nfl_week()
        
        print(f"\n=== Collecting NFL Performance Data for Week {week} ===")
        
        try:
            # Process week data
            week_data = self.process_week_data(week)
            
            if not week_data:
                print(f"No data collected for week {week}")
                return False
            
            # Update performance data and season totals
            self.update_performance_data(week_data)
            
            # Update season totals for each player
            week_key = f"week_{week}"
            if week_key in week_data:
                for player_key, player_performance in week_data[week_key].items():
                    self.update_season_totals(player_key, player_performance)
            
            # Save both files
            self.save_totals_data()
            
            print(f"✅ Successfully collected week {week} performance data")
            return True
            
        except Exception as e:
            print(f"❌ Error collecting week {week} data: {e}")
            return False

    def collect_season(self, start_week: int = 1, end_week: Optional[int] = None) -> None:
        """Collect data for multiple weeks"""
        
        if end_week is None:
            end_week = self.get_current_nfl_week()
        
        print(f"\n=== Collecting Season Data (Weeks {start_week}-{end_week}) ===")
        
        success_count = 0
        for week in range(start_week, end_week + 1):
            if self.collect_week(week):
                success_count += 1
        
        print(f"\n✅ Successfully collected data for {success_count}/{end_week - start_week + 1} weeks")

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect NFL performance data')
    parser.add_argument('--week', type=int, help='Specific week to collect (default: current week)')
    parser.add_argument('--season', action='store_true', help='Collect entire season to current week')
    parser.add_argument('--data-dir', default='data', help='Data directory path')
    
    args = parser.parse_args()
    
    # Initialize collector
    collector = NFLPerformanceCollector(data_dir=args.data_dir)
    
    if args.season:
        collector.collect_season()
    else:
        collector.collect_week(args.week)

if __name__ == "__main__":
    main()
