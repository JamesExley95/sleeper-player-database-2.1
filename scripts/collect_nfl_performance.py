    - name: Test data collection (single week)
      if: ${{ github.event.inputs.test_mode != 'validation_only' }}
      env:
        NFL_WEEK: ${{ github.event.inputs.test_week || '1' }}
      run: |
        echo "=== Testing Data Collection for Week $NFL_WEEK ==="
        
        # Create backup of existing files
        cp data/season_2025_performances.json data/season_2025_performances.json.backup 2>/dev/null || echo "No existing performance file"
        
        # Run collection
        python scripts/collect_nfl_performance.py --week $NFL_WEEK
        
        echo "=== Verifying Output Files ==="
        
        # Check performance file
        if [ -f "data/season_2025_performances.json" ]; then
          echo "✅ Performance file created/updated"
          echo "   Size: $(wc -c < data/season_2025_performances.json) bytes"
          
          # Validate JSON structure
          python << 'EOF'
        import json
        import os
        with open('data/season_2025_performances.json', 'r') as f:
            data = json.load(f)
        
        week_key = f'week_{os.environ["NFL_WEEK"]}'
        if week_key in data:
            player_count = len(data[week_key])
            print(f'✅ Week {os.environ["NFL_WEEK"]} data found with {player_count} players')
            
            # Show sample player
            if player_count > 0:
                sample_player = list(data[week_key].keys())[0]
                sample_data = data[week_key][sample_player]
                print(f'   Sample player: {sample_data.get("player_name", "Unknown")}')
                print(f'   Position: {sample_data.get("position", "Unknown")}')
                fantasy_points = sample_data.get("stats", {}).get("fantasy", {}).get("points_ppr", "Unknown")
                print(f'   Fantasy points: {fantasy_points}')
        else:
            print(f'❌ No data found for week {os.environ["NFL_WEEK"]}')
            exit(1)
        EOF
        else
          echo "❌ Performance file not created"
          exit 1
        fi
        
        # Check totals file
        if [ -f "data/season_2025_totals.json" ]; then
          echo "✅ Totals file created/updated"
          echo "   Size: $(wc -c < data/season_2025_totals.json) bytes"
          
          # Validate totals structure
          python -c "
        import json
        with open('data/season_2025_totals.json', 'r') as f:
            data = json.load(f)
        
        player_count = len(data)
        print(f'✅ Season totals for {player_count} players')
        
        if player_count > 0:
            sample_player = list(data.keys())[0]
            sample_data = data[sample_player]
            print(f'   Sample player: {sample_data.get(\"player_name\", \"Unknown\")}')
            print(f'   Games played: {sample_data.get(\"games_played\", 0)}')
            print(f'   Fantasy average: {sample_data.get(\"metrics\", {}).get(\"average_per_game\", \"Unknown\")}')
          "
        else
          echo "❌ Totals file not created"
          exit 1
        fi
