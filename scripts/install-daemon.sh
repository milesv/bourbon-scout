#!/bin/bash
set -e

PLIST_NAME="com.bourbon-scout.plist"
PLIST_SRC="$(cd "$(dirname "$0")/.." && pwd)/$PLIST_NAME"
PLIST_DST="$HOME/Library/LaunchAgents/$PLIST_NAME"

# Unload if already running
launchctl unload "$PLIST_DST" 2>/dev/null || true

# Copy plist to LaunchAgents
cp "$PLIST_SRC" "$PLIST_DST"
echo "Installed $PLIST_DST"

# Load and start
launchctl load "$PLIST_DST"
echo "Daemon loaded. Checking status..."
sleep 1

if launchctl list | grep -q "com.bourbon-scout"; then
  echo "✓ bourbon-scout is running"
  echo ""
  echo "Useful commands:"
  echo "  tail -f ~/Library/Logs/bourbon-scout.log   # Watch logs"
  echo "  launchctl unload $PLIST_DST                 # Stop daemon"
  echo "  launchctl load $PLIST_DST                   # Start daemon"
else
  echo "✗ bourbon-scout failed to start. Check: launchctl list | grep bourbon"
fi
