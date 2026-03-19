#!/bin/bash
set -e

PLIST_NAME="com.bourbon-scout.plist"
PLIST_DST="$HOME/Library/LaunchAgents/$PLIST_NAME"

if [ -f "$PLIST_DST" ]; then
  launchctl unload "$PLIST_DST" 2>/dev/null || true
  rm "$PLIST_DST"
  echo "✓ bourbon-scout daemon uninstalled"
else
  echo "No daemon installed at $PLIST_DST"
fi
