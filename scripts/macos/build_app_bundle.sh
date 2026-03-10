#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE="${1:-debug}"
APP_NAME="${APP_NAME:-zBase}"
BUNDLE_IDENTIFIER="${BUNDLE_IDENTIFIER:-com.zbase.app}"
ICON_PATH="${ROOT_DIR}/assets/macos/AppIcon.icns"
VERSION="$(awk -F'"' '/^version = / { print $2; exit }' "${ROOT_DIR}/Cargo.toml")"
SKIP_BUILD="${SKIP_BUILD:-0}"

if [[ "${PROFILE}" == "release" ]]; then
  BIN_PATH="${ROOT_DIR}/target/release/zbase"
  if [[ "${SKIP_BUILD}" != "1" ]]; then
    cargo build --release --manifest-path "${ROOT_DIR}/Cargo.toml"
  fi
else
  BIN_PATH="${ROOT_DIR}/target/debug/zbase"
  if [[ "${SKIP_BUILD}" != "1" ]]; then
    cargo build --manifest-path "${ROOT_DIR}/Cargo.toml"
  fi
fi

if [[ ! -f "${BIN_PATH}" ]]; then
  echo "Binary missing at ${BIN_PATH}"
  echo "Build first (or run this script without SKIP_BUILD=1)."
  exit 1
fi

if [[ ! -f "${ICON_PATH}" ]]; then
  echo "Icon missing at ${ICON_PATH}"
  echo "Generate it with iconutil from your source PNG before bundling."
  exit 1
fi

OUT_DIR="${ROOT_DIR}/dist/macos"
APP_DIR="${OUT_DIR}/${APP_NAME}.app"
CONTENTS_DIR="${APP_DIR}/Contents"
MACOS_DIR="${CONTENTS_DIR}/MacOS"
RESOURCES_DIR="${CONTENTS_DIR}/Resources"
PLIST_PATH="${CONTENTS_DIR}/Info.plist"

rm -rf "${APP_DIR}"
mkdir -p "${MACOS_DIR}" "${RESOURCES_DIR}"

cp "${BIN_PATH}" "${MACOS_DIR}/zbase"
chmod +x "${MACOS_DIR}/zbase"
cp "${ICON_PATH}" "${RESOURCES_DIR}/AppIcon.icns"

cat > "${PLIST_PATH}" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleDevelopmentRegion</key>
  <string>en</string>
  <key>CFBundleDisplayName</key>
  <string>${APP_NAME}</string>
  <key>CFBundleExecutable</key>
  <string>zbase</string>
  <key>CFBundleIconFile</key>
  <string>AppIcon</string>
  <key>CFBundleIdentifier</key>
  <string>${BUNDLE_IDENTIFIER}</string>
  <key>CFBundleInfoDictionaryVersion</key>
  <string>6.0</string>
  <key>CFBundleName</key>
  <string>${APP_NAME}</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>CFBundleShortVersionString</key>
  <string>${VERSION}</string>
  <key>CFBundleVersion</key>
  <string>${VERSION}</string>
  <key>LSMinimumSystemVersion</key>
  <string>13.0</string>
  <key>NSHighResolutionCapable</key>
  <true/>
</dict>
</plist>
EOF

plutil -lint "${PLIST_PATH}" >/dev/null

echo "Built app bundle: ${APP_DIR}"
