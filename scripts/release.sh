#!/usr/bin/env bash
#
# Tag a release, push it to GitHub, then update the Homebrew formula
# in cameroncooper/homebrew-tap with the new version and SHA256.
#
# Usage:
#   scripts/release.sh <version>      # e.g. scripts/release.sh 0.1.0
#
# Prerequisites:  gh CLI authenticated, git remote "origin" set.
#
set -euo pipefail

VERSION="${1:?Usage: scripts/release.sh <version>}"
TAG="v${VERSION}"

REPO="cameroncooper/zbase"
TAP_REPO="cameroncooper/homebrew-tap"

echo "==> Tagging ${TAG}…"
git tag -a "${TAG}" -m "zBase ${TAG}"
git push origin "${TAG}"

TARBALL_URL="https://github.com/${REPO}/archive/refs/tags/${TAG}.tar.gz"

echo "==> Downloading source tarball to compute SHA256…"
SHA=$(curl -sL "${TARBALL_URL}" | shasum -a 256 | awk '{print $1}')
echo "    sha256: ${SHA}"

echo "==> Creating GitHub release ${TAG}…"
gh release create "${TAG}" \
  --repo "${REPO}" \
  --title "zBase ${TAG}" \
  --generate-notes \
  --latest

echo "==> Updating Homebrew formula in ${TAP_REPO}…"

FORMULA=$(cat <<RUBY
class Zbase < Formula
  desc "A fast, native chat client"
  homepage "https://github.com/${REPO}"
  url "${TARBALL_URL}"
  sha256 "${SHA}"
  license "MIT"

  depends_on "rust" => :build
  depends_on "cmake" => :build
  depends_on "pkg-config" => :build
  depends_on :macos
  depends_on "ffmpeg"

  def install
    system "cargo", "build", "--release", "--locked"

    bin.install "target/release/zbase"

    app = prefix/"zBase.app/Contents"
    (app/"MacOS").mkpath
    (app/"Resources").mkpath

    cp bin/"zbase", app/"MacOS/zbase"
    cp "assets/macos/AppIcon.icns", app/"Resources/AppIcon.icns" if File.exist?("assets/macos/AppIcon.icns")

    (app/"Info.plist").write <<~PLIST
      <?xml version="1.0" encoding="UTF-8"?>
      <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
      <plist version="1.0">
      <dict>
        <key>CFBundleDevelopmentRegion</key>
        <string>en</string>
        <key>CFBundleDisplayName</key>
        <string>zBase</string>
        <key>CFBundleExecutable</key>
        <string>zbase</string>
        <key>CFBundleIconFile</key>
        <string>AppIcon</string>
        <key>CFBundleIdentifier</key>
        <string>com.zbase.app</string>
        <key>CFBundleInfoDictionaryVersion</key>
        <string>6.0</string>
        <key>CFBundleName</key>
        <string>zBase</string>
        <key>CFBundlePackageType</key>
        <string>APPL</string>
        <key>CFBundleShortVersionString</key>
        <string>#{version}</string>
        <key>CFBundleVersion</key>
        <string>#{version}</string>
        <key>LSMinimumSystemVersion</key>
        <string>13.0</string>
        <key>NSHighResolutionCapable</key>
        <true/>
      </dict>
      </plist>
    PLIST
  end

  def caveats
    <<~EOS
      The zBase.app bundle has been installed to:
        #{prefix}/zBase.app

      To add it to your Applications folder:
        ln -sf #{prefix}/zBase.app /Applications/zBase.app
    EOS
  end

  test do
    assert_predicate bin/"zbase", :executable?
  end
end
RUBY
)

TMPFILE=$(mktemp)
echo "${FORMULA}" > "${TMPFILE}"

EXISTING=$(gh api "repos/${TAP_REPO}/contents/Formula/zbase.rb" --jq '.sha' 2>/dev/null || true)

if [[ -n "${EXISTING}" ]]; then
  gh api --method PUT "repos/${TAP_REPO}/contents/Formula/zbase.rb" \
    -f message="Update zbase formula to ${TAG}" \
    -f content="$(base64 < "${TMPFILE}")" \
    -f sha="${EXISTING}" \
    --silent
else
  gh api --method PUT "repos/${TAP_REPO}/contents/Formula/zbase.rb" \
    -f message="Add zbase formula ${TAG}" \
    -f content="$(base64 < "${TMPFILE}")" \
    --silent
fi

rm -f "${TMPFILE}"

echo ""
echo "Done! Users can now install with:"
echo "  brew tap cameroncooper/tap"
echo "  brew install zbase"
