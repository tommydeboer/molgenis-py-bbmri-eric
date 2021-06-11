current_version="$(git tag | sort -r --version-sort | head -n1)"

if [ -z "$current_version" ]
then
  current_version="0.0.0"
fi

new_version="$(bumpversion --current-version "$current_version" --list "$RELEASE_SCOPE" | grep new_version= | cut -d'=' -f2)"
echo "$new_version"
